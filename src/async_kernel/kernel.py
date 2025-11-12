from __future__ import annotations

import atexit
import builtins
import contextlib
import errno
import functools
import getpass
import importlib.util
import json
import logging
import math
import os
import pathlib
import signal
import sys
import threading
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from logging import Logger, LoggerAdapter
from pathlib import Path
from types import CoroutineType
from typing import TYPE_CHECKING, Any, Literal, Self

import anyio
import IPython.core.completer
import traitlets
import zmq
from aiologic import BinarySemaphore, Event
from aiologic.lowlevel import current_async_library
from IPython.core.error import StdinNotImplementedError
from IPython.utils.tokenutil import token_at_cursor
from jupyter_client import write_connection_file
from jupyter_client.localinterfaces import localhost
from jupyter_client.session import Session
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import CaselessStrEnum, Dict, HasTraits, Instance, Set, Tuple, Unicode, UseEnum
from typing_extensions import override
from zmq import Context, Flag, PollEvent, Socket, SocketOption, SocketType, ZMQError

import async_kernel
from async_kernel import Caller, utils
from async_kernel.asyncshell import AsyncInteractiveShell
from async_kernel.debugger import Debugger
from async_kernel.iostream import OutStream
from async_kernel.kernelspec import Backend, KernelName
from async_kernel.typing import (
    Content,
    ExecuteContent,
    HandlerType,
    Job,
    Message,
    MsgType,
    NoValue,
    RunMode,
    SocketID,
    Tags,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Iterable
    from types import CoroutineType, FrameType

    from async_kernel.comm import CommManager


__all__ = ["Kernel", "KernelInterruptError"]


def error_to_content(error: BaseException, /) -> Content:
    """
    Convert the error to a dict.

    ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#request-reply
    """
    return {
        "status": "error",
        "ename": type(error).__name__,
        "evalue": str(error),
        "traceback": traceback.format_exception(error),
    }


def bind_socket(
    socket: Socket[SocketType],
    transport: Literal["tcp", "ipc"],
    ip: str,
    port: int = 0,
    max_attempts: int | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
) -> int:
    """
    Bind the socket to a port using the settings.

    max_attempts: The maximum number of attempts to bind the socket. If un-specified,
    defaults to 100 if port missing, else 2 attempts.
    """
    if socket.TYPE == SocketType.ROUTER:
        # ref: https://github.com/ipython/ipykernel/issues/270
        socket.router_handover = 1
    if transport == "ipc":
        ip = Path(ip).as_posix()
    if max_attempts is NoValue:
        max_attempts = 2 if port else 100
    for attempt in range(max_attempts):
        try:
            if transport == "tcp":
                if not port:
                    port = socket.bind_to_random_port(f"tcp://{ip}")
                else:
                    socket.bind(f"tcp://{ip}:{port}")
            elif transport == "ipc":
                if not port:
                    port = 1
                    while Path(f"{ip}-{port}").exists():
                        port += 1
                socket.bind(f"ipc://{ip}-{port}")
            else:
                msg = f"Invalid transport: {transport}"  # pyright: ignore[reportUnreachable]
                raise ValueError(msg)
        except ZMQError as e:
            if e.errno not in {errno.EADDRINUSE, 98, 10048, 135}:
                raise
            if port and attempt < max_attempts - 1:
                time.sleep(0.1)
        else:
            return port
    msg = f"Failed to bind {socket} for {transport=} after {max_attempts} attempts."
    raise RuntimeError(msg)


@functools.cache
def wrap_handler(
    runner: Callable[[HandlerType, BinarySemaphore, Job]], lock: BinarySemaphore, handler: HandlerType
) -> Callable[[Job], CoroutineType[Any, Any, None]]:
    """
    A cache of run handlers.

    Args:
        runner: The function that calls and awaits the handler.
        lock: The lock to use for sending the reply.
        handler: The handler to which the runner is associated.

    Used by:
        - [async_kernel.Kernel.receive_msg_loop][]
    """

    @functools.wraps(handler)
    async def wrap_handler(job: Job) -> None:
        await runner(handler, lock, job)

    return wrap_handler


class KernelInterruptError(InterruptedError):
    "Raised to interrupt the kernel."

    # We subclass from InterruptedError so if the backend is a SelectorEventLoop it can catch the exception.
    # Other event loops don't appear to have this issue.


class Kernel(HasTraits):
    """
    A Jupyter kernel that supports [concurrent execution][async_kernel.Kernel.get_run_mode] providing an [IPython InteractiveShell][async_kernel.asyncshell.AsyncInteractiveShell].

    Info:
        Only one instance of a kernel is created at a time per subprocess. The instance can be obtained
        with `Kernel()` or [async_kernel.utils.get_kernel].

    Starting the kernel:
        The kernel should appear in the list of kernels just as other kernels are. Variants of the kernel
        can with custom configuration can be added at the [command line][async_kernel.command.command_line].

        === "From the shell"

            ``` shell
            async-kernel -f .
            ```

        === "Blocking"

            ```python
            Kernel().run()
            ```

        === "Inside a coroutine"

            ```python
            async with Kernel():
                await anyio.sleep_forever()
            ```

    Origins:
        - [IPyKernel Kernel][ipykernel.kernelbase.Kernel]
        - [IPyKernel IPKernelApp][ipykernel.kernelapp.IPKernelApp]
        - [IPyKernel IPythonKernel][ipykernel.ipkernel.IPythonKernel]
    """

    _instance: Self | None = None
    _initialised = False
    _interrupt_requested = False
    _last_interrupt_frame = None
    _stop_on_error_time: float = 0
    _interrupts: traitlets.Container[set[Callable[[], object]]] = Set()
    _settings: Dict[str, Any] = Dict()
    _sockets: Dict[SocketID, zmq.Socket] = Dict()
    callers: Dict[Literal[SocketID.shell, SocketID.control], Caller] = Dict()
    "The caller associated with the kernel once it has started."
    _ports: Dict[SocketID, int] = Dict()
    _execution_count = traitlets.Int(0)
    anyio_backend: traitlets.Container[Backend] = UseEnum(Backend)  # pyright: ignore[reportAssignmentType]
    "The anyio configured backend used to run the event loops."
    anyio_backend_options: Dict[Backend, dict[str, Any] | None] = Dict(allow_none=True)
    "Default options to use with [anyio.run][]. See also: `Kernel.handle_message_request`."
    _stabilisation_delay = traitlets.Float(0.1)
    "The sleep time before entering the message loop to allow the kernel to stabilise."
    help_links = Tuple()
    ""
    quiet = traitlets.Bool(True)
    "Only send stdout/stderr to output stream."
    connection_file: traitlets.TraitType[Path, Path | str] = traitlets.TraitType()
    """
    JSON file in which to store connection info [default: kernel-<pid>.json]

    This file will contain the IP, ports, and authentication key needed to connect
    clients to this kernel. By default, this file will be created in the security dir
    of the current profile, but can be specified by absolute path.
    """
    kernel_name: str | Unicode = Unicode()
    "The kernels name - if it contains 'trio' a trio backend will be used instead of an asyncio backend."

    ip = Unicode()
    """
    The kernel's IP address [default localhost].
    
    If the IP address is something other than localhost, then Consoles on other machines 
    will be able to connect to the Kernel, so be careful!
    """
    log = Instance(logging.LoggerAdapter)
    "The logging adapter."
    shell = Instance(AsyncInteractiveShell, ())
    "The interactive shell."
    session = Instance(Session, ())
    "Handles serialization and sending of messages."
    debugger = Instance(Debugger, ())
    "Handles (debug requests)[https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request]."
    comm_manager: Instance[CommManager] = Instance("async_kernel.comm.CommManager")
    ""
    transport: CaselessStrEnum[str] = CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp", config=True
    )
    event_started = Instance(Event, (), read_only=True)
    "An event that occurs when the kernel is started."
    event_stopped = Instance(Event, (), read_only=True)
    "An event that occurs when the kernel is stopped."

    def load_connection_info(self, info: dict[str, Any]) -> None:
        """
        Load connection info from a dict containing connection info.

        Typically this data comes from a connection file
        and is called by load_connection_file.

        Args:
            info: Dictionary containing connection_info. See the connection_file spec for details.
        """
        if self._ports:
            msg = "Connection info is already loaded!"
            raise RuntimeError(msg)
        self.transport = info.get("transport", self.transport)
        self.ip = info.get("ip") or self.ip
        for socket in SocketID:
            name = f"{socket}_port"
            if socket not in self._ports and name in info:
                self._ports[socket] = info[name]
        if "key" in info:
            key = info["key"]
            if isinstance(key, str):
                key = key.encode()
            assert isinstance(key, bytes)

            self.session.key = key
        if "signature_scheme" in info:
            self.session.signature_scheme = info["signature_scheme"]

    def __new__(cls, settings: dict | None = None, /) -> Self:  # noqa: ARG004
        #  There is only one instance (including subclasses).
        if not (instance := Kernel._instance):
            Kernel._instance = instance = super().__new__(cls)
        return instance  # pyright: ignore[reportReturnType]

    def __init__(self, settings: dict | None = None, /) -> None:
        if not self._initialised:
            self._initialised = True
            super().__init__()
            sys.excepthook = self.excepthook
            sys.unraisablehook = self.unraisablehook
            signal.signal(signal.SIGINT, self._signal_handler)
            if not os.environ.get("MPLBACKEND"):
                os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"
        if settings:
            self.load_settings(settings)

    @override
    def __repr__(self) -> str:
        info = [f"{k}={v}" for k, v in self.settings.items()]
        return f"{self.__class__.__name__}<{', '.join(info)}>"

    async def __aenter__(self) -> Self:
        """
        Start the kernel in the current asynchronous context.

        - Only one instance can (should) run at a time.
        - An instance can only be started once.
        - A new instance can be started after a previous instance has stopped and the context exited.

        Example:
            ```python
            async with Kernel() as kernel:
                await anyio.sleep_forever()
            ```
        """
        assert not self.event_stopped
        async with contextlib.AsyncExitStack() as stack:
            await stack.enter_async_context(self._start_in_context())
            self.__stack = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb) -> None:
        try:
            await self.__stack.__aexit__(exc_type, exc_value, exc_tb)
        finally:
            Kernel._instance = None

    @traitlets.default("log")
    def _default_log(self) -> LoggerAdapter[Logger]:
        return logging.LoggerAdapter(logging.getLogger(self.__class__.__name__))

    @traitlets.default("kernel_name")
    def _default_kernel_name(self) -> Literal[KernelName.trio, KernelName.asyncio]:
        try:
            if current_async_library() == "trio":
                return KernelName.trio
        except Exception:
            pass
        return KernelName.asyncio

    @traitlets.default("connection_file")
    def _default_connection_file(self) -> Path:
        return Path(jupyter_runtime_dir()).joinpath(f"kernel-{uuid.uuid4()}.json")

    @traitlets.default("comm_manager")
    def _default_comm_manager(self) -> CommManager:
        from async_kernel import comm  # noqa: PLC0415

        comm.set_comm()
        return comm.get_comm_manager()

    @traitlets.default("shell")
    def _default_shell(self) -> AsyncInteractiveShell:
        return AsyncInteractiveShell.instance()

    @traitlets.default("anyio_backend_options")
    def _default_anyio_backend_options(self):
        return {Backend.asyncio: {"use_uvloop": True} if importlib.util.find_spec("uvloop") else {}, Backend.trio: None}

    @traitlets.default("ip")
    def _default_ip(self) -> str:
        return str(self.connection_file) + "-ipc" if self.transport == "ipc" else localhost()

    @traitlets.default("help_links")
    def _default_help_links(self) -> tuple[dict[str, str], ...]:
        return (
            {
                "text": "Async Kernel Reference ",
                "url": "https://fleming79.github.io/async-kernel/",
            },
            {
                "text": "IPython Reference",
                "url": "https://ipython.readthedocs.io/en/stable/",
            },
            {
                "text": "IPython magic Reference",
                "url": "https://ipython.readthedocs.io/en/stable/interactive/magics.html",
            },
            {
                "text": "Matplotlib ipympl Reference",
                "url": "https://matplotlib.org/ipympl/",
            },
            {
                "text": "Matplotlib Reference",
                "url": "https://matplotlib.org/contents.html",
            },
        )

    @traitlets.observe("connection_file")
    def _observe_connection_file(self, change) -> None:
        if not self._ports and (path := self.connection_file).exists():
            self.log.debug("Loading connection file %s", path)
            with path.open("r") as f:
                self.load_connection_info(json.load(f))

    @traitlets.validate("ip")
    def _validate_ip(self, proposal) -> str:
        return "0.0.0.0" if (val := proposal["value"]) == "*" else val

    @traitlets.validate("connection_file")
    def _validate_connection_file(self, proposal) -> Path:
        return pathlib.Path(proposal.value)

    @property
    def settings(self) -> dict[str, Any]:
        "Settings that have been set to customise the behaviour of the kernel."
        return {k: getattr(self, k) for k in ("kernel_name", "connection_file")} | self._settings

    @property
    def execution_count(self) -> int:
        "The execution count in context of the current coroutine, else the current value if there isn't one in context."
        return utils.get_execution_count()

    @property
    def kernel_info(self) -> dict[str, str | dict[str, str | dict[str, str | int]] | Any | tuple[Any, ...] | bool]:
        ""
        return {
            "protocol_version": async_kernel.kernel_protocol_version,
            "implementation": "async_kernel",
            "implementation_version": async_kernel.__version__,
            "language_info": async_kernel.kernel_protocol_version_info,
            "banner": self.shell.banner,
            "help_links": self.help_links,
            "debugger": not utils.LAUNCHED_BY_DEBUGPY,
            "kernel_name": self.kernel_name,
        }

    def load_settings(self, settings: dict[str, Any]) -> None:
        """
        Load settings into the kernel.

        Permitted until the kernel async context has been entered.

        Args:
            settings:
                key: dotted.path.of.attribute.
                value: The value to set.
        """
        if self._sockets:
            msg = "It is too late to load settings!"
            raise RuntimeError(msg)
        settings_ = self._settings or {"kernel_name": self.kernel_name}
        for k, v in settings.items():
            settings_ |= utils.setattr_nested(self, k, v)
        self._settings = settings_

    def run(self, wait_exit: Callable[[], Awaitable] = anyio.sleep_forever, /):
        """
        Run the kernel (blocking).

        Args:
            wait_exit: The kernel will stop when the awaitable is complete.

        Warning:
            Running the kernel in a thread other than the 'MainThread' is permitted, but discouraged.

            - Blocking calls can only be interrupted in the 'MainThread' because [*'threads cannot be destroyed, stopped, suspended, resumed, or interrupted'*](https://docs.python.org/3/library/threading.html#module-threading).
            - Some libraries may assume the call is occurring in the 'MainThread'.
            - If there is an asyncio or trio event loop already running in the 'MainThread. Simply use `async with kernel` instead.
        """
        if getattr(self, "_started", False):
            raise RuntimeError
        self._started = True

        async def _run() -> None:
            async with self:
                with contextlib.suppress(anyio.get_cancelled_exc_class()):
                    await wait_exit()

        try:
            backend = Backend.trio if "trio" in self.kernel_name.lower() else Backend.asyncio
            anyio.run(_run, backend=backend, backend_options=self.anyio_backend_options.get(backend))
        finally:
            self.stop()

    @staticmethod
    def stop() -> None:
        """
        A [staticmethod][] to stop the running kernel.

        Once an instance of a kernel is stopped the instance cannot be restarted.
        Instead a new instance should be started.
        """
        if (instance := Kernel._instance) and (stop := getattr(instance, "_stop", None)):
            stop()

    @asynccontextmanager
    async def _start_in_context(self) -> AsyncGenerator[Self, Any]:
        """Start the kernel in an already running anyio event loop."""
        if self._sockets:
            msg = "Already started"
            raise RuntimeError(msg)
        assert self.shell
        self.anyio_backend = Backend(current_async_library())
        try:
            async with Caller(log=self.log, create=True, protected=True) as caller:
                self.callers[SocketID.shell] = caller
                await self._start_threads_and_open_sockets()
                assert len(self._sockets) == len(SocketID)
                self._write_connection_file()
                print(f"Kernel started: {self!r}")
                with self._iopub():
                    with anyio.CancelScope() as scope:
                        self._stop = lambda: caller.call_direct(scope.cancel, "Stopping kernel")
                        try:
                            self.event_started.set()
                            self.comm_manager.kernel = self
                            yield self
                        except BaseException:
                            if not scope.cancel_called:
                                raise
                        finally:
                            self.comm_manager.kernel = None
                            self.event_stopped.set()
                            self.callers.clear()
        finally:
            Kernel._instance = None
            AsyncInteractiveShell.clear_instance()
            Caller.stop_all(_stop_protected=True)
            Context.instance().term()
            print(f"Kernel stopped: {self!r}")

    def _signal_handler(self, signum, frame: FrameType | None) -> None:
        "Handle interrupt signals."
        if self._interrupt_requested:
            self._interrupt_requested = False
            if frame and frame.f_locals is self.shell.user_ns:
                raise KernelInterruptError
            if self._last_interrupt_frame is frame:
                # A blocking call that is not an execute_request
                raise KernelInterruptError
            self._last_interrupt_frame = frame

            def clear_last_interrupt_frame():
                if self._last_interrupt_frame is frame:
                    self._last_interrupt_frame = None

            if caller := self.callers.get(SocketID.shell):
                caller.call_direct(clear_last_interrupt_frame)
        else:
            signal.default_int_handler(signum, frame)

    async def _start_threads_and_open_sockets(self) -> None:
        heartbeat_ready = Event()
        pub_proxy_ready = Event()
        control_ready = Event()
        shell_ready = Event()

        def heartbeat(ready=heartbeat_ready.set):
            # ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels
            utils.mark_thread_pydev_do_not_trace()
            socket: Socket = Context.instance().socket(zmq.ROUTER)
            with self._bind_socket(SocketID.heartbeat, socket):
                ready()
                try:
                    zmq.proxy(socket, socket)
                except zmq.ContextTerminated:
                    return

        def pub_proxy(ready=pub_proxy_ready.set):
            # We use an internal proxy to collect pub messages for distribution.
            # Each thread needs to open its own socket to publish to the internal proxy.
            # When thread-safe sockets become available, this could be changed...
            # Ref: https://zguide.zeromq.org/docs/chapter2/#Working-with-Messages (fig 14)
            utils.mark_thread_pydev_do_not_trace()
            frontend: zmq.Socket = Context.instance().socket(zmq.XSUB)
            frontend.bind(Caller.iopub_url)
            iopub_socket: zmq.Socket = Context.instance().socket(zmq.XPUB)
            with self._bind_socket(SocketID.iopub, iopub_socket):
                ready()
                try:
                    zmq.proxy(frontend, iopub_socket)
                except zmq.ContextTerminated:
                    frontend.close(linger=500)

        async def run_in_shell_event_loop(ready=shell_ready.set):
            stdin_socket = Context.instance().socket(SocketType.ROUTER)
            with self._bind_socket(SocketID.stdin, stdin_socket), contextlib.suppress(anyio.get_cancelled_exc_class()):
                thread = threading.Thread(target=self.receive_msg_loop, args=(SocketID.shell, ready), daemon=True)
                thread.start()
                await anyio.sleep_forever()

        self.callers[SocketID.control] = Caller.start_new(
            backend=self.anyio_backend, name="ControlThread", protected=True
        )

        threading.Thread(target=heartbeat, name="heartbeat", daemon=True).start()
        heartbeat_ready.wait()

        threading.Thread(target=pub_proxy, name="iopub proxy", daemon=True).start()
        pub_proxy_ready.wait()

        threading.Thread(target=self.receive_msg_loop, args=(SocketID.control, control_ready.set), daemon=True).start()
        control_ready.wait()

        self.callers[SocketID.shell].call_soon(run_in_shell_event_loop)
        await shell_ready

    @contextlib.contextmanager
    def _bind_socket(self, socket_id: SocketID, socket: zmq.Socket) -> Generator[None, Any, None]:
        """
        Bind a zmq.Socket storing a reference to the socket and the port
        details and closing the socket on leaving the context.
        """
        if socket_id in self._sockets:
            msg = f"{socket_id=} is already loaded"
            raise RuntimeError(msg)
        socket.linger = 500
        port = bind_socket(socket=socket, transport=self.transport, ip=self.ip, port=self._ports.get(socket_id, 0))  # pyright: ignore[reportArgumentType]
        self._ports[socket_id] = port
        self.log.debug("%s socket on port: %i", socket_id, port)
        self._sockets[socket_id] = socket
        try:
            yield
        finally:
            socket.close(linger=500)
            self._sockets.pop(socket_id)

    def _write_connection_file(self) -> None:
        """Write connection info to JSON dict in self.connection_file."""
        if not (path := self.connection_file).exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            write_connection_file(
                str(path),
                transport=self.transport,
                ip=self.ip,
                key=self.session.key,
                signature_scheme=self.session.signature_scheme,
                kernel_name=self.kernel_name,
                **{f"{socket_id}_port": self._ports[socket_id] for socket_id in SocketID},
            )
            ip_files: list[pathlib.Path] = []
            if self.transport == "ipc":
                for s in self._sockets.values():
                    f = pathlib.Path(s.get_string(zmq.LAST_ENDPOINT).removeprefix("ipc://"))
                    assert f.exists()
                    ip_files.append(f)

            def cleanup_file_files() -> None:
                path.unlink(missing_ok=True)
                for f in ip_files:
                    f.unlink(missing_ok=True)

            atexit.register(cleanup_file_files)

    def _input_request(self, prompt: str, *, password=False) -> Any:
        job = utils.get_job()
        if not job["msg"].get("content", {}).get("allow_stdin", False):
            msg = "Stdin is not allowed in this context!"
            raise StdinNotImplementedError(msg)
        socket = self._sockets[SocketID.stdin]
        # Clear messages on the stdin socket
        while socket.get(SocketOption.EVENTS) & PollEvent.POLLIN:  # pyright: ignore[reportOperatorIssue]
            socket.recv_multipart(flags=Flag.DONTWAIT, copy=False)
        # Send the input request.
        assert self is not None
        self.session.send(
            stream=socket,
            msg_or_type="input_request",
            content={"prompt": prompt, "password": password},
            parent=job["msg"],  # pyright: ignore[reportArgumentType]
            ident=job["ident"],
        )
        # Poll for a reply.
        while not (socket.poll(100) & PollEvent.POLLIN):
            if self._last_interrupt_frame:
                raise KernelInterruptError
        return self.session.recv(socket)[1]["content"]["value"]  # pyright: ignore[reportOptionalSubscript]

    @contextlib.contextmanager
    def _iopub(self):
        # Save IO
        self._original_io = sys.stdout, sys.stderr, sys.displayhook, builtins.input, self.getpass

        builtins.input = self.raw_input
        getpass.getpass = self.getpass
        for name in ["stdout", "stderr"]:

            def flusher(string: str, name=name):
                "Publish stdio or stderr when flush is called"
                self.iopub_send(
                    msg_or_type="stream",
                    content={"name": name, "text": string},
                    ident=f"stream.{name}".encode(),
                )
                if not self.quiet and (echo := (sys.__stdout__ if name == "stdout" else sys.__stderr__)):
                    echo.write(string)
                    echo.flush()

            wrapper = OutStream(flusher=flusher)
            setattr(sys, name, wrapper)
        try:
            yield
        finally:
            # Reset IO
            sys.stdout, sys.stderr, sys.displayhook, builtins.input, getpass.getpass = self._original_io

    def receive_msg_loop(
        self, socket_id: Literal[SocketID.control, SocketID.shell], started: Callable[[], None]
    ) -> None:
        """
        Continuously receives and processes messages from a ZeroMQ ROUTER socket in a loop.

        Args:
            socket_id: The identifier for the socket to listen on (either control or shell).
            started: A callback function to be called once the socket is bound and ready to receive messages.

        Behavior:
            - Binds a ROUTER socket for the specified socket_id.
            - Calls the `started` callback after binding and a short delay.
            - Enters a loop to receive messages from the socket.
            - For each received message:
                - Determines the message type and retrieves the appropriate handler.
                - Constructs a job dictionary containing message and context information.
                - Determines the run mode for the message and dispatches the handler accordingly (queue, thread, task, or blocking).
            - Handles invalid messages and logs errors.
            - Exits the loop gracefully if the ZeroMQ context is terminated.

        Exception handling:
            - Handles and logs exceptions during message processing.
            - Breaks the loop on `zmq.ContextTerminated`.
        """
        utils.mark_thread_pydev_do_not_trace()
        msg: Message
        ident: list[bytes]
        caller = self.callers[socket_id]
        socket: Socket[Literal[SocketType.ROUTER]] = Context.instance().socket(SocketType.ROUTER)
        lock = BinarySemaphore()
        with self._bind_socket(socket_id, socket):
            started()
            time.sleep(self._stabilisation_delay)
            while True:
                try:
                    ident, msg = self.session.recv(socket, mode=zmq.BLOCKY, copy=False)  # pyright: ignore[reportAssignmentType]
                    msg_type = MsgType(msg["header"]["msg_type"])
                    handler = wrap_handler(self.run_handler, lock, self.get_handler(msg_type))
                    job: Job = {
                        "socket_id": socket_id,
                        "socket": socket,
                        "ident": ident,
                        "msg": msg,
                        "received_time": time.monotonic(),
                    }
                    run_mode = self.get_run_mode(msg_type, socket_id=socket_id, job=job)
                    self.schedule_job(caller, lock, handler, run_mode, job)
                    self.log.debug("%s %s %s %s %s", socket_id, msg_type, run_mode, handler, msg)
                except zmq.ContextTerminated:
                    break
                except Exception as e:
                    self.log.debug("Bad message on %s: %s", socket_id, e)
                    continue

    def schedule_job(self, caller: Caller, lock: BinarySemaphore, handler: HandlerType, run_mode: RunMode, job: Job, /):
        """
        Schedules a job to be executed by the handler using the specified run mode.

        Args:
            caller: The caller instance responsible for scheduling.
            lock: A binary semaphore for synchronization (not used in this method).
            handler: The function or callable to execute for the job.
            run_mode: The mode in which to run the job.
            job: The job instance or data to be passed to the handler.
        """
        match run_mode:
            case RunMode.blocking:
                caller.call_direct(handler, job)
            case RunMode.queue:
                caller.queue_call(handler, job)
            case RunMode.task:
                caller.call_soon(handler, job)
            case RunMode.thread:
                Caller.to_thread(handler, job)

    def get_run_mode(
        self,
        msg_type: MsgType,
        *,
        socket_id: Literal[SocketID.shell, SocketID.control] = SocketID.shell,
        job: Job | None = None,
    ) -> RunMode:
        """
        Determine the run mode for a given channel, message type and job.

        Args:
            socket_id: The socket ID the message was received on.
            msg_type: The type of the message.
            job: The job associated with the message, if any.

        Returns:
            The run mode for the message.
        """

        # TODO: Are any of these options worth including?
        # if mode_from_metadata := job["msg"]["metadata"].get("run_mode"):
        #     return RunMode( mode_from_metadata)
        # if mode_from_header := job["msg"]["header"].get("run_mode"):
        #     return RunMode( mode_from_header)
        match (socket_id, msg_type):
            case SocketID.shell, MsgType.shutdown_request | MsgType.debug_request:
                msg = f"{msg_type=} not allowed on shell!"
                raise ValueError(msg)
            case SocketID.control, MsgType.execute_request:
                return RunMode.task
            case _, MsgType.execute_request:
                if job:
                    if content := job["msg"].get("content", {}):
                        if (code := content.get("code")) and (mode_ := RunMode.get_mode(code)):
                            return mode_
                        if content.get("silent"):
                            return RunMode.task
                    if mode_ := set(utils.get_tags(job)).intersection(RunMode):
                        return RunMode(next(iter(mode_)))
                return RunMode.queue
            case _, MsgType.inspect_request | MsgType.complete_request | MsgType.is_complete_request:
                return RunMode.thread
            case _, MsgType.history_request:
                return RunMode.thread
            case _, MsgType.comm_msg:
                return RunMode.queue
            case _, MsgType.kernel_info_request | MsgType.comm_info_request | MsgType.comm_open | MsgType.comm_close:
                return RunMode.blocking
            case _, MsgType.debug_request:
                return RunMode.queue
            case _:
                return RunMode.blocking

    def all_concurrency_run_modes(
        self,
        socket_ids: Iterable[Literal[SocketID.shell, SocketID.control]] = (SocketID.shell, SocketID.control),
        msg_types: Iterable[MsgType] = MsgType,
    ) -> dict[
        Literal["SocketID", "MsgType", "RunMode"],
        tuple[SocketID, MsgType, RunMode | None],
    ]:
        """
        Generates a dictionary containing all combinations of SocketID, and MsgType,
        along with their corresponding RunMode (if available).
        """
        data: list[Any] = []
        for socket_id in socket_ids:
            for msg_type in msg_types:
                try:
                    mode = self.get_run_mode(msg_type, socket_id=socket_id)
                except ValueError:
                    mode = None
                data.append((socket_id, msg_type, mode))
        data_ = zip(*data, strict=True)
        return dict(zip(["SocketID", "MsgType", "RunMode"], data_, strict=True))

    def get_handler(self, msg_type: MsgType) -> HandlerType:
        if not callable(f := getattr(self, msg_type, None)):
            msg = f"A handler was not found for {msg_type=}"
            raise TypeError(msg)
        return f  # pyright: ignore[reportReturnType]

    async def run_handler(self, handler: HandlerType, lock: BinarySemaphore, job: Job[dict]) -> None:
        """
        Asynchronously runs a message handler for a given job, managing reply sending and execution state.

        Args:
            handler: The coroutine function to handle the job. Should accept a Job and return a dict or None.
                        - If the handler returns `None` no reply is sent
                        - otherwise the return value (dict expected) set as the `content` in a reply Message.
                        - If `status` is not provided in the content it is added as {'status': 'ok'}.
            lock: An async semaphore used to synchronize reply sending.
            job: The job dictionary containing message, socket, and identification information.

        Workflow:
            - Sets the current job context variable.
            - Sends a "busy" status message on the IOPub channel.
            - Awaits the handler; if it returns content, sends a reply using the provided lock.
            - On exception, sends an error reply and logs the exception.
            - Resets the job context variable.
            - Sends an "idle" status message on the IOPub channel.

        Notes:
            - Replies are sent even if exceptions occur in the handler.
            - The reply message type is derived from the original request type.
            - The function ensures proper synchronization when sending replies.
        """

        async def send_reply(job: Job[dict], content: dict, /) -> None:
            if "status" not in content:
                content["status"] = "ok"
            # Although we aren't sending from the thread where the socket belongs this still appears to be reliable.
            async with lock:
                msg = self.session.send(
                    stream=job["socket"],
                    msg_or_type=job["msg"]["header"]["msg_type"].replace("request", "reply"),
                    content=content,
                    parent=job["msg"]["header"],  # pyright: ignore[reportArgumentType]
                    ident=job["ident"],
                )
                if msg:
                    self.log.debug("*** _send_reply %s*** %s", job["socket_id"], msg)

        token = utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        try:
            self.iopub_send(msg_or_type="status", content={"execution_state": "busy"}, ident=self.topic("status"))
            if (content := await handler(job)) is not None:
                await send_reply(job, content)
        except Exception as e:
            await send_reply(job, error_to_content(e))
            self.log.exception("Exception in message handler:", exc_info=e)
        finally:
            utils._job_var.reset(token)  # pyright: ignore[reportPrivateUsage]
            self.iopub_send(
                msg_or_type="status", parent=job["msg"], content={"execution_state": "idle"}, ident=self.topic("status")
            )

    def iopub_send(
        self,
        msg_or_type: dict[str, Any] | str,
        content: Content | None = None,
        metadata: dict[str, Any] | None = None,
        parent: dict[str, Any] | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        ident: bytes | list[bytes] | None = None,
        buffers: list[bytes] | None = None,
    ) -> None:
        """Send a message on the zmq iopub socket."""
        if socket := Caller.iopub_sockets.get(thread := threading.current_thread()):
            msg = self.session.send(
                stream=socket,
                msg_or_type=msg_or_type,
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else utils.get_parent(),  # pyright: ignore[reportArgumentType]
                ident=ident,
                buffers=buffers,
            )
            if msg:
                self.log.debug(
                    "iopub_send: (thread=%s) msg_type:'%s', content: %s", thread.name, msg["msg_type"], msg["content"]
                )
        elif (caller := self.callers.get(SocketID.control)) and caller.thread is not thread:
            caller.call_direct(
                self.iopub_send,
                msg_or_type=msg_or_type,
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else None,
                ident=ident,
                buffers=buffers,
            )

    def topic(self, topic) -> bytes:
        """prefixed topic for IOPub messages."""
        return (f"kernel.{topic}").encode()

    async def kernel_info_request(self, job: Job[Content], /) -> Content:
        """Handle a [kernel info request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-info)."""
        return self.kernel_info

    async def comm_info_request(self, job: Job[Content], /) -> Content:
        """Handle a [comm info request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-info)."""
        c = job["msg"]["content"]
        target_name = c.get("target_name", None)
        comms = {
            k: {"target_name": v.target_name}
            for (k, v) in tuple(self.comm_manager.comms.items())
            if v.target_name == target_name or target_name is None
        }
        return {"comms": comms}

    async def execute_request(self, job: Job[ExecuteContent], /) -> Content:
        """Handle a [execute request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)."""
        c = job["msg"]["content"]
        if (job["received_time"] < self._stop_on_error_time) and not c.get("silent", False):
            self.log.info("Aborting execute_request: %s", job)
            return error_to_content(RuntimeError("Aborting due to prior exception")) | {
                "execution_count": self.execution_count
            }
        metadata = job["msg"].get("metadata") or {}
        if not (silent := c["silent"]):
            self._execution_count += 1
            utils._execution_count_var.set(self._execution_count)  # pyright: ignore[reportPrivateUsage]
            self.iopub_send(
                msg_or_type="execute_input",
                content={"code": c["code"], "execution_count": self.execution_count},
                parent=job["msg"],
                ident=self.topic("execute_input"),
            )
        caller = Caller()
        err = None
        with anyio.CancelScope() as scope:

            def cancel():
                if not silent:
                    caller.call_direct(scope.cancel, "Interrupted")

            try:
                self._interrupts.add(cancel)
                result = await self.shell.run_cell_async(
                    raw_cell=c["code"],
                    store_history=c.get("store_history", False),
                    silent=silent,
                    transformed_cell=self.shell.transform_cell(c["code"]),
                    shell_futures=True,
                )
            except (Exception, anyio.get_cancelled_exc_class()) as e:
                # A safeguard to catch exceptions not caught by the shell.
                err = KernelInterruptError() if self._last_interrupt_frame else e
            else:
                err = result.error_before_exec or result.error_in_exec if result else KernelInterruptError()
            self._interrupts.discard(cancel)
        if (err) and (
            (Tags.suppress_error in metadata.get("tags", ()))
            or (isinstance(err, anyio.get_cancelled_exc_class()) and (utils.get_execute_request_timeout() is not None))
        ):
            # Suppress the error due to either:
            # 1. tag
            # 2. timeout
            err = None
        content = {
            "status": "error" if err else "ok",
            "execution_count": self.execution_count,
            "user_expressions": self.shell.user_expressions(c.get("user_expressions", {})),
        }
        if err:
            content |= error_to_content(err)
            if (not silent) and c.get("stop_on_error"):
                try:
                    self._stop_on_error_time = math.inf
                    self.log.info("An error occurred in a non-silent execution request")
                    await anyio.sleep(0)
                finally:
                    self._stop_on_error_time = time.monotonic()
        return content

    async def complete_request(self, job: Job[Content], /) -> Content:
        """Handle a [completion request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#completion)."""
        c = job["msg"]["content"]
        code: str = c["code"]
        cursor_pos = c.get("cursor_pos") or len(code)
        with IPython.core.completer.provisionalcompleter():
            completions = self.shell.Completer.completions(code, cursor_pos)
            completions = list(IPython.core.completer.rectify_completions(code, completions))
        comps = [
            {
                "start": comp.start,
                "end": comp.end,
                "text": comp.text,
                "type": comp.type,
                "signature": comp.signature,
            }
            for comp in completions
        ]
        s, e = completions[0].start, completions[0].end if completions else (cursor_pos, cursor_pos)
        matches = [c.text for c in completions]
        return {
            "matches": matches,
            "cursor_end": e,
            "cursor_start": s,
            "metadata": {"_jupyter_types_experimental": comps},
        }

    async def is_complete_request(self, job: Job[Content], /) -> Content:
        """Handle a [is_complete request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness)."""
        status, indent_spaces = self.shell.input_transformer_manager.check_complete(job["msg"]["content"]["code"])
        content = {"status": status}
        if status == "incomplete":
            content["indent"] = " " * indent_spaces
        return content

    async def inspect_request(self, job: Job[Content], /) -> Content:
        """Handle a [inspect request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introspection)."""
        c = job["msg"]["content"]
        detail_level = int(c.get("detail_level", 0))
        omit_sections = set(c.get("omit_sections", []))
        name = token_at_cursor(c["code"], c["cursor_pos"])
        content = {"data": {}, "metadata": {}, "found": True}
        try:
            bundle = self.shell.object_inspect_mime(name, detail_level=detail_level, omit_sections=omit_sections)
            content["data"] = bundle
            if not self.shell.enable_html_pager:
                content["data"].pop("text/html")
        except KeyError:
            content["found"] = False
        return content

    async def history_request(self, job: Job[Content], /) -> Content:
        """Handle a [history request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#history)."""
        c = job["msg"]["content"]
        history_manager = self.shell.history_manager
        assert history_manager
        if c.get("hist_access_type") == "tail":
            hist = history_manager.get_tail(c["n"], raw=c.get("raw"), output=c.get("output"), include_latest=True)
        elif c.get("hist_access_type") == "range":
            hist = history_manager.get_range(
                c.get("session", 0),
                c.get("start", 1),
                c.get("stop", None),
                raw=c.get("raw", True),
                output=c.get("output", False),
            )
        elif c.get("hist_access_type") == "search":
            hist = history_manager.search(
                c.get("pattern"), raw=c.get("raw"), output=c.get("output"), n=c.get("n"), unique=c.get("unique")
            )
        else:
            hist = []
        return {"history": list(hist)}

    async def comm_open(self, job: Job[Content], /) -> None:
        """Handle a [comm open request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#opening-a-comm)."""
        self.comm_manager.comm_open(stream=job["socket"], ident=job["ident"], msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_msg(self, job: Job[Content], /) -> None:
        """Handle a [comm msg request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-messages)."""
        self.comm_manager.comm_msg(stream=job["socket"], ident=job["ident"], msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_close(self, job: Job[Content], /) -> None:
        """Handle a [comm close request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#tearing-down-comms)."""
        self.comm_manager.comm_close(stream=job["socket"], ident=job["ident"], msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def interrupt_request(self, job: Job[Content], /) -> Content:
        """Handle a [interrupt request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-interrupt) (control only)."""
        self._interrupt_requested = True
        if sys.platform == "win32":
            signal.raise_signal(signal.SIGINT)
            time.sleep(0)
        else:
            os.kill(os.getpid(), signal.SIGINT)
        for interrupter in tuple(self._interrupts):
            interrupter()
        return {}

    async def shutdown_request(self, job: Job[Content], /) -> Content:
        """Handle a [shutdown request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-shutdown) (control only)."""
        self.stop()
        return {"restart": job["msg"]["content"].get("restart", False)}

    async def debug_request(self, job: Job[Content], /) -> Content:
        """Handle a [debug request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request) (control only)."""
        return await self.debugger.process_request(job["msg"]["content"])

    def excepthook(self, etype, evalue, tb) -> None:
        """Handle an exception."""
        # write uncaught traceback to 'real' stderr, not zmq-forwarder
        traceback.print_exception(etype, evalue, tb, file=sys.__stderr__)

    def unraisablehook(self, unraisable: sys.UnraisableHookArgs, /) -> None:
        "Handle unraisable exceptions (during gc for instance)."
        exc_info = (
            unraisable.exc_type,
            unraisable.exc_value or unraisable.exc_type(unraisable.err_msg),
            unraisable.exc_traceback,
        )
        self.log.exception(unraisable.err_msg, exc_info=exc_info, extra={"object": unraisable.object})

    def raw_input(self, prompt="") -> Any:
        """
        Forward raw_input to frontends.

        Raises:
           IPython.core.error.StdinNotImplementedError: if active frontend doesn't support stdin.
        """
        return self._input_request(str(prompt), password=False)

    def getpass(self, prompt="") -> Any:
        """Forward getpass to frontends."""
        return self._input_request(prompt, password=True)

    def get_connection_info(self) -> dict[str, Any]:
        """Return the connection info as a dict."""
        with self.connection_file.open("r") as f:
            return json.load(f)

    def get_parent(self) -> Message[dict[str, Any]] | None:
        """
        A convenience method to access the 'message' in the current context if there is one.

        'parent' is the parameter name used by [Session.send][jupyter_client.session.Session.send] to provide context when sending a reply.

        See also:
            - [Kernel.iopub_send][async_kernel.Kernel.iopub_send]
            - [ipywidgets.Output][ipywidgets.widgets.widget_output.Output]:
                Uses `get_ipython().kernel.get_parent()` to obtain the `msg_id` which
                is used to 'capture' output when its context has been acquired.
        """
        return utils.get_parent()
