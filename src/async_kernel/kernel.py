from __future__ import annotations

import atexit
import builtins
import contextlib
import errno
import functools
import gc
import getpass
import importlib.util
import json
import logging
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

import aiologic
import anyio
import traitlets
import zmq
from aiologic import BinarySemaphore, Event
from aiologic.lowlevel import current_async_library
from IPython.core.error import StdinNotImplementedError
from jupyter_client import write_connection_file
from jupyter_client.localinterfaces import localhost
from jupyter_client.session import Session
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import CaselessStrEnum, CUnicode, Dict, HasTraits, Instance, Tuple, Unicode, UseEnum
from typing_extensions import override
from zmq import Flag, PollEvent, Socket, SocketOption, SocketType, ZMQError

import async_kernel
from async_kernel import Caller, utils
from async_kernel.asyncshell import (
    AsyncInteractiveShell,
    AsyncInteractiveSubshell,
    KernelInterruptError,
    SubshellManager,
    SubshellPendingManager,
)
from async_kernel.comm import CommManager
from async_kernel.common import Fixed
from async_kernel.debugger import Debugger
from async_kernel.iostream import OutStream
from async_kernel.typing import (
    Backend,
    Content,
    ExecuteContent,
    HandlerType,
    Job,
    KernelName,
    Message,
    MsgType,
    NoValue,
    RunMode,
    SocketID,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Iterable
    from types import CoroutineType, FrameType


__all__ = ["Kernel", "KernelInterruptError"]


def bind_socket(
    socket: Socket[SocketType],
    transport: Literal["tcp", "ipc"],
    ip: str,
    port: int = 0,
    max_attempts: int | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
) -> int:
    """
    Bind the socket to a port using the settings.

    'url = <transport>://<ip>:<port>'

    Args:
        socket: The socket to bind.
        transport: The type of transport.
        ip: Inserted in the url.
        port: The port to bind. If `0` will bind to a random port.
        max_attempts: The maximum number of attempts to bind the socket. If un-specified,
            defaults to 100 if port missing, else 2 attempts.

    Returns: The port that was bound.
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
def cache_wrap_handler(
    subshell_id: str | None,
    send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]],
    runner: Callable[[str | None, Callable[[Job, dict], CoroutineType[Any, Any, None]], HandlerType, Job]],
    handler: HandlerType,
) -> Callable[[Job], CoroutineType[Any, Any, None]]:
    @functools.wraps(handler)
    async def wrap_handler(job: Job) -> None:
        await runner(subshell_id, send_reply, handler, job)

    return wrap_handler


class Kernel(HasTraits, anyio.AsyncContextManagerMixin):
    """
    A Jupyter kernel that supports concurrent execution providing an [IPython InteractiveShell][async_kernel.asyncshell.AsyncInteractiveShell].

    Info:
        Only one instance of a kernel is created at a time per subprocess. The instance can be obtained
        with `Kernel()` or [get_kernel].

    Starting the kernel:
        The kernel should appear in the list of kernels just as other kernels are. Variants of the kernel
        can with custom configuration can be added at the [command line][command.command_line].

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

    Warning:
        Starting the kernel outside the main thread has the following implicatations:
            - Execute requests won't be run in the main thread.
            - Interrupts via signals won't work, so thread blocking calls in the shell cannot be interrupted.

    Origins:
        - [IPyKernel Kernel][ipykernel.kernelbase.Kernel]
        - [IPyKernel IPKernelApp][ipykernel.kernelapp.IPKernelApp]
        - [IPyKernel IPythonKernel][ipykernel.ipkernel.IPythonKernel]
    """

    _instance: Self | None = None
    _initialised = False
    _interrupt_requested: bool | Literal["FORCE"] = False
    _last_interrupt_frame = None
    _settings = Fixed(dict)
    _zmq_context = Fixed(zmq.Context)
    _sockets: Fixed[Self, dict[SocketID, zmq.Socket]] = Fixed(dict)
    _ports: Fixed[Self, dict[SocketID, int]] = Fixed(dict)

    interrupts: Fixed[Self, set[Callable[[], object]]] = Fixed(set)
    "A set for callables can be added to run code when a kernel interrupt is initiated (control thread)."

    callers: Fixed[Self, dict[Literal[SocketID.shell, SocketID.control], Caller]] = Fixed(dict)
    "The caller associated with the kernel once it has started."
    ""
    subshell_manager = Fixed(SubshellManager)
    "Dedicated to management of sub shells."

    # Public traits
    anyio_backend: traitlets.Container[Backend] = UseEnum(Backend)  # pyright: ignore[reportAssignmentType]
    "The anyio configured backend used to run the event loops."

    anyio_backend_options: Dict[Backend, dict[str, Any] | None] = Dict(allow_none=True)
    "Default options to use with [anyio.run][]. See also: `Kernel.handle_message_request`."

    help_links = Tuple()
    ""
    quiet = traitlets.Bool(True)
    "Only send stdout/stderr to output stream."

    print_kernel_messages = traitlets.Bool(True)
    "When enabled the kernel will print startup, shutdown and terminal errors."

    connection_file: traitlets.TraitType[Path, Path | str] = traitlets.TraitType()
    """
    JSON file in which to store connection info 
    
    `"kernel-<pid>.json"`

    This file will contain the IP, ports, and authentication key needed to connect
    clients to this kernel. By default, this file will be created in the security dir
    of the current profile, but can be specified by absolute path.
    """

    kernel_name = CUnicode()
    "The kernels name - if it contains 'trio' a trio backend will be used instead of an asyncio backend."

    ip = Unicode()
    """
    The kernel's IP address [default localhost].
    
    If the IP address is something other than localhost, then Consoles on other machines 
    will be able to connect to the Kernel, so be careful!
    """

    transport: CaselessStrEnum[str] = CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp"
    )
    "Transport for sockets."

    log = Instance(logging.LoggerAdapter)
    "The logging adapter."

    # Public fixed
    main_shell = Fixed(lambda _: AsyncInteractiveShell.instance())
    "The interactive shell."

    session = Fixed(Session)
    "Handles serialization and sending of messages."

    debugger = Fixed(Debugger)
    "Handles [debug requests](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request)."

    comm_manager = Fixed(CommManager)
    "Creates [async_kernel.comm.Comm][] instances and maintains a mapping to `comm_id` to `Comm` instances."

    event_started = Fixed(Event)
    "An event that occurs when the kernel is started."

    event_stopped = Fixed(Event)
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
            if not os.environ.get("MPLBACKEND"):
                os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"
        if settings:
            self.load_settings(settings)

    @override
    def __repr__(self) -> str:
        info = [f"{k}={v}" for k, v in self.settings.items()]
        return f"{self.__class__.__name__}<{', '.join(info)}>"

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

    @traitlets.default("anyio_backend_options")
    def _default_anyio_backend_options(self):
        use_uv = importlib.util.find_spec("winloop") or importlib.util.find_spec("uvloop")
        return {Backend.asyncio: {"use_uvloop": True} if use_uv else {}, Backend.trio: None}

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
    def shell(self) -> AsyncInteractiveShell | AsyncInteractiveSubshell:
        """
        The shell given the current context.

        Notes:
            - The `subshell_id` of the main shell is `None`.
        """
        return self.subshell_manager.get_shell()

    @property
    def caller(self) -> Caller:
        "The caller for the shell channel."
        return self.callers[SocketID.shell]

    @property
    def kernel_info(self) -> dict[str, str | dict[str, str | dict[str, str | int]] | Any | tuple[Any, ...] | bool]:
        "A dict of detail sent in reply to for a 'kernel_info_request'."
        return {
            "protocol_version": async_kernel.kernel_protocol_version,
            "implementation": "async_kernel",
            "implementation_version": async_kernel.__version__,
            "language_info": async_kernel.kernel_protocol_version_info,
            "banner": self.shell.banner,
            "help_links": self.help_links,
            "debugger": not utils.LAUNCHED_BY_DEBUGPY,
            "kernel_name": self.kernel_name,
            "supported_features": ["kernel subshells"],
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
        self._settings.update(settings_)

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
                await wait_exit()

        if not self.trait_has_value("anyio_backend") and "trio" in self.kernel_name.lower():
            self.anyio_backend = Backend.trio
        backend = self.anyio_backend
        backend_options = self.anyio_backend_options.get(backend)
        anyio.run(_run, backend=backend, backend_options=backend_options)

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
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        """Start the kernel in an already running anyio event loop."""
        assert self.main_shell
        self.anyio_backend = Backend(current_async_library())
        # Callers
        caller = Caller("manual", name="Shell", protected=True, log=self.log, zmq_context=self._zmq_context)
        self.callers[SocketID.shell] = caller
        self.callers[SocketID.control] = caller.get(name="Control", log=self.log, protected=True)
        start = Event()
        try:
            async with caller:
                self._start_hb_iopub_shell_control_threads(start)
                with self._bind_socket(SocketID.stdin):
                    assert len(self._sockets) == len(SocketID)
                    self._write_connection_file()
                    if self.print_kernel_messages:
                        print(f"Kernel started: {self!r}")
                    with self._iopub():
                        with anyio.CancelScope() as scope:
                            self._stop = lambda: caller.call_direct(scope.cancel, "Stopping kernel")
                            sys.excepthook = self.excepthook
                            sys.unraisablehook = self.unraisablehook
                            try:
                                sig = signal.signal(signal.SIGINT, self._signal_handler)
                            except ValueError:
                                sig = None
                            self.comm_manager.patch_comm()
                            try:
                                self.comm_manager.kernel = self
                                start.set()
                                self.event_started.set()
                                yield self
                            except BaseException:
                                if not scope.cancel_called:
                                    raise
                            finally:
                                if sig:
                                    signal.signal(signal.SIGINT, sig)
                                self.comm_manager.kernel = None
                                self.event_stopped.set()
        finally:
            self.shell.reset(new_session=False)
            self.subshell_manager.stop_all_subshells(force=True)
            self.callers.clear()
            Kernel._instance = None
            AsyncInteractiveShell.clear_instance()
            with anyio.CancelScope(shield=True):
                await anyio.sleep(0.1)
            self._zmq_context.term()
            if self.print_kernel_messages:
                print(f"Kernel stopped: {self!r}")
            gc.collect()

    def _interrupt_now(self, *, force=False):
        """
        Request an interrupt of the currently running shell thread.

        If called from the main thread, sets the interrupt request flag and sends a SIGINT signal
        to the current process. On Windows, uses `signal.raise_signal`; on other platforms, uses `os.kill`.
        If `force` is True, sets the interrupt request flag to "FORCE".

        Args:
            force: If True, requests a forced interrupt. Defaults to False.
        """
        # Restricted this to when the shell is running in the main thread.
        if self.callers[SocketID.shell].thread is threading.main_thread():
            self._interrupt_requested = "FORCE" if force else True
            if sys.platform == "win32":
                signal.raise_signal(signal.SIGINT)
                time.sleep(0)
            else:
                os.kill(os.getpid(), signal.SIGINT)

    @aiologic.lowlevel.enable_signal_safety
    def _signal_handler(self, signum, frame: FrameType | None) -> None:
        "Handle interrupt signals."

        match self._interrupt_requested:
            case "FORCE":
                self._interrupt_requested = False
                raise KernelInterruptError
            case True:
                if frame and frame.f_locals is self.shell.user_ns:
                    self._interrupt_requested = False
                    raise KernelInterruptError
                self._last_interrupt_frame = frame

                def clear_last_interrupt_frame():
                    if self._last_interrupt_frame is frame:
                        self._last_interrupt_frame = None

                def re_raise():
                    if self._last_interrupt_frame is frame:
                        self._interrupt_now(force=True)

                # Race to check if the main thread should be interrupted.
                self.callers[SocketID.shell].call_direct(clear_last_interrupt_frame)
                self.callers[SocketID.control].call_later(1, re_raise)
            case False:
                signal.default_int_handler(signum, frame)

    def _start_hb_iopub_shell_control_threads(self, start: Event) -> None:
        def heartbeat(ready: Event) -> None:
            # ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels
            utils.mark_thread_pydev_do_not_trace()
            with self._bind_socket(SocketID.heartbeat) as socket:
                ready.set()
                try:
                    zmq.proxy(socket, socket)
                except zmq.ContextTerminated:
                    return

        def pub_proxy(ready: Event) -> None:
            # We use an internal proxy to collect pub messages for distribution.
            # Each thread needs to open its own socket to publish to the internal proxy.
            # When thread-safe sockets become available, this could be changed...
            # Ref: https://zguide.zeromq.org/docs/chapter2/#Working-with-Messages (fig 14)
            utils.mark_thread_pydev_do_not_trace()
            frontend: zmq.Socket = self._zmq_context.socket(zmq.XSUB)
            frontend.bind(Caller.iopub_url)
            with self._bind_socket(SocketID.iopub) as iopub_socket:
                ready.set()
                try:
                    zmq.proxy(frontend, iopub_socket)
                except zmq.ContextTerminated:
                    frontend.close(linger=50)

        hb_ready, iopub_ready = (Event(), Event())
        threading.Thread(target=heartbeat, name="heartbeat", args=[hb_ready]).start()
        hb_ready.wait()
        threading.Thread(target=pub_proxy, name="iopub proxy", args=[iopub_ready]).start()
        iopub_ready.wait()
        # message loops
        for socket_id in [SocketID.shell, SocketID.control]:
            ready = Event()
            name = f"{socket_id}-receive_msg_loop"
            threading.Thread(target=self.receive_msg_loop, name=name, args=(socket_id, ready, start)).start()
            ready.wait()

    @contextlib.contextmanager
    def _bind_socket(self, socket_id: SocketID) -> Generator[Any | Socket[Any], Any, None]:
        """
        Bind a zmq.Socket storing a reference to the socket and the port
        details and closing the socket on leaving the context.
        """
        if socket_id in self._sockets:
            msg = f"{socket_id=} is already loaded"
            raise RuntimeError(msg)
        match socket_id:
            case SocketID.shell | SocketID.control | SocketID.heartbeat | SocketID.stdin:
                socket_type = zmq.ROUTER
            case SocketID.iopub:
                socket_type = zmq.XPUB
        socket: zmq.Socket = self._zmq_context.socket(socket_type)
        socket.linger = 50
        port = bind_socket(socket=socket, transport=self.transport, ip=self.ip, port=self._ports.get(socket_id, 0))  # pyright: ignore[reportArgumentType]
        self._ports[socket_id] = port
        self.log.debug("%s socket on port: %i", socket_id, port)
        self._sockets[socket_id] = socket
        try:
            yield socket
        finally:
            socket.close(linger=50)
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
                parent=parent if parent is not NoValue else self.get_parent(),  # pyright: ignore[reportArgumentType]
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

    def receive_msg_loop(
        self, socket_id: Literal[SocketID.control, SocketID.shell], ready: Event, start: Event
    ) -> None:
        "Opens a zmq socket for socket_id, receives messages and calls the message handler."

        if not utils.LAUNCHED_BY_DEBUGPY:
            utils.mark_thread_pydev_do_not_trace()

        session, log, message_handler = self.session, self.log, self.msg_handler
        with self._bind_socket(socket_id) as socket:
            lock = BinarySemaphore()

            async def send_reply(job: Job, content: dict, /) -> None:
                if "status" not in content:
                    content["status"] = "ok"
                async with lock:
                    msg = session.send(
                        stream=socket,
                        msg_or_type=job["msg"]["header"]["msg_type"].replace("request", "reply"),
                        content=content,
                        parent=job["msg"],  # pyright: ignore[reportArgumentType]
                        ident=job["ident"],
                    )
                    if msg:
                        log.debug("*** send_reply %s*** %s", socket_id, msg)

            ready.set()
            start.wait()
            while True:
                try:
                    ident, msg = session.recv(socket, mode=zmq.BLOCKY, copy=False)
                    try:
                        subshell_id = msg["content"]["subshell_id"]  # pyright: ignore[reportOptionalSubscript]
                    except KeyError:
                        try:
                            subshell_id = msg["header"]["subshell_id"]  # pyright: ignore[reportOptionalSubscript]
                        except KeyError:
                            subshell_id = None
                    job = Job(received_time=time.monotonic(), socket_id=socket_id, msg=msg, ident=ident)  # pyright: ignore[reportArgumentType]
                    message_handler(subshell_id, socket_id, MsgType(job["msg"]["header"]["msg_type"]), job, send_reply)
                except zmq.ContextTerminated:
                    break
                except Exception as e:
                    log.debug("Bad message on %s: %s", socket_id, e)
                    continue

    def msg_handler(
        self,
        subshell_id: str | None,
        socket_id: Literal[SocketID.shell, SocketID.control],
        msg_type: MsgType,
        job: Job,
        send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]],
        /,
    ):
        """Schedule a message to be executed."""
        # Note: There are never any active pending trackers in this context.
        handler = cache_wrap_handler(subshell_id, send_reply, self.run_handler, self.get_handler(msg_type))
        run_mode = self.get_run_mode(msg_type, socket_id=socket_id, job=job)
        match run_mode:
            case RunMode.direct:
                self.callers[socket_id].call_direct(handler, job)
            case RunMode.queue:
                self.callers[socket_id].queue_call(handler, job)
            case RunMode.task:
                self.callers[socket_id].call_soon(handler, job)
            case RunMode.thread:
                self.callers[socket_id].to_thread(handler, job)
        self.log.debug("%s %s %s %s", msg_type, handler, run_mode, job)

    def get_handler(self, msg_type: MsgType) -> HandlerType:
        if not callable(f := getattr(self, msg_type, None)):
            msg = f"A handler was not found for {msg_type=}"
            raise TypeError(msg)
        return f  # pyright: ignore[reportReturnType]

    async def run_handler(
        self,
        subshell_id: str | None,
        send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]],
        handler: HandlerType,
        job: Job[dict],
    ) -> None:
        """
        Asynchronously run a message handler for a given job, managing reply sending and execution state.

        Args:
            subshell_id: The id of the subshell to set the context of the handler.
            send_reply: A coroutine function responsible for sending the reply.
            handler: A coroutine function to handle the job / message.

                - It is a method on the kernel whose name corresponds to the [message type that it handles][async_kernel.typing.MsgType].
                - The handler should return a dict to use as 'content'in a reply.
                - If status is not included in the dict it gets added automatically as `{'status': 'ok'}`.
                - If a reply is not expected the handler should return `None`.

            job: The job dictionary containing message, socket, and identification information.

        Workflow:
            - Sets the current job and subshell_id context variables.
            - Sends a "busy" status message on the IOPub channel.
            - Awaits the handler; if the handler returns a content dict, a reply is sent using send_reply.
            - On exception, sends an error reply and logs the exception.
            - Resets the job and subshell_id context variables.
            - Sends an "idle" status message on the IOPub channel.

        Notes:
            - Replies are sent even if exceptions occur in the handler.
            - The reply message type is derived from the original request type.
        """
        job_token = utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        subshell_token = SubshellPendingManager._contextvar.set(subshell_id)  # pyright: ignore[reportPrivateUsage]

        try:
            self.iopub_send(
                msg_or_type="status",
                parent=job["msg"],
                content={"execution_state": "busy"},
                ident=self.topic(topic="status"),
            )
            if (content := await handler(job)) is not None:
                await send_reply(job, content)
        except Exception as e:
            await send_reply(job, utils.error_to_content(e))
            self.log.exception("Exception in message handler:", exc_info=e)
        finally:
            utils._job_var.reset(job_token)  # pyright: ignore[reportPrivateUsage]
            SubshellPendingManager._contextvar.reset(subshell_token)  # pyright: ignore[reportPrivateUsage]
            self.iopub_send(
                msg_or_type="status",
                parent=job["msg"],
                content={"execution_state": "idle"},
                ident=self.topic("status"),
            )
            del job

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
        # receive_msg_loop - DEBUG WARNING

        # TODO: Are any of these options worth including?
        # if mode_from_metadata := job["msg"]["metadata"].get("run_mode"):
        #     return RunMode( mode_from_metadata)
        # if mode_from_header := job["msg"]["header"].get("run_mode"):
        #     return RunMode( mode_from_header)
        match (socket_id, msg_type):
            case _, MsgType.comm_msg:
                return RunMode.queue
            case SocketID.control, MsgType.execute_request:
                return RunMode.queue
            case _, MsgType.execute_request:
                if job:
                    if content := job["msg"].get("content", {}):
                        if code := content.get("code"):
                            try:
                                if (code := code.strip().split("\n", maxsplit=1)[0]).startswith(("# ", "##")):
                                    return RunMode(code[2:])
                                if code.startswith("RunMode."):
                                    return RunMode(code.removeprefix("RunMode."))
                            except ValueError:
                                pass
                        if content.get("silent"):
                            return RunMode.task
                    if mode_ := set(utils.get_tags(job)).intersection(RunMode):
                        return RunMode(next(iter(mode_)))
                return RunMode.queue
            case (
                SocketID.shell,
                MsgType.shutdown_request
                | MsgType.debug_request
                | MsgType.create_subshell_request
                | MsgType.delete_subshell_request
                | MsgType.list_subshell_request,
            ):
                msg = f"{msg_type=} not allowed on shell!"
                raise ValueError(msg)
            case _, MsgType.debug_request:
                return RunMode.queue
            case (
                _,
                MsgType.complete_request
                | MsgType.inspect_request
                | MsgType.history_request
                | MsgType.create_subshell_request
                | MsgType.delete_subshell_request
                | MsgType.is_complete_request,
            ):
                return RunMode.thread
            case _:
                pass
        return RunMode.direct

    def all_concurrency_run_modes(
        self,
        socket_ids: Iterable[Literal[SocketID.shell, SocketID.control]] = (SocketID.shell, SocketID.control),
        msg_types: Iterable[MsgType] = MsgType,
    ) -> dict[
        Literal["SocketID", "MsgType", "RunMode"],
        tuple[SocketID, MsgType, RunMode | None],
    ]:
        """
        Generates a dictionary containing all combinations of SocketID, and MsgType, along with their
        corresponding RunMode (if available).
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
        return await self.shell.execute_request(**job["msg"]["content"])  # pyright: ignore[reportArgumentType]

    async def complete_request(self, job: Job[Content], /) -> Content:
        """Handle a [completion request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#completion)."""
        return await self.shell.do_complete_request(
            code=job["msg"]["content"].get("code", ""), cursor_pos=job["msg"]["content"].get("cursor_pos", 0)
        )

    async def is_complete_request(self, job: Job[Content], /) -> Content:
        """Handle a [is_complete request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness)."""
        return await self.shell.is_complete_request(job["msg"]["content"].get("code", ""))

    async def inspect_request(self, job: Job[Content], /) -> Content:
        """Handle a [inspect request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introspection)."""
        c = job["msg"]["content"]
        return await self.shell.inspect_request(
            code=c.get("code", ""),
            cursor_pos=c.get("cursor_pos", 0),
            detail_level=c.get("detail_level", 0),
        )

    async def history_request(self, job: Job[Content], /) -> Content:
        """Handle a [history request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#history)."""
        return await self.shell.history_request(**job["msg"]["content"])

    async def comm_open(self, job: Job[Content], /) -> None:
        """Handle a [comm open request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#opening-a-comm)."""
        self.comm_manager.comm_open(stream=None, ident=None, msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_msg(self, job: Job[Content], /) -> None:
        """Handle a [comm msg request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-messages)."""
        self.comm_manager.comm_msg(stream=None, ident=None, msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_close(self, job: Job[Content], /) -> None:
        """Handle a [comm close request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#tearing-down-comms)."""
        self.comm_manager.comm_close(stream=None, ident=None, msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def interrupt_request(self, job: Job[Content], /) -> Content:
        """Handle an [interrupt request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-interrupt) (control only)."""
        self._interrupt_now()
        while self.interrupts:
            try:
                self.interrupts.pop()()
            except Exception:
                pass
        return {}

    async def shutdown_request(self, job: Job[Content], /) -> Content:
        """Handle a [shutdown request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-shutdown) (control only)."""
        self.stop()
        return {"restart": job["msg"]["content"].get("restart", False)}

    async def debug_request(self, job: Job[Content], /) -> Content:
        """Handle a [debug request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request) (control only)."""
        return await self.debugger.process_request(job["msg"]["content"])

    async def create_subshell_request(self: Kernel, job: Job[Content], /) -> Content:
        """Handle a [create subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#create-subshell) (control only)."""

        return {"subshell_id": self.subshell_manager.create_subshell(protected=False).subshell_id}

    async def delete_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle a [delete subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#delete-subshell) (control only)."""
        self.subshell_manager.delete_subshell(job["msg"]["content"]["subshell_id"])
        return {}

    async def list_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle a [list subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#list-subshells) (control only)."""
        return {"subshell_id": list(self.subshell_manager.list_subshells())}

    def excepthook(self, etype, evalue, tb) -> None:
        """Handle an exception."""
        # write uncaught traceback to 'real' stderr, not zmq-forwarder
        if self.print_kernel_messages:
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
            - [Kernel.iopub_send][Kernel.iopub_send]
            - [ipywidgets.Output][ipywidgets.widgets.widget_output.Output]:
                Uses `get_ipython().kernel.get_parent()` to obtain the `msg_id` which
                is used to 'capture' output when its context has been acquired.
        """
        return utils.get_parent()
