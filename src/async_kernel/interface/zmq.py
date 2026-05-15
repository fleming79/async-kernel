"A collection of objects defining the kernel interface using zmq sockets."

from __future__ import annotations

import atexit
import contextlib
import errno
import importlib.util
import json
import os
import pathlib
import signal
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING, Any, Literal, Self

import zmq
from aiologic import BinarySemaphore
from aiologic.lowlevel import AsyncLibraryNotFoundError, current_async_library, enable_signal_safety
from IPython.core.application import BaseIPythonApplication
from IPython.core.error import StdinNotImplementedError
from IPython.core.profiledir import ProfileDir
from IPython.core.shellapp import InteractiveShellApp, shell_aliases, shell_flags
from jupyter_client import write_connection_file
from jupyter_client.connect import ConnectionFileMixin
from jupyter_client.localinterfaces import localhost
from jupyter_client.session import Session
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import traitlets
from typing_extensions import override
from zmq import Flag, PollEvent, Socket, SocketOption, SocketType, ZMQError

import async_kernel.event_loop
from async_kernel import Kernel, utils
from async_kernel.asyncshell import AsyncInteractiveShell
from async_kernel.caller import Caller
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.interface.base import BaseKernelInterface, DictValueLiteralEval
from async_kernel.kernelspec import expand_path
from async_kernel.typing import Backend, Channel, Content, Hosts, Job, Message, MsgHeader, NoValue, RunSettings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator
    from types import FrameType


__all__ = ["ZMQKernelInterface"]


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


class PathTrait(traitlets.TraitType[pathlib.Path, pathlib.Path | str]):
    "A trait for a [pathlib.Path][] that casts strings to paths."

    def validate(self, obj: traitlets.HasTraits, value: pathlib.Path | str) -> Path:
        if not isinstance(value, pathlib.Path):
            value = expand_path(value)
        if self.name and obj.trait_has_value(self.name) and getattr(obj, self.name) == value:
            return getattr(obj, self.name)
        return value

    @override
    def from_string(self, s: str) -> pathlib.Path:
        return expand_path(s)


class ZMQKernelInterface(BaseKernelInterface, ConnectionFileMixin, BaseIPythonApplication, InteractiveShellApp):  # pyright: ignore[reportUnsafeMultipleInheritance]
    description = traitlets.Unicode(
        "async-kernel: A Jupyter kernel providing an asynchronous IPython shell.",
    ).tag(config=True)
    "A description to use for the command line interface."

    classes = [Kernel, AsyncInteractiveShell, InteractiveShellApp, ProfileDir, Session]  # noqa: RUF012

    aliases = (
        shell_aliases
        | BaseIPythonApplication.aliases
        | {
            "name": "ZMQKernelInterface.name",
            "start_interface": "ZMQKernelInterface.start_interface",
            "kernel_class": "ZMQKernelInterface.kernel_class",
            "quiet": "ZMQKernelInterface.quiet",
            "timeout": "AsyncInteractiveShell.timeout",
            "f": "ZMQKernelInterface.connection_file",
            "connection_file": "ZMQKernelInterface.connection_file",
            "ip": "ZMQKernelInterface.ip",
            "hb": "ZMQKernelInterface.hb_port",
            "shell": "ZMQKernelInterface.shell_port",
            "iopub": "ZMQKernelInterface.iopub_port",
            "stdin": "ZMQKernelInterface.stdin_port",
            "control": "ZMQKernelInterface.control_port",
            "transport": "ZMQKernelInterface.transport",
            "backend": "ZMQKernelInterface.backend",
            "host": "ZMQKernelInterface.host",
            "backend_options": "ZMQKernelInterface.backend_options",
            "host_options": "ZMQKernelInterface.host_options",
        }
    )
    flags = shell_flags | BaseIPythonApplication.flags

    # Disabled unsupported InteractiveShellApp configurable traits

    # Remove associated aliases and flags
    for _n in ["matplotlib", "gui", "pylab", "pdb", "autoindent", "pprint"]:
        aliases.pop(_n, None)
        flags.pop(_n, None)
    del _n  # cleanup symbol

    matplotlib = None
    "Not supported. If a gui event loop is required use 'host'."
    gui = None
    "Not supported. If a gui event loop is required use 'host'."

    connection_file = PathTrait().tag(config=True)
    """JSON file in which to store connection info."""

    session = traitlets.Instance(Session)

    transport: traitlets.CaselessStrEnum[str] = traitlets.CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp"
    ).tag(config=True)
    "Transport for sockets."

    host: traitlets.TraitType[Hosts | None, Hosts | None] = traitlets.UseEnum(
        Hosts, default_value=None, allow_none=True
    ).tag(config=True)
    "The name of the (gui) event loop if one is used."

    host_options = DictValueLiteralEval(allow_none=True).tag(config=True)
    "Options for starting the loop."

    backend_options = DictValueLiteralEval(allow_none=True).tag(config=True)
    "Options for starting the backend."

    pre_start = traitlets.List(["init_path", "init_extensions", "init_code"]).tag(config=True)
    ""

    _initialized = False
    _zmq_context = Fixed(zmq.Context)
    _interrupt_requested: bool | Literal["FORCE"] = False
    _iopub_url = "inproc://iopub-capture"
    _sockets: Fixed[Self, dict[Channel, zmq.Socket]] = Fixed(dict)

    @traitlets.default("kernel")
    def default_kernel(self):
        self.initialize()
        return self.kernel_class()

    @traitlets.default("backend")
    def _default_backend(self) -> Backend:
        try:
            return Backend(current_async_library())
        except AsyncLibraryNotFoundError:
            if (
                not self.host
                and not self.trait_has_value("backend_options")
                and (importlib.util.find_spec("winloop") or importlib.util.find_spec("uvloop"))
            ):
                self.backend_options["use_uvloop"] = True
            return Backend.asyncio

    @traitlets.default("banner")
    @override
    def _default_banner(self):
        return super()._default_banner() + f" host:{str(self.host or '')!r} f:{str(self.connection_file)!r}"

    @traitlets.validate("connection_file")
    def _validate_connection_file(self, proposal: dict) -> Path:

        if self._sockets and self.trait_has_value("connection_file") and proposal["value"] != self.connection_file:
            msg = "It is too late to set the connection file!"
            raise RuntimeError(msg)
        return proposal["value"]

    @traitlets.default("connection_file")
    def _default_connection_file(self) -> Path:
        return Path(jupyter_runtime_dir()).joinpath(f"kernel-{os.getpid()}.json")

    @traitlets.observe("connection_file")
    def _observe_connection_file(self, change) -> None:
        if (path := change["new"]).exists() and path != change["old"] and not self._sockets:
            self.log.debug("Loading connection file %s", path)
            self.load_connection_info(json.loads(path.read_bytes()))

    @override
    def load_connection_info(self, info: dict[str, Any]) -> None:
        if self._sockets:
            msg = "It is too late to configure!"
            raise RuntimeError(msg)
        super().load_connection_info(info)

    @traitlets.validate("ip")
    def _validate_ip(self, proposal) -> str:
        return "0.0.0.0" if (val := proposal["value"]) == "*" else val

    @traitlets.default("ip")
    def _default_ip(self) -> str:
        return str(self.connection_file) + "-ipc" if self.transport == "ipc" else localhost()

    @traitlets.default("session")
    def _default_session(self) -> Session:
        session = Session(config=self.config)
        if not session.trait_has_value(" check_pid"):
            session.check_pid = False
        return session

    @classmethod
    @override
    def initialized(cls) -> bool:
        """Has an instance been created and initialized?"""
        return getattr(cls._instance, "_initialized", False)

    @override
    def initialize(self, argv: None | list | NoValue = NoValue) -> None:  # pyright: ignore[reportInvalidTypeForm]
        """
        Initialize the interface.

        This may be called more than once, however configuration is only done on the first call.
        """
        assert self._instance is self
        initialize = not self._initialized
        self._initialized = True

        # Environment variables
        if not os.environ.get("MPLBACKEND"):
            os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"
        if not os.environ.get("UV_PROJECT_ENVIRONMENT"):
            os.environ["UV_PROJECT_ENVIRONMENT"] = sys.prefix
        if "pytest" not in sys.modules and "IPYTHON_SUPPRESS_CONFIG_ERRORS" not in os.environ:
            os.environ["IPYTHON_SUPPRESS_CONFIG_ERRORS"] = "1"  # pragma: no cover

        if initialize:
            super().initialize([] if argv is NoValue else argv)

    @override
    def start(self) -> None:
        """
        Start the kernel blocking until the kernel stops.

        Warning:
            - Running the kernel in a thread other than the 'MainThread' is permitted, but discouraged.
            - Blocking calls can only be interrupted in the 'MainThread' because [*'threads cannot be destroyed, stopped, suspended, resumed, or interrupted'*](https://docs.python.org/3/library/threading.html#module-threading).
            - Some libraries may assume the call is occurring in the 'MainThread'.
            - If there is an `asyncio` or `trio` event loop already running in the 'MainThread`;
                start the kernel asynchronously instead (`async with kernel: ...`).
        """
        settings = RunSettings(
            backend=self.backend,
            backend_options=self.backend_options,
            host=self.host,
            host_options=self.host_options,
        )
        async_kernel.event_loop.run(self.run, (), settings)

    @classmethod
    @override
    def launch_instance(cls, argv: list[str] | None = None, **kwargs: Any) -> None:
        if BaseKernelInterface._instance:
            msg = "An instance has already been created!"
            raise RuntimeError(msg)
        app = cls.instance(**kwargs)
        try:
            app.initialize(argv)
            app.start()
        finally:
            if inst := BaseKernelInterface._instance:
                inst.clear_instance()
            app.exit()

    @override
    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Kernel]:
        self.initialize()
        self.backend = Backend(current_async_library())
        start = Event()
        try:
            self._start_hb_iopub_shell_control_threads(start)
            with self._bind_socket(Channel.stdin):
                assert len(self._sockets) == len(Channel)
                self._write_connection_file()
                async with super().__asynccontextmanager__() as kernel:
                    start.set()
                    yield kernel
        finally:
            start.set()
            self._zmq_context.term()

    def _start_hb_iopub_shell_control_threads(self, start: Event) -> None:
        def heartbeat(ready: Event) -> None:
            # ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels
            utils.mark_thread_pydev_do_not_trace()
            with self._bind_socket(Channel.heartbeat) as socket:
                ready.set()
                try:
                    zmq.proxy(socket, socket)
                except zmq.ContextTerminated:
                    return

        def pub_proxy(ready: Event) -> None:
            utils.mark_thread_pydev_do_not_trace()

            # We use an internal proxy to collect pub messages for distribution.
            # Each thread needs to open its own socket to publish to the internal proxy.
            # Ref: https://zguide.zeromq.org/docs/chapter2/#Working-with-Messages (fig 14)

            frontend: zmq.Socket = self._zmq_context.socket(zmq.XSUB)
            frontend.bind(Caller.iopub_url)

            # Capture broadcast messages received on both frontend and backend
            capture = self._zmq_context.socket(zmq.PUB)
            capture.bind(self._iopub_url)
            threading.Thread(target=self._pub_capture).start()

            with self._bind_socket(Channel.iopub) as iopub_socket:
                ready.set()
                try:
                    zmq.proxy(frontend, iopub_socket, capture)
                except (zmq.ContextTerminated, Exception):
                    pass
            frontend.close(linger=50)
            capture.close(linger=50)

        hb_ready, iopub_ready = (Event(), Event())
        threading.Thread(target=heartbeat, name="heartbeat", args=[hb_ready]).start()
        hb_ready.wait()
        threading.Thread(target=pub_proxy, name="iopub proxy", args=[iopub_ready]).start()
        iopub_ready.wait()
        # message loops
        for channel in [Channel.shell, Channel.control]:
            ready = Event()
            name = f"{channel}-receive_msg_loop"
            threading.Thread(target=self.receive_msg_loop, name=name, args=(channel, ready, start)).start()
            ready.wait()

    def _pub_capture(self) -> None:
        """
        Capture connection messages on iopub.

        Will send an 'iopub_welcome' whenever a socket subscribes to the iopub socket [ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#welcome-message).
        """

        utils.mark_thread_pydev_do_not_trace()

        socket: zmq.Socket = self._zmq_context.socket(zmq.SUB)
        socket.linger = 0
        socket.connect(self._iopub_url)
        # welcome_message:  https://jupyter.org/enhancement-proposals/65-jupyter-xpub/jupyter-xpub.html#replace-pub-socket-with-xpub-socket
        # Only subscribe to the 'pub subscribe' topic byte `1` (byte `0` is 'pub unsubscribe').
        socket.subscribe(b"\x01")
        with socket:
            while True:
                try:
                    if frames := socket.recv_multipart():
                        frame = next(iter(frames))
                        if frame[0] == 1:
                            msg = self.msg("iopub_welcome", content={"subscription": frame[1:].decode()})
                            self.iopub_send(msg, parent=None)
                except zmq.ContextTerminated:
                    break
                except Exception:
                    continue

    @contextlib.contextmanager
    def _bind_socket(self, channel: Channel) -> Generator[Any | Socket[Any], Any, None]:
        """
        Bind a zmq.Socket storing a reference to the socket and the port
        details and closing the socket on leaving the context.
        """
        match channel:
            case Channel.shell | Channel.control | Channel.heartbeat | Channel.stdin:
                socket_type = zmq.ROUTER
            case Channel.iopub:
                socket_type = zmq.XPUB
        socket: zmq.Socket = self._zmq_context.socket(socket_type)
        socket.linger = 50
        name = f"{channel}_port"
        port = bind_socket(socket=socket, transport=self.transport, ip=self.ip, port=getattr(self, name))  # pyright: ignore[reportArgumentType]
        self.set_trait(name, port)
        self.log.debug("%s socket on port: %i", channel, port)
        self._sockets[channel] = socket
        try:
            yield socket
        finally:
            socket.close(linger=50)
            self._sockets.pop(channel)

    @override
    def write_connection_file(self, **kwargs: Any) -> None:
        raise NotImplementedError

    def _write_connection_file(
        self,
    ) -> None:
        """
        Write connection info to JSON dict in kernel.connection_file.
        """
        if not (path := self.connection_file).exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            write_connection_file(
                str(path),
                transport=self.transport,
                ip=str(self.ip),
                key=self.session.key,
                signature_scheme=self.session.signature_scheme,
                kernel_name=self.name,
                **{f"{channel}_port": getattr(self, f"{channel}_port") for channel in Channel},
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

    @override
    def input_request(self, prompt: str, *, password=False) -> Any:
        job = utils.get_job()
        if not job["msg"].get("content", {}).get("allow_stdin", False):
            msg = "Stdin is not allowed in this context!"
            raise StdinNotImplementedError(msg)
        socket = self._sockets[Channel.stdin]
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
            if self.last_interrupt_frame:  # pragma: no cover
                raise KernelInterrupt
        return self.session.recv(socket)[1]["content"]["value"]  # pyright: ignore[reportOptionalSubscript]

    @override
    def iopub_send(
        self,
        msg_or_type: Message[dict[str, Any]] | dict[str, Any] | str,
        *,
        content: Content | None = None,
        metadata: dict[str, Any] | None = None,
        parent: dict[str, Any] | MsgHeader | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        ident: bytes | list[bytes] | None = None,
        buffers: list[bytes] | None = None,
    ) -> None:
        """
        Send a message on the zmq iopub socket.
        """
        if socket := Caller.iopub_sockets.get(t_ident := Caller.id_current()):
            msg = self.session.send(
                stream=socket,
                msg_or_type=msg_or_type,  # pyright: ignore[reportArgumentType]
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else utils.get_parent(),  # pyright: ignore[reportArgumentType]
                ident=ident,
                buffers=buffers,  # pyright: ignore[reportArgumentType]
            )
            if msg:
                self.log.debug("iopub_send: msg_type:'%s', content: %s", msg["header"]["msg_type"], msg["content"])
        elif (caller := self.callers.get(Channel.control)) and caller.id != t_ident:
            caller.call_direct(
                self.iopub_send,
                msg_or_type=msg_or_type,
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else None,
                ident=ident,
                buffers=buffers,
            )

    def receive_msg_loop(self, channel: Literal[Channel.control, Channel.shell], ready: Event, start: Event) -> None:
        """
        Opens a zmq socket for the channel, receives messages and calls the message handler.
        """
        if not utils.LAUNCHED_BY_DEBUGPY:
            utils.mark_thread_pydev_do_not_trace()

        session, log, message_handler = self.session, self.log, self.message_handler
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
                    buffers=content.pop("buffers", None),
                )
                if msg:
                    log.debug("*** send_reply %s*** %s", channel, msg)

        with self._bind_socket(channel) as socket:
            ready.set()
            start.wait()
            while True:
                try:
                    ident, msg = session.recv(socket, mode=zmq.BLOCKY, copy=False)
                    msg["channel"] = channel  # pyright: ignore[reportOptionalSubscript]
                    job = Job(received_time=time.monotonic(), msg=msg, ident=ident)  # pyright: ignore[reportArgumentType]
                    message_handler(job, send_reply)
                except zmq.ContextTerminated:
                    break
                except Exception as e:
                    log.debug("Bad message on %s: %s", channel, e)
                    continue

    @enable_signal_safety
    @override
    def _signal_handler(self, signum, frame: FrameType | None) -> None:
        "Handle interrupt signals."

        match self._interrupt_requested:
            case "FORCE":
                self._interrupt_requested = False
                raise KernelInterrupt
            case True:
                if frame and frame.f_locals is self.kernel.shell.user_ns:
                    self._interrupt_requested = False
                    raise KernelInterrupt
                self.last_interrupt_frame = frame

                def clearlast_interrupt_frame():
                    if self.last_interrupt_frame is frame:
                        self.last_interrupt_frame = None

                def re_raise():
                    if self.last_interrupt_frame is frame:
                        self._interrupt_now(force=True)

                # Race to check if the main thread should be interrupted.
                self.callers[Channel.shell].call_direct(clearlast_interrupt_frame)
                self.callers[Channel.control].call_later(1, re_raise)
            case False:
                signal.default_int_handler(signum, frame)

    def _interrupt_now(self, *, force=False) -> None:
        """
        Request an interrupt of the currently running shell thread.

        If called from the main thread, sets the interrupt request flag and sends a SIGINT signal
        to the current process. On Windows, uses `signal.raise_signal`; on other platforms, uses `os.kill`.
        If `force` is True, sets the interrupt request flag to "FORCE".

        Args:
            force: If True, requests a forced interrupt. Defaults to False.
        """
        # Restricted this to when the shell is running in the main thread.
        if self.callers[Channel.shell].id == Caller.CALLER_MAIN_THREAD_ID:
            self._interrupt_requested = "FORCE" if force else True
            if sys.platform == "win32":
                signal.raise_signal(signal.SIGINT)
                time.sleep(0)
            else:
                os.kill(os.getpid(), signal.SIGINT)

    @override
    def interrupt(self) -> None:
        """
        Perform a keyboard interrupt.
        """

        if not getattr(self.kernel.debugger, "stopped_threads", None):
            self._interrupt_now()
        super().interrupt()
