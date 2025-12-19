from __future__ import annotations

import atexit
import builtins
import contextlib
import errno
import getpass
import os
import pathlib
import signal
import sys
import threading
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self

import traitlets
import zmq
from aiologic import BinarySemaphore, Event
from aiologic.lowlevel import enable_signal_safety
from IPython.core.error import StdinNotImplementedError
from jupyter_client import write_connection_file
from jupyter_client.localinterfaces import localhost
from jupyter_client.session import Session
from traitlets import CaselessStrEnum, Unicode
from typing_extensions import override
from zmq import Flag, PollEvent, Socket, SocketOption, SocketType, ZMQError

import async_kernel
from async_kernel import utils
from async_kernel.asyncshell import KernelInterruptError
from async_kernel.caller import Caller
from async_kernel.common import Fixed
from async_kernel.interface.interface import InterfaceBase
from async_kernel.iostream import OutStream
from async_kernel.typing import Content, Job, Message, MsgType, NoValue, SocketID

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator
    from types import FrameType

__all__ = ["ZMQ_Interface"]


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


class ZMQ_Interface(InterfaceBase):
    "An interface for the kernel that uses zmq sockets."

    _zmq_context = Fixed(zmq.Context)
    sockets: Fixed[Self, dict[SocketID, zmq.Socket]] = Fixed(dict)
    ports: Fixed[Self, dict[SocketID, int]] = Fixed(dict)

    ip = Unicode()
    """
    The kernel's IP address [default localhost].
    
    If the IP address is something other than localhost, then Consoles on other machines 
    will be able to connect to the Kernel, so be careful!
    """
    session = Fixed(Session)
    "Handles serialization and sending of messages."

    transport: CaselessStrEnum[str] = CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp"
    )
    "Transport for sockets."

    @override
    def load_connection_info(self, info: dict[str, Any]) -> None:
        """
        Load connection info from a dict containing connection info.

        Typically this data comes from a connection file
        and is called by load_connection_file.

        Args:
            info: Dictionary containing connection_info. See the connection_file spec for details.
        """
        if self.ports:
            msg = "Connection info is already loaded!"
            raise RuntimeError(msg)
        self.transport = info.get("transport", self.transport)
        self.ip = info.get("ip") or self.ip
        for socket in SocketID:
            name = f"{socket}_port"
            if socket not in self.ports and name in info:
                self.ports[socket] = info[name]
        if "key" in info:
            key = info["key"]
            if isinstance(key, str):
                key = key.encode()
            assert isinstance(key, bytes)

            self.session.key = key
        if "signature_scheme" in info:
            self.session.signature_scheme = info["signature_scheme"]

    @traitlets.validate("ip")
    def _validate_ip(self, proposal) -> str:
        return "0.0.0.0" if (val := proposal["value"]) == "*" else val

    @traitlets.default("ip")
    def _default_ip(self) -> str:
        return str(self.kernel.connection_file) + "-ipc" if self.transport == "ipc" else localhost()

    @override
    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        """Create caller, and open socketes."""
        sig = restore_io = None
        caller = Caller("manual", name="Shell", protected=True, log=self.kernel.log, zmq_context=self._zmq_context)
        self.callers[SocketID.shell] = caller
        self.callers[SocketID.control] = caller.get(name="Control", log=self.kernel.log, protected=True)
        start = Event()
        try:
            async with caller:
                self._start_hb_iopub_shell_control_threads(start)
                with self._bind_socket(SocketID.stdin):
                    assert len(self.sockets) == len(SocketID)
                    self.write_connection_file()
                    restore_io = self._patch_io()
                    with contextlib.suppress(ValueError):
                        sig = signal.signal(signal.SIGINT, self._signal_handler)
                    start.set()
                    yield self
        finally:
            start.set()
            if sig:
                signal.signal(signal.SIGINT, sig)
            if restore_io:
                restore_io()
            self._zmq_context.term()

    def _start_hb_iopub_shell_control_threads(self, start: Event) -> None:
        def heartbeat(ready: Event) -> None:
            # ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels
            async_kernel.utils.mark_thread_pydev_do_not_trace()
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
        match socket_id:
            case SocketID.shell | SocketID.control | SocketID.heartbeat | SocketID.stdin:
                socket_type = zmq.ROUTER
            case SocketID.iopub:
                socket_type = zmq.XPUB
        socket: zmq.Socket = self._zmq_context.socket(socket_type)
        socket.linger = 50
        port = bind_socket(socket=socket, transport=self.transport, ip=self.ip, port=self.ports.get(socket_id, 0))  # pyright: ignore[reportArgumentType]
        self.ports[socket_id] = port
        self.log.debug("%s socket on port: %i", socket_id, port)
        self.sockets[socket_id] = socket
        try:
            yield socket
        finally:
            socket.close(linger=50)
            self.sockets.pop(socket_id)

    def _patch_io(self) -> Callable[[], None]:
        original_io = sys.stdout, sys.stderr, sys.displayhook, builtins.input, self.getpass

        def restore():
            sys.stdout, sys.stderr, sys.displayhook, builtins.input, getpass.getpass = original_io

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
                if not self.kernel.quiet and (echo := (sys.__stdout__ if name == "stdout" else sys.__stderr__)):
                    echo.write(string)
                    echo.flush()

            wrapper = OutStream(flusher=flusher)
            setattr(sys, name, wrapper)

        return restore

    @override
    def write_connection_file(
        self,
    ) -> None:
        """Write connection info to JSON dict in kernel.connection_file."""
        if not (path := self.kernel.connection_file).exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            write_connection_file(
                str(path),
                transport=self.transport,
                ip=self.ip,
                key=self.session.key,
                signature_scheme=self.session.signature_scheme,
                kernel_name=self.kernel.kernel_name,
                **{f"{socket_id}_port": self.ports[socket_id] for socket_id in SocketID},
            )
            ip_files: list[pathlib.Path] = []
            if self.transport == "ipc":
                for s in self.sockets.values():
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
        job = async_kernel.utils.get_job()
        if not job["msg"].get("content", {}).get("allow_stdin", False):
            msg = "Stdin is not allowed in this context!"
            raise StdinNotImplementedError(msg)
        socket = self.sockets[SocketID.stdin]
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
            if self.last_interrupt_frame:
                raise KernelInterruptError
        return self.session.recv(socket)[1]["content"]["value"]  # pyright: ignore[reportOptionalSubscript]

    @override
    def iopub_send(
        self,
        msg_or_type: Message[dict[str, Any]] | dict[str, Any] | str,
        content: Content | None = None,
        metadata: dict[str, Any] | None = None,
        parent: dict[str, Any] | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        ident: bytes | list[bytes] | None = None,
        buffers: list[bytes] | None = None,
    ) -> None:
        """Send a message on the zmq iopub socket."""
        if socket := Caller.iopub_sockets.get(t_ident := Caller.current_ident()):
            msg = self.session.send(
                stream=socket,
                msg_or_type=msg_or_type,  # pyright: ignore[reportArgumentType]
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else utils.get_parent(),  # pyright: ignore[reportArgumentType]
                ident=ident,
                buffers=buffers,
            )
            if msg:
                self.log.debug("iopub_send: msg_type:'%s', content: %s", msg["msg_type"], msg["content"])
        elif (caller := self.callers.get(SocketID.control)) and caller.ident != t_ident:
            caller.call_direct(
                self.iopub_send,
                msg_or_type=msg_or_type,
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else None,
                ident=ident,
                buffers=buffers,
            )

    def receive_msg_loop(
        self, socket_id: Literal[SocketID.control, SocketID.shell], ready: Event, start: Event
    ) -> None:
        "Opens a zmq socket for socket_id, receives messages and calls the message handler."

        if not utils.LAUNCHED_BY_DEBUGPY:
            utils.mark_thread_pydev_do_not_trace()

        session, log, message_handler = self.session, self.log, self.kernel.msg_handler
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

    @enable_signal_safety
    def _signal_handler(self, signum, frame: FrameType | None) -> None:
        "Handle interrupt signals."

        match self._interrupt_requested:
            case "FORCE":
                self._interrupt_requested = False
                raise KernelInterruptError
            case True:
                if frame and frame.f_locals is self.kernel.shell.user_ns:
                    self._interrupt_requested = False
                    raise KernelInterruptError
                self.last_interrupt_frame = frame

                def clearlast_interrupt_frame():
                    if self.last_interrupt_frame is frame:
                        self.last_interrupt_frame = None

                def re_raise():
                    if self.last_interrupt_frame is frame:
                        self._interrupt_now(force=True)

                # Race to check if the main thread should be interrupted.
                self.callers[SocketID.shell].call_direct(clearlast_interrupt_frame)
                self.callers[SocketID.control].call_later(1, re_raise)
            case False:
                signal.default_int_handler(signum, frame)

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
        if self.callers[SocketID.shell].ident == Caller.MAIN_THREAD_IDENT:
            self._interrupt_requested = "FORCE" if force else True
            if sys.platform == "win32":
                signal.raise_signal(signal.SIGINT)
                time.sleep(0)
            else:
                os.kill(os.getpid(), signal.SIGINT)

    @override
    def interrupt(self):
        self._interrupt_now()
        while self.interrupts:
            try:
                self.interrupts.pop()()
            except Exception:
                pass

    @override
    def msg(
        self,
        msg_type: str,
        content: dict | None = None,
        parent: Message | dict[str, Any] | None = None,
        header: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Message[dict[str, Any]]:
        return self.session.msg(msg_type, content, parent, header, metadata)  # pyright: ignore[reportReturnType, reportArgumentType]
