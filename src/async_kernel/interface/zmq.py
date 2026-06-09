"A collection of objects defining the kernel interface using zmq sockets."

from __future__ import annotations

import atexit
import contextlib
import errno
import json
import os
import pathlib
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING, Any, Generic, Literal, Self

import zmq
from aiologic import BinarySemaphore
from jupyter_client.connect import ConnectionFileMixin, write_connection_file
from jupyter_client.localinterfaces import localhost
from jupyter_client.session import Session
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import traitlets
from typing_extensions import override
from zmq import Flag, PollEvent, Socket, SocketOption, SocketType, ZMQError

from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.interface.base import BaseInterface, HasInterface
from async_kernel.kernelspec import expand_path
from async_kernel.typing import Channel, Content, Job, Message, MsgHeader, NoValue, T_shell_co

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from jupyter_client import KernelConnectionInfo


__all__ = ["ZMQInterface"]


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


class AsyncSession(HasInterface, Session):
    check_pid = traitlets.Bool(False).tag(config=True)


class ZMQInterface(BaseInterface[T_shell_co], ConnectionFileMixin, Generic[T_shell_co]):  # pyright: ignore[reportUnsafeMultipleInheritance]
    aliases = BaseInterface.aliases | {
        "ip": "ZMQInterface.ip",
        "hb": "ZMQInterface.hb_port",
        "shell": "ZMQInterface.shell_port",
        "iopub": "ZMQInterface.iopub_port",
        "stdin": "ZMQInterface.stdin_port",
        "control": "ZMQInterface.control_port",
        "transport": "ZMQInterface.transport",
    }
    ""

    connection_file = PathTrait().tag(config=True)
    """JSON file in which to store connection info."""

    session = traitlets.Instance(AsyncSession, ())
    ""

    transport: traitlets.CaselessStrEnum[str] = traitlets.CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp"
    ).tag(config=True)
    "Transport for sockets."

    _initialized = False
    _zmq_context = Fixed(zmq.Context)
    _iopub_url = "inproc://iopub-capture"
    _sockets: Fixed[Self, dict[Channel, zmq.Socket]] = Fixed(dict)

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
    def load_connection_info(self, info: KernelConnectionInfo) -> None:
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

    @property
    @override
    def summary(self) -> str:
        return f"{super().summary} connection_file={str(self.connection_file)!r}"

    @override
    @asynccontextmanager
    async def __asynccontextmanager__(self, *, set_started=True) -> AsyncGenerator[Self]:
        start = Event()
        try:
            self._start_hb_iopub_shell_control_threads(start)
            with self._bind_socket(Channel.stdin):
                assert len(self._sockets) == len(Channel)
                self._write_connection_file()
                async with super().__asynccontextmanager__(set_started=False):
                    start.set()
                    if set_started:
                        self._started()
                    yield self
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
        if self.curve_secretkey is not None and self.curve_publickey is not None:
            socket.curve_secretkey = self.curve_secretkey
            socket.curve_publickey = self.curve_publickey
            socket.curve_server = True
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
                curve_publickey=self.curve_publickey,
                curve_secretkey=self.curve_secretkey,
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
            raise RuntimeError(msg)
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
                parent=parent if parent is not NoValue else utils.get_parent_message(),  # pyright: ignore[reportArgumentType]
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

        session, log, message_handler, iopub_send = self.session, self.log, self.kernel.message_handler, self.iopub_send
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
                    message_handler(job, send_reply, iopub_send)
                except zmq.ContextTerminated:
                    break
                except Exception as e:
                    log.debug("Bad message on %s: %s", channel, e)
                    continue
