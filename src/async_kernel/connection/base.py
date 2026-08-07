"""The base class definition to interface with the kernel."""

from __future__ import annotations

import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, Never, Self, final
from uuid import uuid4

import anyio
from traitlets import traitlets
from traitlets.config import LoggingConfigurable
from typing_extensions import override

import async_kernel
from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed, SingleAsyncQueue
from async_kernel.interface import HasInterface
from async_kernel.pending import Pending, PendingMessage, ProtectedPending
from async_kernel.typing import (
    BuffersType,
    Channel,
    Content,
    ExecuteContent,
    Job,
    Message,
    MessageProtocol,
    MsgHeader,
    MsgType,
    MsgTypeNoReply,
    NoValue,
    T,
    T_interface_co,
)

if TYPE_CHECKING:
    import logging
    from collections.abc import AsyncGenerator, Callable
    from types import CoroutineType


__all__ = ["BaseKernelClient", "BaseMessage", "Connection", "LocalClient", "PendingMessage"]


def extract_header(msg_or_header: dict[str, Any]) -> MsgHeader | dict:
    """Given a message or header, return the header."""
    if not msg_or_header:
        return {}
    try:
        # See if msg_or_header is the entire message.
        h = msg_or_header["header"]
    except KeyError:
        try:
            # See if msg_or_header is just the header
            h = msg_or_header["msg_id"]
        except KeyError:  # noqa: TRY203
            raise
        else:
            h = msg_or_header
    return h


class BaseMessage(LoggingConfigurable, anyio.AsyncContextManagerMixin, MessageProtocol):
    """The base for messaging between kernel interfaces and clients."""

    callers: Fixed[Self, dict[Literal[Channel.shell, Channel.control], Caller]] = Fixed[
        Self, dict[Literal[Channel.shell, Channel.control], Caller]
    ](dict)
    """The callers used by the messaging application."""

    started: Fixed[Self, ProtectedPending] = Fixed(ProtectedPending)
    ""
    stopped: Fixed[Self, ProtectedPending] = Fixed(ProtectedPending)
    ""

    session_id = Fixed(lambda _: str(uuid4()))
    "Used to identify this object as the `session` in a message header."

    bsession: Fixed[Self, bytes] = Fixed[Self, bytes](lambda c: c["owner"].session_id.encode())
    "Used to identfiy this object as the origin of a message."

    log: logging.Logger

    _pending_messages: Fixed[Self, dict[str, PendingMessage[Any]]] = Fixed(dict)

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        """A context manager to open the channels in a caller named 'Control'."""
        async with Caller() as caller, caller.get(name="Control") as caller_ctrl:
            self.callers[Channel.shell] = caller
            self.callers[Channel.control] = caller_ctrl
            # Open channels
            pen_channels: Pending[None] = caller_ctrl.call_soon(self.open_channels)
            await self.started
            try:
                yield self
            finally:
                self.stopped.set_result(None)
                await pen_channels.wait(shield=True)
                del pen_channels

    async def open_channels(self) -> None:
        """Open the channels, set ready when ready block until stopped."""
        self.started.set_result(None)
        await self.stopped

    @override
    def handle_incoming_msg(self, msg: Message, ident: list[bytes]) -> None:
        raise NotImplementedError

    @override
    def handle_reply(self, msg: Message) -> None:
        # Thread: undefined
        if (parent := msg.get("parent_header")) and (f := self._pending_messages.pop(parent["msg_id"], None)):
            self.log.debug("Received %s %s", msg["header"]["msg_type"], msg)
            f.set_result(msg)

    @property
    def as_owner(self) -> Callable[[], Self]:
        """Provides a callable with reference to self."""
        return lambda: self

    @override
    def msg(
        self,
        msg_type: str | MsgType,
        content: T | None = None,
        *,
        channel: Channel,
        parent: Message | dict[str, Any] | None = None,
        header: MsgHeader | dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        buffers: BuffersType = None,
    ) -> Message[T]:
        parent = parent or utils.get_parent_message()
        if header is None:
            header = MsgHeader(
                date=datetime.now(UTC),
                msg_id=str(uuid4()),
                msg_type=msg_type,
                session=self.session_id,
                username="",
                version=async_kernel.kernel_protocol_version,
            )
        return Message(
            channel=channel,
            header=header,  # pyright: ignore[reportArgumentType]
            parent_header=extract_header(parent),  # pyright: ignore[reportArgumentType]
            content={} if content is None else content,
            metadata=metadata if metadata is not None else {},
            buffers=buffers,
        )

    @final
    def _base_send_msg(self, msg: Message, ident: bytes | list[bytes] | None = None) -> Message:
        self.transmit_msg(msg, [] if ident is None else ident if isinstance(ident, list) else [ident])
        return msg

    @override
    @final
    def send_message(
        self,
        msg: Message,
        ident: bytes | list[bytes] | None = None,
    ) -> PendingMessage[Content]:
        """Sends the message to the other side (client for kernel and vice versa) and returns a PendingMessage."""
        if MsgType(msg["header"]["msg_type"]) in MsgTypeNoReply:
            msg_ = f"{msg['header']['msg_type']} does not send a reply! Use `send_message_no_reply` instead."
            raise TypeError(msg_)
        self.log.debug("Send mssage %s %s", msg["header"]["msg_type"], msg)
        self._pending_messages[msg["header"]["msg_id"]] = pen = PendingMessage()
        pen.metadata.update(parent=self._base_send_msg(msg, ident))
        return pen

    @override
    @final
    def send_message_no_reply(self, msg: Message, ident: bytes | list[bytes] | None = None) -> None:
        self._base_send_msg(msg, ident)

    @override
    def send_reply(self, job: Job, content: dict, /, *, buffers: BuffersType = None) -> None:
        if "status" not in content:
            content["status"] = "ok"
        msg = self.msg(
            job["msg"]["header"]["msg_type"].replace("request", "reply"),
            content=content,
            parent=job["msg"],
            channel=job["msg"]["channel"],
        )
        self.send_message_no_reply(msg, job["ident"])
        if msg:
            self.log.debug("send_reply %s", msg)

    @override
    def transmit_msg(self, msg: Message, ident: list[bytes]) -> None:
        raise NotImplementedError


class Connection(HasInterface[T_interface_co], BaseMessage, Generic[T_interface_co]):
    """Provides a connection to the interface for messaging."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        ctx = super().__asynccontextmanager__()

        def start() -> ProtectedPending[Any]:
            # Nesting inside __init__ means this is only called once for normal usage.
            async def run(stopped=self.stopped) -> None:
                async with ctx:
                    await stopped

            del self.start
            try:
                interface = self.parent
                assert not interface.stopping.done()

                interface.started.add_done_callback(lambda _: interface.callers[Channel.shell].call_soon(run))
                interface.stopped.add_done_callback(self.stop)
            except Exception as e:
                self.started.set_exception(e)
                self.stop()
            return self.started

        self.start = start

    @override
    def __asynccontextmanager__(self) -> Never:
        msg = f"Directly using async context of {self} is not allowed!"
        raise RuntimeError(msg)

    @override
    async def open_channels(self) -> None:
        """Open the channels, set ready when ready block until stopped."""
        self.parent.refresh_connections(self)
        self.started.set_result(None)
        await self.stopped
        self.parent.refresh_connections()

    def start(self) -> ProtectedPending[Any]:
        """Required to start the connection."""
        return self.started

    @override
    def handle_incoming_msg(self, msg: Message, ident: list[bytes]) -> None:

        match msg["channel"]:
            case Channel.control | Channel.shell:
                self.parent.kernel.handle_request(
                    Job(msg=msg, ident=ident, received_time=time.monotonic(), owner=self.as_owner)
                )
            case Channel.stdin:
                self.handle_reply(msg)
            case _:
                self.log.debug("Unhandled message %r %r", msg, ident)

    def stop(self, _=None, /) -> ProtectedPending[Any]:
        """Stop the connection."""
        self.stopped.set_result(None)
        self.parent.stopped.remove_done_callback(self.stop)
        return self.stopped

    def connection_info(self) -> str:
        return ""


class BaseKernelClient(BaseMessage, Generic[T_interface_co]):
    """Communicates with a single connection."""

    _input_handlers: Fixed[Self, dict[str, Callable[[Content], CoroutineType[Any, Any, str]]]] = Fixed(dict)

    default_input_hander: Callable[[Content], CoroutineType[Any, Any, str]] | None = traitlets.Callable(  # pyright: ignore[reportAssignmentType]
        None, allow_none=True
    ).tag(config=True)

    _iopub_queues: Fixed[Self, deque[tuple[bytes, SingleAsyncQueue]]] = Fixed(deque)

    @override
    def handle_incoming_msg(self, msg: Message, ident: list[bytes]) -> None:
        match msg["channel"]:
            case Channel.control | Channel.shell:
                self.handle_reply(msg)
            case Channel.stdin:
                self._handle_request(Job(owner=self.as_owner, msg=msg, ident=ident, received_time=time.monotonic()))
            case Channel.iopub:
                for topic, queue in self._iopub_queues:
                    if not topic or topic in ident:
                        queue.append(msg)
            case _:
                self.log.debug("Unhandled message")

    def _handle_request(self, job: Job) -> None:
        # Thread: undefined
        handler = getattr(self, job["msg"]["header"]["msg_type"])
        self.callers[Channel.control].to_thread(self._wrap_request_handler, handler, job)

    async def _wrap_request_handler(self, func: Callable[[Job], CoroutineType[Any, Any, Content]], job: Job) -> None:
        """Handle messages from the kernel (interface), currently only `input_request` is implemented."""
        reply_msg_type: MsgType = MsgType(job["msg"]["header"]["msg_type"].replace("request", "reply"))
        try:
            content = await func(job)
            assert content["status"] in ["error", "ok"]
        except Exception as e:
            content = utils.error_to_content(e)
        self.send_message_no_reply(
            self.msg(reply_msg_type, content=content, channel=job["msg"]["channel"], parent=job["msg"]),
            job["msg"]["header"]["session"].encode(),
        )

    async def input_request(self, job: Job[Content]) -> Content:
        """Handle an `input_request` raised by the connected kernel."""
        if (parent := job["msg"]["parent_header"]) and (handler := self._input_handlers.get(parent["msg_id"])):
            result = await handler(job["msg"]["content"])
            return Content(status="ok", value=result)
        msg_ = "A handler is not available!"
        raise RuntimeError(msg_)

    @asynccontextmanager
    async def iopub_subscribe(self, topic=b"") -> AsyncGenerator[SingleAsyncQueue[Message]]:
        """Open a new iopub socket and subscribe to a particular topic.

        Usaage:
        ```python
        async with client.iopub_subscribe() as queue:
            async for msg in queue:
                pass
        ```
        """
        queue = SingleAsyncQueue()
        self._iopub_queues.append((topic, queue))
        try:
            yield queue
        finally:
            self._iopub_queues.remove((topic, queue))
            queue.stop()

    # Methods to send specific messages on channels (only relevant to execute). All other message types are decided by the kernel.
    def execute(
        self,
        code: str,
        silent: bool = False,
        *,
        store_history: bool = True,
        user_expressions: dict[str, str] | None = None,
        stop_on_error: NoValue | bool = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        metadata: dict[str, Any] | None = None,
        input_handler: Callable[[Content], CoroutineType[Any, Any, str]] | NoValue | None = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        channel: Literal[Channel.shell, Channel.control] = Channel.shell,
        subshell_id: str | None = None,
    ) -> PendingMessage:
        """Execute code in the kernel.

        Params:
            code: A string of code in the kernel's language.
            silent: If set, the kernel will execute the code as quietly possible, and
                will force store_history to be False.
            store_history: If set, the kernel will store command history.  This is forced
                to be False if silent is True.
            user_expressions: A dict mapping names to expressions to be evaluated in the user's
                dict. The expression values are returned as strings formatted using [repr][].
            input_handler:  A handler for the stdin requests associated with the execute request.
                When not provided, stdin is disabled.
            stop_on_error: Flag whether to abort the execution queue, if an exception is encountered.
        """
        input_handler = self.default_input_hander if input_handler is NoValue else input_handler
        content: ExecuteContent = {
            "code": code,
            "silent": silent,
            "store_history": store_history,
            "user_expressions": user_expressions or {},
            "allow_stdin": bool(input_handler),
            "stop_on_error": (not silent) if stop_on_error is NoValue else stop_on_error,
            "subshell_id": subshell_id,
        }
        msg = self.msg(MsgType.execute_request, content=content, metadata=metadata, channel=channel)
        if input_handler:
            self._input_handlers[msg["header"]["msg_id"]] = input_handler
        pen = self.send_message(msg)
        pen.add_done_callback(lambda _: self._input_handlers.pop(pen.msg_id))
        return pen

    def complete(self, code: str, cursor_pos: int | None = None) -> PendingMessage[Content]:
        """Tab complete text in the kernel's namespace.

        Args:
            code: The context in which completion is requested.
                Can be anything between a variable name and an entire cell.
            cursor_pos: The position of the cursor in the block of code where the completion was requested.
                Default: `len(code)`.
        """
        if cursor_pos is None:
            cursor_pos = len(code)
        content = {"code": code, "cursor_pos": cursor_pos}
        msg = self.msg(MsgType.complete_request, channel=Channel.shell, content=content)
        return self.send_message(msg)

    def inspect(self, code: str, cursor_pos: int | None = None, detail_level: int = 0) -> PendingMessage[Content]:
        """Get metadata information about an object in the kernel's namespace.

        It is up to the kernel to determine the appropriate object to inspect.

        Params:
            code: Context in which info is requested.
                Can be anything between a variable name and an entire cell.
            cursor_pos: The position of the cursor in the block of code where the info was requested.
            detail_level:  The level of detail for the introspection (0-2).
        """
        if cursor_pos is None:
            cursor_pos = len(code)
        content = {"code": code, "cursor_pos": cursor_pos, "detail_level": detail_level}
        return self.send_message(self.msg(MsgType.inspect_request, channel=Channel.shell, content=content))

    def history(
        self,
        raw: bool = True,
        output: bool = False,
        hist_access_type: Literal["tail", "range", "search"] = "range",
        **kwargs: Any,
    ) -> PendingMessage[Content]:
        """Get entries from the kernel's history list.

        Args:
        raw: If True, return the raw input.
        output: If True, then return the output as well.
        hist_access_type: 'range' (fill in session, start and stop params), 'tail' (fill in n)
             or 'search' (fill in pattern param).
        **kwargs:
            session: For a range request, the session from which to get lines. Session numbers
                are positive integers; negative ones count back from the current session.
            start: The first line number of a history range.
            stop: The final (excluded) line number of a history range.
            n: The number of lines of history to get for a tail request.
            pattern: The glob-syntax pattern for a search request.

        Returns: The ID of the message sent.
        """
        if hist_access_type == "range":
            kwargs.setdefault("session", 0)
            kwargs.setdefault("start", 0)
        content = dict(raw=raw, output=output, hist_access_type=hist_access_type, **kwargs)
        return self.send_message(self.msg(MsgType.history_request, channel=Channel.shell, content=content))

    def kernel_info(self) -> PendingMessage[Content]:
        """Request kernel info."""
        return self.send_message(self.msg(MsgType.kernel_info_request, channel=Channel.shell))

    def comm_info(self, target_name: str | None = None) -> PendingMessage[Content]:
        """Request comm info."""
        content = {} if target_name is None else {"target_name": target_name}
        return self.send_message(self.msg(MsgType.comm_info_request, channel=Channel.shell, content=content))

    def is_complete(self, code: str) -> PendingMessage[Content]:
        """Ask the kernel whether some code is complete and ready to execute."""
        return self.send_message(self.msg(MsgType.is_complete_request, channel=Channel.shell, content={"code": code}))

    def shutdown(self, restart: bool = False) -> PendingMessage[Content]:
        """Request an immediate kernel shutdown.

        Upon receipt of the (empty) reply, client code can safely assume that
        the kernel has shut down and it's safe to forcefully terminate it if
        it's still alive.
        """
        return self.send_message(
            self.msg(MsgType.shutdown_request, content={"restart": restart}, channel=Channel.control)
        )


class LocalClient(HasInterface[T_interface_co], BaseKernelClient[T_interface_co], Generic[T_interface_co]):
    """A connection for an interface running in the current process."""

    connection: Fixed[Self, Connection[T_interface_co]] = Fixed(Connection)
    session_id: Fixed[Self, str] = Fixed(lambda c: c["owner"].connection.session_id)

    @override
    async def open_channels(self) -> None:
        # Cross-connect
        self.connection.transmit_msg = self.handle_incoming_msg
        self.transmit_msg = self.connection.handle_incoming_msg

        await self.connection.start()
        self.connection.stopped.add_done_callback(lambda _: self.stopped.set_result(None))
        await super().open_channels()
        self.connection.stop()
