"""Base class to manage the interaction with a running kernel."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

# Updates 2026 MIT license

from __future__ import annotations

import weakref
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Generic, Literal, Self

import jupyter_client
import jupyter_client.session
import traitlets
from typing_extensions import override

from async_kernel import utils
from async_kernel.common import Fixed, SingleAsyncQueue
from async_kernel.interface.base import BaseMessageApplication, PendingMessage
from async_kernel.typing import Channel, Content, ExecuteContent, Job, Message, MsgType, NoValue, T_interface_co

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from types import CoroutineType


class ClientSession(jupyter_client.session.Session):
    check_pid = traitlets.Bool(False).tag(config=True)


class BaseKernelClient(BaseMessageApplication, Generic[T_interface_co]):
    """Communicates with a single kernel on any host via zmq channels."""

    _input_handlers: Fixed[Self, dict[str, Callable[[Content], CoroutineType[Any, Any, str]]]] = Fixed(dict)

    default_input_hander: Callable[[Content], CoroutineType[Any, Any, str]] | None = traitlets.Callable(  # pyright: ignore[reportAssignmentType]
        None, allow_none=True
    ).tag(config=True)

    interface_class: traitlets.Type[type[T_interface_co], type[T_interface_co] | str] = traitlets.Type(
        "async_kernel.interface.base.BaseInterface"
    ).tag(  # pyright: ignore[reportAssignmentType]
        config=True
    )

    def set_interface(self, interface: T_interface_co) -> None:  # pyright: ignore[reportGeneralTypeIssues]
        assert isinstance(interface, self.interface_class)
        assert not interface.stopping.done()
        assert not self.started.done()
        self._interface = weakref.ref(interface)

    def _interface(self) -> T_interface_co | None:
        return None

    @property
    def interface(self) -> T_interface_co | None:
        return self._interface()

    @override
    def _handle_request(self, job: Job) -> None:
        # Thread: undefined
        handler = getattr(self, job["msg"]["header"]["msg_type"])
        self.callers[Channel.control].to_thread(self._wrap_request_handler, handler, job)

    async def _wrap_request_handler(self, func: Callable[[Job], CoroutineType[Any, Any, Content]], job: Job) -> None:
        """Handle messages from the kernel (interface), currently only `input_request` is implemented."""
        reply_msg_type = MsgType(job["msg"]["header"]["msg_type"].replace("request", "reply"))
        try:
            content = await func(job)
            assert content["status"] in ["error", "ok"]
        except Exception as e:
            content = utils.error_to_content(e)
        buffers = content.pop("buffer", None)
        self.send_message_no_reply(
            self.msg(reply_msg_type, content=content, channel=job["msg"]["channel"], parent=job["msg"]),
            buffers=buffers,
            ident=job["msg"]["header"]["session"].encode(),
        )

    async def input_request(self, job: Job[Content]) -> Content:
        """Handle an input_request raised by the connected kernel."""
        if (parent := job["msg"]["parent_header"]) and (handler := self._input_handlers.get(parent["msg_id"])):
            result = await handler(job["msg"]["content"])
            return Content(status="ok", value=result)
        msg_ = "A handler is not available"
        raise RuntimeError(msg_)

    @asynccontextmanager
    async def iopub_subscribe(self, topic=b"") -> AsyncGenerator[SingleAsyncQueue[Message]]:
        """Subscribe to a iopub messages given a particular topic.

        Default is to subscribe to all iopub messages.

        Usaage:
        ```python
        async with client.iopub_subscribe() as queue:
            async for msg in queue:
                pass
        ```
        """
        raise NotImplementedError
        yield  # pyright: ignore[reportUnreachable]

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
        if self.interface:
            msg = "Local shutdown is prohibited. Use `interface.stop` instead."
            raise RuntimeError(msg)
        return self.send_message(
            self.msg(MsgType.shutdown_request, content={"restart": restart}, channel=Channel.control)
        )
