from __future__ import annotations

import builtins
import getpass
import logging
import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from logging import Logger, LoggerAdapter
from typing import TYPE_CHECKING, Any, Literal, Self
from uuid import uuid4

import anyio
import traitlets
from traitlets import HasTraits, Instance

import async_kernel
from async_kernel.caller import Caller
from async_kernel.common import Fixed
from async_kernel.iostream import OutStream
from async_kernel.typing import Content, Message, MsgHeader, NoValue, SocketID

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from async_kernel.kernel import Kernel


__all__ = ["InterfaceBase"]


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


class InterfaceBase(HasTraits, anyio.AsyncContextManagerMixin):
    "A base class to interface with the kernel. Must be overloaded to be useful."

    log = Instance(logging.LoggerAdapter)
    "The logging adapter."

    callers: Fixed[Self, dict[Literal[SocketID.shell, SocketID.control], Caller]] = Fixed(dict)
    "The caller associated with the kernel once it has started."
    ""
    kernel: Fixed[Self, Kernel] = Fixed(lambda _: async_kernel.Kernel())

    interrupts: Fixed[Self, set[Callable[[], object]]] = Fixed(set)
    "A set for callables can be added to run code when a kernel interrupt is initiated (control thread)."

    _interrupt_requested: bool | Literal["FORCE"] = False
    last_interrupt_frame = None

    message_cnt = traitlets.Int(0)

    def load_connection_info(self, info: dict[str, Any]) -> None:
        raise NotImplementedError

    @traitlets.default("log")
    def _default_log(self) -> LoggerAdapter[Logger]:
        return logging.LoggerAdapter(logging.getLogger(self.__class__.__name__))

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        """Create caller, and open socketes."""
        restore_io = None
        caller = Caller("manual", name="Shell", protected=True, log=self.kernel.log)
        self.callers[SocketID.shell] = caller
        self.callers[SocketID.control] = caller.get(name="Control", log=self.kernel.log, protected=True)
        async with caller:
            try:
                restore_io = self._patch_io()
                yield self
            finally:
                if restore_io:
                    restore_io()

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

    def input_request(self, prompt: str, *, password=False) -> Any:
        raise NotImplementedError

    def raw_input(self, prompt="") -> Any:
        """
        Forward raw_input to frontends.

        Raises:
           IPython.core.error.StdinNotImplementedError: if active frontend doesn't support stdin.
        """
        return self.input_request(str(prompt), password=False)

    def getpass(self, prompt="") -> Any:
        """Forward getpass to frontends."""
        return self.input_request(prompt, password=True)

    def interrupt(self):
        raise NotImplementedError

    def msg(
        self,
        msg_type: str,
        *,
        content: dict | None = None,
        parent: Message | dict[str, Any] | None = None,
        header: MsgHeader | dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Message[dict[str, Any]]:
        """Return the nested message dict.

        This format is different from what is sent over the wire. The
        serialize/deserialize methods converts this nested message dict to the wire
        format, which is a list of message parts.
        """
        parent = parent or async_kernel.utils.get_parent()
        if header is None:
            session = ""
            if parent and (header := parent.get("header")):
                session = header.get("session", "")
            header = MsgHeader(
                date=datetime.now(UTC),
                msg_id=str(uuid4()),
                msg_type=msg_type,
                session=session,
                username="",
                version=async_kernel.kernel_protocol_version,
            )
        return Message(
            header=header,  # pyright: ignore[reportArgumentType]
            parent_header=extract_header(parent),  # pyright: ignore[reportArgumentType]
            content={} if content is None else content,
            metadata=metadata,
        )

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
        """Send an iopub message."""
        raise NotImplementedError
