from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from logging import Logger, LoggerAdapter
from typing import TYPE_CHECKING, Any, Literal, Self

import anyio
import traitlets
from traitlets import HasTraits, Instance

import async_kernel
from async_kernel.common import Fixed
from async_kernel.typing import Content, Message, NoValue, SocketID

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from async_kernel.caller import Caller
    from async_kernel.kernel import Kernel


__all__ = ["InterfaceBase"]


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

    def load_connection_info(self, info: dict[str, Any]) -> None:
        """
        Load connection info from a dict containing connection info.

        Typically this data comes from a connection file
        and is called by load_connection_file.

        Args:
            info: Dictionary containing connection_info. See the connection_file spec for details.
        """
        raise NotImplementedError

    @traitlets.default("log")
    def _default_log(self) -> LoggerAdapter[Logger]:
        return logging.LoggerAdapter(logging.getLogger(self.__class__.__name__))

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        """Create caller, and open socketes."""
        raise NotImplementedError
        yield self  # pyright: ignore[reportUnreachable]

    def write_connection_file(self) -> None:
        """Write connection info to JSON dict in kernel.connection_file."""
        raise NotImplementedError

    def input_request(self, prompt: str, *, password=False) -> Any:
        raise NotImplementedError

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
        content: dict | None = None,
        parent: Message | dict[str, Any] | None = None,
        header: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Message[dict[str, Any]]:
        "Create a new message."
        raise NotImplementedError
