from __future__ import annotations

from io import TextIOBase
from typing import TYPE_CHECKING, Literal

from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import _SupportsRedirect  # pyright: ignore[reportPrivateUsage]
    from contextvars import ContextVar


class OutStream(TextIOBase):
    """
    A file like object that sends or redirects text as it is written.
    """

    def __init__(self, send: Callable[[str], None], context: ContextVar[_SupportsRedirect | None]) -> None:
        """
        Args:
            send: A callback to send text as it is written.
            context: A context variable to an potential alternate target for the text.
        """
        super().__init__()
        self._send = send
        self._out = ""
        self._context: ContextVar[_SupportsRedirect | None] = context

    @override
    def isatty(self) -> Literal[True]:
        return True

    @override
    def readable(self) -> Literal[False]:
        return False

    @override
    def seekable(self) -> Literal[False]:
        return False

    @override
    def writable(self) -> Literal[True]:
        return True

    @override
    def flush(self) -> None:
        if c_out := self._context.get():
            c_out.flush()

    @override
    def write(self, string: str) -> int:
        if out := self._context.get():
            out.write(string)
        else:
            self._send(string)
        return len(string)

    @override
    def writelines(self, sequence) -> None:
        self.write("".join(sequence))
        self.flush()
