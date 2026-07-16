from __future__ import annotations

import io
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Literal

from typing_extensions import override

import async_kernel
from async_kernel.interface import HasInterface

if TYPE_CHECKING:
    from collections.abc import Callable


class OutStream(HasInterface, io.TextIOBase):
    """
    A file like object that sends or redirects text as it is written.

    Only intended for internal use.
    """

    def __init__(self, name: Literal["stdout", "stderr"]) -> None:
        """
        Args:
            send: A callback to send text as it is written.
            context: A context variable to an potential alternate target for the text.
        """
        super().__init__()
        self.name = name
        if name == "stdout":
            self._context = async_kernel.utils._stdout_context  # pyright: ignore[reportPrivateUsage]
        else:
            self._context = async_kernel.utils._stderr_context  # pyright: ignore[reportPrivateUsage]
        self.ident = f"stream.{self.name}".encode()
        self._origin = None

    def patch(self) -> Callable[[], None]:
        self._origin = origin = getattr(sys, self.name)
        setattr(sys, self.name, self)

        def restore() -> None:
            setattr(sys, self.name, origin)

        return restore

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
        if not isinstance(string, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            msg = f"Not a string: {string!r}"  # pyright: ignore[reportUnreachable]
            raise TypeError(msg)
        if out := self._context.get():
            out.write(string)
        else:
            interface = self.parent
            interface.iopub_send(msg_or_type="stream", content={"name": self.name, "text": string}, ident=self.ident)
            if self._origin and not self.parent.quiet:
                self._origin.write(string)  # pragma: no cover
                self._origin.flush()  # pragma: no cover

        return len(string)

    @override
    def writelines(self, sequence) -> None:
        self.write("".join(sequence))
        self.flush()
