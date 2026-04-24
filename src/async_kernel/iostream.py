from __future__ import annotations

from io import TextIOBase
from typing import TYPE_CHECKING, Literal

from aiologic import Lock
from typing_extensions import override

import async_kernel
from async_kernel.common import Fixed

if TYPE_CHECKING:
    from collections.abc import Callable


class OutStream(TextIOBase):
    """A file like object that calls the flusher with the string output when flush is called."""

    _write_lock = Fixed(Lock)

    def __init__(self, flusher: Callable[[str], None], *, mode: str) -> None:
        """
        Args:
            flusher: A callback responsible for sending the output.
            ctx: The context variable to redirect output.

        [reference for IOBase](https://docs.python.org/3/library/io.html#io.IOBase)
        """
        super().__init__()
        self._flusher = flusher
        self._out = ""
        self._ctx = {"stdout": async_kernel.utils._stdout_context, "stderr": async_kernel.utils._stdout_context}[mode]  # pyright: ignore[reportPrivateUsage]

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
        if out := self._out:
            self._out = ""
            self._flusher(out)

    @override
    def write(self, string: str) -> int:
        if out := self._ctx.get():
            out.write(string)
            out.flush()
        else:
            with self._write_lock:
                self._out = string
                self.flush()
        return len(string)

    @override
    def writelines(self, sequence) -> None:
        self.write("".join(sequence))
