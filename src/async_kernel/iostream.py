from __future__ import annotations

from io import TextIOBase
from threading import Lock
from typing import TYPE_CHECKING, Literal

from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable


class OutStream(TextIOBase):
    """A file like object that calls the flusher with the string output when flush is called."""

    _write_lock = Lock()

    def __init__(self, flusher: Callable[[str], None]) -> None:
        """
        Args:
            flusher: A callback responsible for sending the output.

        [reference for IOBase](https://docs.python.org/3/library/io.html#io.IOBase)
        """
        super().__init__()
        self._flusher = flusher
        self._out = ""

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
        """
        Write to current stream after encoding if necessary

        Returns: number of items from input parameter written to stream.
        """
        with self._write_lock:
            self._out = string
            self.flush()
        return len(string)

    @override
    def writelines(self, sequence) -> None:
        """Write lines to the stream (separators are not added)."""
        self.write("".join(sequence))
