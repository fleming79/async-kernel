from __future__ import annotations

import io

import pytest

from async_kernel import utils as as_utils
from async_kernel.iostream import OutStream


def test_io_api():
    """Test that wrapped stdout has the same API as a normal TextIO object."""
    output = ""

    def flusher(string: str):
        nonlocal output
        output += string

    stream = OutStream(flusher, context=as_utils._stdout_context)  # pyright: ignore[reportPrivateUsage]

    assert stream.errors is None
    with pytest.raises(io.UnsupportedOperation):
        stream.detach()
    with pytest.raises(io.UnsupportedOperation):
        next(stream)
    with pytest.raises(io.UnsupportedOperation):
        stream.read()
    with pytest.raises(io.UnsupportedOperation):
        stream.readline()
    with pytest.raises(io.UnsupportedOperation):
        stream.seek(0)
    with pytest.raises(io.UnsupportedOperation):
        stream.tell()
    with pytest.raises(TypeError):
        stream.write(b" ")  # pyright: ignore[reportArgumentType]
    stream.writelines(("a", "b"))
    assert output == "ab"
    assert stream.writable() is True
    assert stream.isatty() is True
    assert stream.readable() is False
    assert stream.seekable() is False
