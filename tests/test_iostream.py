from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING

import pytest

from async_kernel.interface import BaseInterface
from async_kernel.kernel import OutStream
from async_kernel.shell.base import BaseShell

if TYPE_CHECKING:
    from async_kernel.typing import Backend


async def test_io_api(anyio_backend: Backend, mocker):
    """Test that wrapped stdout has the same API as a normal TextIO object."""
    mock_stdout = mocker.patch.object(sys, "stdout")
    async with BaseInterface(shell_class=BaseShell) as interface:
        iopub_send = mocker.patch.object(interface, "iopub_send")
        stream = sys.stdout
        assert isinstance(stream, OutStream)
        assert stream._origin is mock_stdout  # pyright: ignore[reportPrivateUsage]

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
        assert iopub_send.call_args[1] == {
            "msg_or_type": "stream",
            "content": {"name": "stdout", "text": "ab"},
            "ident": b"stream.stdout",
        }
        assert stream.writable() is True
        assert stream.isatty() is True
        assert stream.readable() is False
        assert stream.seekable() is False
        assert not mock_stdout.write.called

        interface.quiet = False
        stream.write("test")
        assert mock_stdout.write.called
        assert mock_stdout.write.call_args[0][0] == "test"
