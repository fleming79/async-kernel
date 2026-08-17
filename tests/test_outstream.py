from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING

import pytest

from async_kernel.interface import Interface
from async_kernel.messaging import LocalClient
from async_kernel.outstream import OutStream
from async_kernel.shell.base import BaseShell
from async_kernel.typing import MsgType
from tests import utils

if TYPE_CHECKING:
    from async_kernel.typing import Backend


async def test_io_api(anyio_backend: Backend, mocker):
    """Test that wrapped stdout has the same API as a normal TextIO object."""
    mock_stdout = mocker.patch.object(sys, "stdout")
    async with (
        Interface(shell_class=BaseShell).start() as interface,
        LocalClient().start() as client,
        client.iopub_subscribe(b"stream.stdout") as queue,
    ):
        reader = aiter(queue)
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
        msg = await utils.read_until_msg_type(reader, MsgType.iopub_stream)
        assert msg["content"] == {"name": "stdout", "text": "ab"}
        assert stream.writable() is True
        assert stream.isatty() is True
        assert stream.readable() is False
        assert stream.seekable() is False
        assert not mock_stdout.write.called

        interface.quiet = False
        stream.write("test")
        msg = await utils.read_until_msg_type(reader, MsgType.iopub_stream)
        assert msg["content"] == {"name": "stdout", "text": "test"}
        assert mock_stdout.write.called
        assert mock_stdout.write.call_args[0][0] == "test"
