from __future__ import annotations

import sys
from typing import Literal

import pytest
import zmq

from async_kernel.interface.zmq import bind_socket

# pyright: reportPrivateUsage=false


@pytest.fixture(scope="module", params=["tcp", "ipc"] if sys.platform == "linux" else ["tcp"])
def transport(request):
    return request.param


def test_bind_socket(transport: Literal["tcp", "ipc"], tmp_path):
    ctx = zmq.Context()
    ip = tmp_path / "mypath" if transport == "ipc" else "0.0.0.0"
    try:
        socket = ctx.socket(zmq.SocketType.ROUTER)
        try:
            port = bind_socket(socket, transport, ip)  # pyright: ignore[reportArgumentType]
        finally:
            socket.close(linger=0)
        socket = ctx.socket(zmq.SocketType.ROUTER)
        try:
            assert bind_socket(socket, transport, ip, port) == port  # pyright: ignore[reportArgumentType]
            if transport == "tcp":
                with pytest.raises(RuntimeError):
                    bind_socket(socket, transport, ip, max_attempts=0)  # pyright: ignore[reportArgumentType]
                with pytest.raises(ValueError, match="Invalid transport"):
                    bind_socket(socket, "", ip, max_attempts=1)  # pyright: ignore[reportArgumentType]
        finally:
            socket.close(linger=0)
    finally:
        ctx.term()
