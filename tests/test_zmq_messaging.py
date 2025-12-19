from __future__ import annotations

import sys
import threading
import time
from typing import Literal

import pytest
import zmq

from async_kernel.caller import Caller
from async_kernel.interface._zmq_interface import ZMQ_Interface, bind_socket
from async_kernel.typing import Backend, SocketID

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


@pytest.mark.parametrize("mode", ["direct", "proxy"])
async def test_iopub(mode: Literal["direct", "proxy"], anyio_backend: Backend) -> None:
    def pubio_subscribe():
        """Consume messages."""

        socket = ctx.socket(zmq.SocketType.SUB)
        socket.connect(url)
        socket.setsockopt(zmq.SocketOption.SUBSCRIBE, b"")
        try:
            i = 0
            while i < n:
                msg = socket.recv_multipart()
                if msg[0] == b"0":
                    assert int(msg[1]) == i
                    i += 1
            # Also test iopub from a thread that doesn't have a socket works via control thread.
            print("done")
            msg = socket.recv_multipart()
            assert msg[-1] == b'{"name": "stdout", "text": "done"}'
        finally:
            socket.close(linger=0)

    messaging = ZMQ_Interface()
    async with messaging:
        n = 10
        socket = messaging.sockets[SocketID.iopub]
        url = socket.get_string(zmq.SocketOption.LAST_ENDPOINT)
        assert url.endswith(str(messaging.ports[SocketID.iopub]))
        ctx = zmq.Context()
        try:
            thread = threading.Thread(target=pubio_subscribe)
            thread.start()
            time.sleep(0.05)
            if mode == "proxy":
                socket = Caller.iopub_sockets[messaging.callers[SocketID.control].ident]
            for i in range(n):
                socket.send_multipart([b"0", f"{i}".encode()])
            thread.join()
        finally:
            ctx.term()
