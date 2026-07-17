from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import zmq
from aiologic.lowlevel import create_async_waiter

from async_kernel.event_loop.zmq_poll import Poll
from async_kernel.interface import BaseInterface
from async_kernel.interface.ip_app import IPApp

if TYPE_CHECKING:
    from async_kernel.typing import Backend

# pyright: reportPrivateUsage=false


@pytest.mark.parametrize("gui", ["tk", "qt"])
def test_gui_sets_host(gui):
    try:
        interface = IPApp(gui=gui)
        assert interface.gui == gui
        assert interface.host == gui
        interface.host = None
    finally:
        BaseInterface._instance = None


async def test_user_ns(anyio_backend: Backend):
    async with IPApp() as interface:
        assert interface.user_ns is interface.shell.user_ns
        with pytest.raises(AttributeError):
            interface.user_ns = {}  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize("topic", ["", "kernel"])
async def test_iopub_welcome(topic: str, anyio_backend: Backend):
    """Test iopub welcome message. https://jupyter-client.readthedocs.io/en/stable/messaging.html#welcome-message"""
    async with IPApp() as interface:
        with Poll() as poll:
            ip, port, transport = interface.ip, interface.iopub_port, interface.transport
            addr = f"tcp://{ip}:{port}" if transport == "tcp" else f"ipc://{ip}-{port}"
            sock = poll.socket(zmq.SocketType.SUB)
            msg, ident = None, None

            sock.connect(addr)
            sock.subscribe(topic)

            def read_iopub(sock: zmq.Socket, event: int) -> None:
                nonlocal ident, msg
                ident, msg = interface.session.recv(sock)

            done = create_async_waiter()
            with poll.event_handler(sock, read_iopub, countdown=(1, done.wake)):
                await done

            assert ident == [topic.encode()]
            assert msg
            assert msg["msg_type"] == "iopub_welcome"
            assert msg["content"]["subscription"] == topic
