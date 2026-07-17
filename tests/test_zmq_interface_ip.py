from __future__ import annotations

import json
from typing import TYPE_CHECKING

import anyio
import pytest
import zmq
from aiologic import Latch
from aiologic.lowlevel import create_async_waiter

from async_kernel import Caller
from async_kernel.event_loop.zmq_poll import Poll
from async_kernel.interface import BaseInterface
from async_kernel.interface.ip_app import IPApp
from tests import utils

if TYPE_CHECKING:
    from async_kernel.caller import Caller
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


async def test_iopub_stress_test_iopub_send(anyio_backend: Backend):
    "Stress test interface.iopub_send and the associated socket."

    async with IPApp() as interface:
        caller: Caller = interface.kernel.caller
        with Poll() as poll:
            ip, port, transport = interface.ip, interface.iopub_port, interface.transport
            addr = f"tcp://{ip}:{port}" if transport == "tcp" else f"ipc://{ip}-{port}"
            sock = poll.socket(zmq.SocketType.SUB)

            sock.connect(addr)
            sock.subscribe(b"")
            for n in range(2, 20, 4):
                barrier = Latch(n)
                done = create_async_waiter()
                total = 0
                target = sum(range(n))

                def accumulate_iopub(sock: zmq.sugar.Socket, event: int, target=target, done=done):
                    nonlocal total
                    msg = sock.recv_multipart()
                    data = json.loads(msg[-1])
                    if data.get("name") == "stdout":
                        total = total + int(data["text"].strip())
                    if total == target:
                        done.wake()

                async def f(i, barrier=barrier):
                    await barrier
                    print(i)

                with anyio.fail_after(utils.TIMEOUT), poll.event_handler(sock, accumulate_iopub):
                    for i in range(n):
                        caller.to_thread(f, i)
                    await done
