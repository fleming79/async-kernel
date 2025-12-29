from __future__ import annotations

import sys

import orjson
import pytest

from async_kernel import Kernel
from async_kernel.interface._pyodide_interface import PyodideInterface


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def interface(anyio_backend, mocker):
    mocker.patch.object(sys, "platform", new="emscripten")
    kernel = Kernel()
    assert isinstance(kernel.interface, PyodideInterface)

    def sender(msg_string):
        assert isinstance(msg_string, str)

    await kernel.interface.start(sender, {})
    try:
        yield kernel.interface
    finally:
        kernel.stop()


class TestPyodideInterface:
    async def test_start(self, interface: PyodideInterface):
        assert interface.kernel.event_started

    async def test_msg(self, interface: PyodideInterface, mocker):
        sender = mocker.patch.object(interface, "_sender")
        msg = interface.msg("execute_request", content={"code": "dir()"})
        msg["header"]["session"] = "test session"
        async with interface.kernel.caller.create_pending_group():
            interface._handle_msg(orjson.dumps(msg).decode())  # pyright: ignore[reportPrivateUsage]

        assert sender.call_count == 5
        orjson.loads(sender.call_args[0][0])
