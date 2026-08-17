from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import async_kernel.utils
from async_kernel import Kernel
from async_kernel.typing import Channel, MsgType

if TYPE_CHECKING:
    from async_kernel.messaging.zmq import ZMQClient


@pytest.mark.parametrize("command", ["debugInfo", "inspectVariables", "modules", "dumpCell", "source"])
async def test_debug_static(client: ZMQClient, command: str, mocker, kernel: Kernel):
    # These are tests on the debugger that don't required the debugger to be connected.
    mocker.patch.object(async_kernel.utils, "LAUNCHED_BY_DEBUGPY", new=True)

    code = "my_variable=123"
    if command == "debugInfo":
        assert async_kernel.utils.LAUNCHED_BY_DEBUGPY
    content = {"type": "request", "seq": 1, "command": command, "arguments": {"code": code}}
    reply = await client.send_message(client.msg(MsgType.debug_request, content, Channel.control))
    assert reply["content"]["status"] == "ok"
    if command == "dumpCell":
        path = reply["content"]["body"]["sourcePath"]
        content = {"type": "request", "seq": 1, "command": "source", "arguments": {"source": {"path": path}}}
        reply = await client.send_message(client.msg(MsgType.debug_request, content, Channel.control))
        assert reply["content"]["status"] == "ok"
        assert reply["content"]["body"] == {"content": code}


async def test_debug_raises_no_socket(kernel: Kernel):
    with pytest.raises(RuntimeError):
        await kernel.debugger.debugpy_client.send_request({})


async def test_debug_not_connected(client: ZMQClient, kernel: Kernel, mocker):
    mocker.patch.object(kernel.log, "exception")
    content = {"type": "request", "seq": 1, "command": "disconnect", "arguments": {}}
    reply = await client.send_message(
        client.msg(MsgType.debug_request, content, Channel.control),
    )
    assert reply["content"]["status"] == MsgType.iopub_error
    assert reply["content"]["evalue"] == "Debugpy client not connected."


@pytest.mark.parametrize("variable_name", ["my_variable", "invalid variable name", "special variables"])
async def test_debug_static_richInspectVariables(client: ZMQClient, variable_name: str):
    # These are tests on the debugger that don't required the debugger to be connected.
    content = {
        "type": "request",
        "seq": 1,
        "command": "richInspectVariables",
        "arguments": {"code": "my_variable=123", "variableName": variable_name},
    }
    reply = await client.send_message(client.msg(MsgType.debug_request, content, Channel.control))
    assert reply["content"]["status"] == "ok"
