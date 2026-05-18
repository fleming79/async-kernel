from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import async_kernel.utils
from async_kernel import Kernel
from async_kernel.typing import MsgType
from tests import utils

if TYPE_CHECKING:
    from jupyter_client.asynchronous.client import AsyncKernelClient


@pytest.mark.parametrize("command", ["debugInfo", "inspectVariables", "modules", "dumpCell", "source"])
async def test_debug_static(client: AsyncKernelClient, command: str, mocker, kernel: Kernel):
    # These are tests on the debugger that don't required the debugger to be connected.
    mocker.patch.object(async_kernel.utils, "LAUNCHED_BY_DEBUGPY", new=True)

    code = "my_variable=123"
    if command == "debugInfo":
        assert async_kernel.utils.LAUNCHED_BY_DEBUGPY
    reply = await utils.send_control_message(
        client, MsgType.debug_request, {"type": "request", "seq": 1, "command": command, "arguments": {"code": code}}
    )
    assert reply["content"]["status"] == "ok"
    if command == "dumpCell":
        path = reply["content"]["body"]["sourcePath"]
        reply = await utils.send_control_message(
            client,
            MsgType.debug_request,
            {"type": "request", "seq": 1, "command": "source", "arguments": {"source": {"path": path}}},
        )
        assert reply["content"]["status"] == "ok"
        assert reply["content"]["body"] == {"content": code}


async def test_debug_raises_no_socket(kernel: Kernel):
    with pytest.raises(RuntimeError):
        await kernel.debugger.debugpy_client.send_request({})


async def test_debug_not_connected(client: AsyncKernelClient, kernel: Kernel, mocker):
    mock_method = mocker.patch.object(kernel.parent.log, "exception")
    reply = await utils.send_control_message(
        client, MsgType.debug_request, {"type": "request", "seq": 1, "command": "disconnect", "arguments": {}}
    )
    assert reply["content"]["status"] == "error"
    assert reply["content"]["evalue"] == "Debugpy client not connected."
    assert str(mock_method.call_args).startswith("call('Exception in message handler:'")


@pytest.mark.parametrize("variable_name", ["my_variable", "invalid variable name", "special variables"])
async def test_debug_static_richInspectVariables(client: AsyncKernelClient, variable_name: str):
    # These are tests on the debugger that don't required the debugger to be connected.
    reply = await utils.send_control_message(
        client,
        MsgType.debug_request,
        {
            "type": "request",
            "seq": 1,
            "command": "richInspectVariables",
            "arguments": {"code": "my_variable=123", "variableName": variable_name},
        },
    )
    assert reply["content"]["status"] == "ok"
