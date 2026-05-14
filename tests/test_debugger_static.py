from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING, Literal

import anyio
import pytest
from IPython.core import page

import async_kernel.utils
from async_kernel import Kernel, Pending
from async_kernel.asyncshell import AsyncInteractiveShell, AsyncInteractiveSubshell, SubshellManager
from async_kernel.caller import Caller
from async_kernel.comm import Comm
from async_kernel.debugger import Debugger
from async_kernel.typing import Channel, MsgType, RunMode, Tags
from tests import utils

if TYPE_CHECKING:
    from jupyter_client.asynchronous.client import AsyncKernelClient


@pytest.mark.parametrize("command", ["debugInfo", "inspectVariables", "modules", "dumpCell", "source"])
async def test_debug_static(client: AsyncKernelClient, command: str, mocker, kernel:Kernel):
    # These are tests on the debugger that don't required the debugger to be connected.
    kernel._traits.pop('debugger')
    mocker.patch.object(async_kernel.utils, "LAUNCHED_BY_DEBUGPY", new=True)
    assert 
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
    debugger = kernel.debugger
    assert isinstance(debugger, Debugger)
    with pytest.raises(RuntimeError):
        await debugger.debugpy_client.send_request({})


async def test_debug_not_connected(client: AsyncKernelClient):
    reply = await utils.send_control_message(
        client, MsgType.debug_request, {"type": "request", "seq": 1, "command": "disconnect", "arguments": {}}
    )
    assert reply["content"]["status"] == "error"
    assert reply["content"]["evalue"] == "Debugpy client not connected."


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

