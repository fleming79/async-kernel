from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

import pytest
from jupyter_client.asynchronous.client import AsyncKernelClient

from async_kernel.interface import BaseInterface
from async_kernel.interface.ip_app import IPApp
from async_kernel.interface.zmq import ZMQInterface

if TYPE_CHECKING:
    import pathlib

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


@pytest.mark.parametrize("transport", ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"])
async def test_transport(anyio_backend: Backend, transport: str, tmp_path: pathlib.Path):
    os.chdir(tmp_path)
    async with ZMQInterface(transport=transport) as interface:
        assert interface.transport == transport
        client = AsyncKernelClient()
        client.load_connection_file(interface.connection_file)
        client.start_channels()
        msg = await client.get_iopub_msg()
        assert msg["msg_type"] == "iopub_welcome"
