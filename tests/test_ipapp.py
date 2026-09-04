from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from async_kernel.interface import Interface
from async_kernel.interface.ip_app import IPApp
from async_kernel.messaging import LocalClient
from tests import utils

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
        Interface._instance = None


async def test_user_ns(anyio_backend: Backend):
    async with IPApp().start() as interface:
        assert interface.user_ns is interface.shell.user_ns
        with pytest.raises(AttributeError):
            interface.user_ns = {}  # pyright: ignore[reportAttributeAccessIssue]


async def test_start_direct(anyio_backend: Backend):
    app = IPApp().start()
    async with LocalClient().start() as client:
        await client.execute("1+1")
    app.stop()


async def test_force_shutdown(anyio_backend: Backend) -> None:
    interface = IPApp()
    interface.force_shutdown_delay = 0
    async with interface.start(), LocalClient().start() as client:
        pen = client.shutdown()
        await pen.wait(timeout=utils.TIMEOUT, protect=True)
