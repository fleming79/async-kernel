from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from async_kernel.interface import BaseInterface
from async_kernel.interface.zmq_ip import ZMQInterfaceIP

if TYPE_CHECKING:
    from async_kernel.typing import Backend

# pyright: reportPrivateUsage=false


@pytest.mark.parametrize("gui", ["tk", "qt"])
def test_gui_sets_host(gui):
    try:
        interface = ZMQInterfaceIP(gui=gui)
        assert interface.gui == gui
        assert interface.host == gui
        interface.host = None
    finally:
        BaseInterface._instance = None


async def test_user_ns(anyio_backend: Backend):
    async with ZMQInterfaceIP() as interface:
        assert interface.user_ns is interface.shell.user_ns
        with pytest.raises(AttributeError):
            interface.user_ns = {}  # pyright: ignore[reportAttributeAccessIssue]
