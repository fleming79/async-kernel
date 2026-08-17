from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from async_kernel.interface import Interface
from async_kernel.messaging import LocalClient

if TYPE_CHECKING:
    from async_kernel import Caller


@pytest.mark.parametrize("anyio_backend", argvalues=["asyncio", "trio"])
async def test_start_interface_in_context(anyio_backend):
    async with Interface().start() as interface:
        assert interface.started
        assert interface.backend == anyio_backend
        # Test prohibit nested async context.
        with pytest.raises(RuntimeError, match="has already been entered"):
            async with interface:
                pass
    interface2 = Interface()
    async with interface2.start():
        # Test we start a new interface.
        assert interface2.started
        assert interface2 is not interface.kernel


@pytest.mark.parametrize("anyio_backend", argvalues=["asyncio", "trio"])
async def test_start_interface_in_non_main_thread(caller: Caller):

    interface = Interface()
    try:
        caller.to_thread(interface.start)
        async with LocalClient().start() as client:
            await client.kernel_info()
    finally:
        await interface.stop()
