from __future__ import annotations

import pytest

from async_kernel.interface import Interface


@pytest.mark.parametrize("anyio_backend", argvalues=["asyncio", "trio"])
async def test_start_kernel_in_context(anyio_backend):
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
