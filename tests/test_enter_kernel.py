from __future__ import annotations

import pytest

from async_kernel.interface.zmq import ZMQInterface


@pytest.mark.parametrize("anyio_backend", argvalues=["asyncio", "trio"])
async def test_start_kernel_in_context(anyio_backend):
    async with ZMQInterface() as interface:
        assert isinstance(interface, ZMQInterface)
        connection_file = interface.connection_file
        assert interface.started
        assert interface.backend == anyio_backend
        # Test prohibit nested async context.
        with pytest.raises(RuntimeError, match="has already been entered"):
            async with interface:
                pass
    interface2 = ZMQInterface()
    interface2.connection_file = connection_file
    async with interface2:
        # Test we start a new kernel.
        assert interface2.started
        assert interface2 is not interface.kernel
