from __future__ import annotations

import pytest

from async_kernel.interface.zmq import ZMQKernelInterface


@pytest.mark.parametrize("anyio_backend", argvalues=["asyncio", "trio"])
async def test_start_kernel_in_context(anyio_backend):
    async with ZMQKernelInterface() as interface:
        assert isinstance(interface, ZMQKernelInterface)
        connection_file = interface.connection_file
        assert interface.event_started
        assert interface.backend == anyio_backend
        # Test prohibit nested async context.
        with pytest.raises(RuntimeError, match="has already been entered"):
            async with interface:
                pass
    interface2 = ZMQKernelInterface()
    interface2.connection_file = connection_file
    async with interface2:
        # Test we start a new kernel.
        assert interface2.event_started
        assert interface2 is not interface.kernel
