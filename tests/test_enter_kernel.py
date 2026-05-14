from __future__ import annotations

import pytest

from async_kernel.interface.zmq import ZMQKernelInterface


@pytest.mark.parametrize("anyio_backend", argvalues=["asyncio", "trio"])
async def test_start_kernel_in_context(anyio_backend):
    async with ZMQKernelInterface() as kernel:
        assert isinstance(kernel.interface, ZMQKernelInterface)
        connection_file = kernel.interface.connection_file
        assert kernel.event_started
        assert kernel.interface.backend == anyio_backend
        # Test prohibit nested async context.
        with pytest.raises(RuntimeError, match="has already been entered"):
            async with kernel.interface:
                pass
    interface = ZMQKernelInterface()
    interface.connection_file = connection_file
    async with interface as kernel2:
        # Test we start a new kernel.
        assert kernel2.event_started
        assert kernel2 is not kernel
