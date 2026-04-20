from __future__ import annotations

import pytest

from async_kernel.kernel import Kernel


@pytest.mark.parametrize("anyio_backend", argvalues=["asyncio", "trio"])
async def test_start_kernel_in_context(anyio_backend):
    async with Kernel({"print_kernel_messages": False}).interface as kernel:
        assert kernel.kernel_name == {"asyncio": "async", "trio": "async-trio"}[anyio_backend]
        connection_file = kernel.connection_file
        # Test prohibit nested async context.
        with pytest.raises(RuntimeError, match="has already been entered"):
            async with kernel.interface:
                pass
        with pytest.raises(RuntimeError):
            Kernel({"invalid": None})
    async with Kernel({"connection_file": connection_file, "print_kernel_messages": False}).interface:
        # Test we can re-enter the kernel.
        pass
