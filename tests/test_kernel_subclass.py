from __future__ import annotations

from typing import TYPE_CHECKING

import async_kernel

if TYPE_CHECKING:
    from async_kernel.kernelspec import Backend


async def test_kernel_subclass(anyio_backend: Backend):
    # Ensure the subclass correctly overrides the kernel.
    async_kernel.Kernel.stop()

    class MyKernel(async_kernel.Kernel):
        pass

    async with MyKernel() as kernel:
        assert async_kernel.Kernel._instance is kernel  # pyright: ignore[reportPrivateUsage]
        assert isinstance(kernel, MyKernel)
        assert isinstance(async_kernel.Kernel(), MyKernel)
        assert isinstance(async_kernel.utils.get_kernel(), MyKernel)
    assert not MyKernel._instance  # pyright: ignore[reportPrivateUsage]
