from __future__ import annotations

from typing import TYPE_CHECKING

import async_kernel
from async_kernel.kernel import Kernel

if TYPE_CHECKING:
    from async_kernel.typing import Backend


async def test_kernel_subclass(anyio_backend: Backend):
    # Ensure the subclass correctly overrides the kernel.
    Kernel.stop()

    class MyKernel(Kernel):
        print_kernel_messages = False

    async with MyKernel() as kernel:
        assert Kernel._instance is kernel  # pyright: ignore[reportPrivateUsage]
        assert isinstance(kernel, MyKernel)
        assert isinstance(Kernel(), MyKernel)
        assert isinstance(async_kernel.utils.get_kernel(), MyKernel)
    assert not MyKernel._instance  # pyright: ignore[reportPrivateUsage]
