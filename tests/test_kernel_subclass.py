from __future__ import annotations

from typing import TYPE_CHECKING

from async_kernel.interface.zmq import ZMQKernelInterface
from async_kernel.kernel import Kernel

if TYPE_CHECKING:
    from async_kernel.typing import Backend


async def test_kernel_subclass(anyio_backend: Backend):
    # Ensure the subclass correctly overrides the kernel.
    try:

        class MyKernel(Kernel):
            pass

        async with ZMQKernelInterface() as kernel:
            assert isinstance(kernel, MyKernel)
    finally:
        Kernel._cls = None  # pyright: ignore[reportPrivateUsage]
