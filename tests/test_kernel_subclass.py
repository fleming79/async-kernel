from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from async_kernel.interface.zmq import ZMQKernelInterface
from async_kernel.kernel import Kernel

if TYPE_CHECKING:
    from async_kernel.typing import Backend


def test_kernel_validators():
    with pytest.raises(RuntimeError, match="An interface must be created prior to creating the kernel"):
        Kernel()


async def test_kernel_subclass(anyio_backend: Backend):
    # Ensure the subclass correctly overrides the kernel.
    try:

        class MyKernel(Kernel):
            pass

        async with ZMQKernelInterface() as kernel:
            assert isinstance(kernel, MyKernel)
    finally:
        Kernel._cls = None  # pyright: ignore[reportPrivateUsage]


async def test_kernel_subclass_override(anyio_backend: Backend):
    # Ensure the subclass correctly overrides the kernel.
    try:

        class MyKernelA(Kernel):
            pass

        class MyKernelB(Kernel):
            pass

        async with ZMQKernelInterface() as kernel:
            assert isinstance(kernel, MyKernelB)
            assert MyKernelB() is kernel
            with pytest.raises(TypeError, match="An incompatible kernel is loaded"):
                MyKernelA()

        async with ZMQKernelInterface(kernel_class=MyKernelA) as kernel:
            assert isinstance(kernel, MyKernelA)

    finally:
        Kernel._cls = None  # pyright: ignore[reportPrivateUsage]
