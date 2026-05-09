from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import async_kernel
from async_kernel.kernel import Kernel

if TYPE_CHECKING:
    from async_kernel.typing import Backend


async def test_kernel_subclass(anyio_backend: Backend):
    # Ensure the subclass correctly overrides the kernel.
    try:

        class MyKernel(Kernel):
            pass

        async with Kernel().interface as kernel:
            assert Kernel._instance is kernel  # pyright: ignore[reportPrivateUsage]
            assert isinstance(kernel, MyKernel)
            assert isinstance(Kernel(), MyKernel)
            assert isinstance(async_kernel.utils.get_kernel(), MyKernel)
        assert MyKernel._instance is None  # pyright: ignore[reportPrivateUsage]
    finally:
        Kernel._cls = None  # pyright: ignore[reportPrivateUsage]


async def test_kernel_specific(anyio_backend: Backend):
    # Ensure the subclass correctly overrides the kernel.
    try:

        class K1(Kernel):
            pass

        class K2(Kernel):
            "This subclass is defined later, but we want to use K1"

        async with K1().interface as kernel:
            assert Kernel._instance is kernel  # pyright: ignore[reportPrivateUsage]
            assert isinstance(kernel, K1)
            assert isinstance(Kernel(), K1)
            assert isinstance(async_kernel.utils.get_kernel(), K1)
            with pytest.raises(TypeError):
                K2()
        assert K1._instance is None  # pyright: ignore[reportPrivateUsage]

        async with K2().interface as kernel:
            assert Kernel._instance is kernel  # pyright: ignore[reportPrivateUsage]
            assert isinstance(kernel, K2)
            assert isinstance(Kernel(), K2)
            assert isinstance(async_kernel.utils.get_kernel(), K2)
            with pytest.raises(TypeError):
                K1()

    finally:
        Kernel._cls = None  # pyright: ignore[reportPrivateUsage]
