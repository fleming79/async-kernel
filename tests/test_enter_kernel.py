from __future__ import annotations

import pytest

from async_kernel.kernel import Kernel
from async_kernel.typing import Backend, KernelName, SocketID


@pytest.fixture(scope="module", params=list(KernelName))
def kernel_name(request):
    return request.param


@pytest.fixture(scope="module")
def anyio_backend(kernel_name: KernelName):
    return "trio" if kernel_name is KernelName.trio else "asyncio"


async def test_start_kernel_in_context(anyio_backend: Backend, kernel_name: KernelName):
    async with Kernel({"print_kernel_messages": False}) as kernel:
        assert kernel.kernel_name == kernel_name
        connection_file = kernel.connection_file
        # Test prohibit nested async context.
        with pytest.raises(RuntimeError, match="this Kernel has already been entered"):
            async with kernel:
                pass
        with pytest.raises(RuntimeError):
            Kernel({"invalid": None})
        # Test prevents binding socket more than once.
        with (
            pytest.raises(RuntimeError, match="is already loaded"),
            kernel._bind_socket(SocketID.shell),  # pyright: ignore[reportPrivateUsage]
        ):
            pass
    async with Kernel({"connection_file": connection_file, "print_kernel_messages": False}):
        # Test we can re-enter the kernel.
        pass
