from __future__ import annotations

from typing import TYPE_CHECKING

from async_kernel.asyncshell import AsyncInteractiveShell
from async_kernel.interface.zmq import ZMQKernelInterface
from async_kernel.kernel import Kernel

if TYPE_CHECKING:
    from async_kernel.typing import Backend


async def test_subclass(anyio_backend: Backend):

    class MyInterface(ZMQKernelInterface):
        pass

    class ShellSubclass(AsyncInteractiveShell):
        pass

    class MyKernel(Kernel):
        pass

    async with MyInterface(kernel_class=MyKernel, shell_class=ShellSubclass) as interface:
        assert isinstance(interface, MyInterface)
        assert isinstance(interface.kernel, MyKernel)
        assert isinstance(interface.shell, ShellSubclass)
        subshell = await interface.kernel.parent.create_subshell()
        assert isinstance(subshell, ShellSubclass)

    assert interface.event_stopped
