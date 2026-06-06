from __future__ import annotations

from typing import TYPE_CHECKING

from async_kernel.interface.zmq import ZMQInterface
from async_kernel.kernel import Kernel
from async_kernel.shell import IPShell

if TYPE_CHECKING:
    from async_kernel.typing import Backend


async def test_subclass(anyio_backend: Backend):

    class MyInterface(ZMQInterface[IPShell]):
        pass

    class ShellSubclass(IPShell):
        pass

    class MyKernel(Kernel[MyInterface, IPShell]):
        pass

    async with MyInterface(kernel_class=MyKernel, shell_class=ShellSubclass) as interface:
        assert isinstance(interface, MyInterface)
        assert isinstance(interface.kernel, MyKernel)
        assert isinstance(interface.kernel.shell, ShellSubclass)
        subshell = interface.kernel.create_subshell()
        assert isinstance(subshell, ShellSubclass)

    assert interface.event_stopped
