from __future__ import annotations

from typing import TYPE_CHECKING

from async_kernel.interface.base import BaseKernelInterface

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_kernel.interface.callable import Handlers

__all__ = ["BaseKernelInterface", "start_kernel_callable_interface", "start_kernel_zmq_interface"]


async def start_kernel_callable_interface(
    *,
    send: Callable[[str, list | None, bool], None | str],
    stopped: Callable[[], None],
    settings: dict | None = None,
) -> Handlers:
    """
    Start the kernel with the callback based kernel interface [CallableKernelInterface][async_kernel.interface.callable.CallableKernelInterface].

    Used by:
        - [`jupyterlite-async-kernel`](https://github.com/fleming79/echo-kernel): A Jupyterlite compatible Pyodide kernel (Experimental)
    """
    from async_kernel.interface.callable import CallableKernelInterface  # noqa: PLC0415

    return await CallableKernelInterface(settings).start(send=send, stopped=stopped)


def start_kernel_zmq_interface(settings: dict | None = None) -> None:
    """
    Start the kernel with the zmq socket based kernel interface [ZMQKernelInterface][async_kernel.interface.zmq.ZMQKernelInterface].

    Available in CPython.
    """
    from async_kernel.interface.zmq import ZMQKernelInterface  # noqa: PLC0415

    ZMQKernelInterface(settings).start()
