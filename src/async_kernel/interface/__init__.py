from __future__ import annotations

from typing import TYPE_CHECKING

from async_kernel.interface.base import BaseInterface, HasParentInterface
from async_kernel.typing import NoValue

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_kernel.interface.callable import Handlers

__all__ = [
    "BaseInterface",
    "HasParentInterface",
    "launch_zmq_kernel",
    "start_kernel_callable_interface",
    "start_kernel_zmq_interface",
]


async def start_kernel_callable_interface(
    *,
    send: Callable[[str, list | None, bool], None | str],
    stopped: Callable[[], None],
    settings: dict | None = None,
) -> Handlers:
    """
    Start the kernel with the callback based kernel interface [CallableInterface][async_kernel.interface.callable.CallableInterface].
    """
    from async_kernel.interface.callable import CallableInterface  # noqa: PLC0415

    return await CallableInterface(**settings or {}).start_async(send=send, stopped=stopped)


def start_kernel_zmq_interface(settings: dict) -> None:
    """
    Start the kernel with the zmq socket based kernel interface [ZMQInterface][async_kernel.interface.zmq.ZMQInterface][] loading settings prior to starting.


    Args:
        settings: The settings to apply.

    Notes:
        - Available in CPython.
        - settings loaded by the interface.
        - [sys.argv][] is not used.
    """
    from async_kernel.interface.zmq import ZMQInterface  # noqa: PLC0415

    ZMQInterface.launch_instance(argv=NoValue, settings=settings)


def launch_zmq_kernel(settings: dict) -> None:
    """
    Start the kernel with the zmq socket based kernel interface [ZMQInterface][async_kernel.interface.zmq.ZMQInterface] using the traitlets style configuration.

    Notes:
        - Available in CPython.
        - `settings` are NOT used.
        - `sys.argv` is used for configuration. Use `async-kernel --help-all` to see all configuration options.
        - [traitlets configuration documentation](https://traitlets.readthedocs.io/en/stable/config.html#module-traitlets.config).
    """
    from async_kernel.interface.zmq import ZMQInterface  # noqa: PLC0415

    ZMQInterface.launch_instance()
