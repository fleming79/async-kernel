from __future__ import annotations

from typing import TYPE_CHECKING

from async_kernel.interface.base import BaseInterface, HasInterface

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_kernel.interface.callable import Handlers

__all__ = ["BaseInterface", "HasInterface", "launch_zmq_interface", "start_kernel_callable_interface"]


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


def launch_zmq_interface(settings: dict | None = None) -> None:
    """
    Launch the global instance of the ZMQInterface [ZMQInterface][async_kernel.interface.zmq.ZMQInterface].

    Notes:
        - Available in CPython.
        - `settings` are NOT used.
        - `sys.argv` is used for configuration. Use `async-kernel --help-all` to see all configuration options.
        - [traitlets configuration documentation](https://traitlets.readthedocs.io/en/stable/config.html#module-traitlets.config).
    """
    from async_kernel.interface.zmq import ZMQInterface  # noqa: PLC0415

    ZMQInterface.launch_instance()
