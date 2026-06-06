from __future__ import annotations

from typing import TYPE_CHECKING

from async_kernel.common import import_item
from async_kernel.interface.base import BaseInterface, HasInterface

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_kernel.interface.callable import Handlers

__all__ = ["BaseInterface", "HasInterface", "launch_interface", "start_kernel_callable_interface"]


async def start_kernel_callable_interface(
    *, send: Callable[[str, list | None, bool], None | str], stopped: Callable[[], None]
) -> Handlers:
    """
    Start the global interface as an instance of [CallableInterface][async_kernel.interface.callable.CallableInterface].
    """
    from async_kernel.interface.callable import CallableInterface  # noqa: PLC0415

    return await CallableInterface().start_async(send=send, stopped=stopped)


def launch_interface(settings: dict) -> None:
    """
    Launch a kernel interface blocking until it has stopped.

    Notes:
        - Available in CPython.
        - 'interface_class' can be specified in settings as a subclass of [BaseInterface][async_kernel.interface.base.BaseInterface]
            or as an importable string.
        - `settings` are NOT loaded.
        - `sys.argv` is used for configuration. Use `async-kernel --help-all` to see all configuration options.
        - [traitlets configuration documentation](https://traitlets.readthedocs.io/en/stable/config.html#module-traitlets.config).
    """

    val = settings.get("interface_class") or settings.get("BaseInterface.interface_class")
    val = val or "async_kernel.interface.ip_app.IPApp"
    cls = import_item(val) if isinstance(val, str) else val
    assert issubclass(cls, BaseInterface)
    cls.launch_instance()
