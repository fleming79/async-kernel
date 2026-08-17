from __future__ import annotations

from async_kernel.common import import_item
from async_kernel.interface.base import HasInterface, Interface
from async_kernel.interface.callable import start_kernel_callable_interface

__all__ = ["HasInterface", "Interface", "launch_interface", "start_kernel_callable_interface"]


def launch_interface(settings: dict) -> None:
    """Launch a kernel interface blocking until it has stopped.

    Notes:
        - Available in CPython.
        - 'interface_class' can be specified in settings as a subclass of [Interface][async_kernel.interface.base.Interface]
            or as an importable string.
        - `settings` are NOT loaded.
        - `sys.argv` is used for configuration. Use `async-kernel --help-all` to see all configuration options.
        - [traitlets configuration documentation](https://traitlets.readthedocs.io/en/stable/config.html#module-traitlets.config).
    """
    val = settings.get("interface_class") or settings.get("Interface.interface_class")
    val = val or "async_kernel.interface.ip_app.IPApp"
    cls = import_item(val) if isinstance(val, str) else val
    assert issubclass(cls, Interface)
    cls.launch_instance()
