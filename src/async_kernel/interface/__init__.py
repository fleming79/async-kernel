from __future__ import annotations

from typing import TYPE_CHECKING

from async_kernel.common import import_item
from async_kernel.interface.base import BaseInterface, HasInterface
from async_kernel.kernelspec import make_argv

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_kernel.interface.callable import CallableInterface, Handlers

__all__ = ["BaseInterface", "HasInterface", "launch_interface", "start_kernel_callable_interface"]


async def start_kernel_callable_interface(
    *, send: Callable[[str, list | None, bool], None | str], stopped: Callable[[], None], settings: dict | None = None
) -> Handlers:
    """
    Start the global interface as an instance of [CallableInterface][async_kernel.interface.callable.CallableInterface].

    Args:
        send: A callback responsible for sending messages from the kernel on all channels.
        stopped: A callback that is called once the kernel has stopped.
        settings: Additional settings to configure the interface/kernel/shell etc using traitlets config conventions.
            The settings are converted to argv using [async_kernel.kernelspec.make_argv][]. All settings,
            including aliases and flags are accepted. _flags_ should be passed as `'flags': [<flag1>, <flag2>, ...]`.

    Tip:
        - To list all config options available for a `CallableInterface` use the command:
            ```shell
            async-kernel --help-all --interface_class=async_kernel.interface.callable.CallableInterface
            ```
    """

    settings = settings or {}
    interface_class = settings.get("interface_class") or "async_kernel.interface.callable.CallableInterface"
    cls: type[CallableInterface] = import_item(interface_class)

    argv = make_argv(command=(), connection_file="", **settings)[1:]
    app = cls(argv)
    assert issubclass(cls, BaseInterface)
    return await app.start_async(send=send, stopped=stopped)


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
