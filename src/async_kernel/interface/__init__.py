from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from async_kernel.common import import_item
from async_kernel.compat.json import pack_json_str, unpack_json
from async_kernel.interface.base import HasInterface, Interface
from async_kernel.kernelspec import make_argv

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_kernel.typing import BuffersType, Message, T


__all__ = ["HasInterface", "Interface", "launch_interface", "start_kernel_callable_interface"]


async def start_kernel_callable_interface(
    *,
    transmit: Callable[[T, list[bytes], BuffersType], Any],
    stopped: Callable[[], Any],
    settings: dict | None = None,
    pack_unpack: tuple[Callable[[Message], T], Callable[[T], Message]] = (pack_json_str, unpack_json),
) -> Callable[[T, list[bytes], BuffersType], None]:
    """Start the interface using functions for passing serialised messages.

    Args:
        transmit: A function for the interface to call to transmit messages.
        stopped: A callback that is called when the interface has stopped.
        settings: Additional settings to configure the interface/kernel/shell etc using traitlets config conventions.
            The settings are converted to argv using [async_kernel.kernelspec.make_argv][]. All settings,
            including aliases and flags are accepted. _flags_ should be passed as `'flags': [<flag1>, <flag2>, ...]`.
        pack_unpack: A pair of methods to serialize and unserialize messages.

    Returns: The function to send serialised messages to the interface.
    """
    from async_kernel.connection.base import Connection  # noqa: PLC0415

    settings = settings or {}
    interface_class = settings.get("interface_class") or "async_kernel.interface.Interface"
    cls: type[Interface] = import_item(interface_class)

    argv = make_argv(command=(), connection_file="", **settings)[1:]
    app = cls(argv)
    assert issubclass(cls, Interface)

    def transmit_msg(msg: Message, ident: list[bytes], transmit=transmit, pack=pack_unpack[0]) -> None:
        """Send a message using `transmit`."""
        buffers: BuffersType = msg.pop("buffers", None)  # pyright: ignore[reportAssignmentType]
        transmit(pack(msg), ident, buffers)

    def receive_message(encoded_msg: T, ident: list[bytes], buffers: BuffersType, app=app, unpack=pack_unpack[1]):
        """Receive an external message."""
        msg: Message = unpack(encoded_msg)
        msg["buffers"] = buffers
        connection.handle_incoming_msg(msg, ident)

    connection = Connection()
    connection.transmit_msg = transmit_msg
    app.stopped.add_done_callback(lambda _: stopped())
    app.start()
    await app.started
    connection.start()
    return receive_message


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
