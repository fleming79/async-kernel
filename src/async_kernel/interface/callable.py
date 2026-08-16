from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, TypedDict

from aiologic import BinarySemaphore

from async_kernel.common import import_item
from async_kernel.compat.json import pack_json_str, unpack_json
from async_kernel.connection.base import Connection
from async_kernel.interface import Interface
from async_kernel.kernelspec import make_argv
from async_kernel.typing import Channel, T

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_kernel.typing import BuffersType, Message


__all__ = ["start_kernel_callable_interface"]


class Handlers(TypedDict, Generic[T]):
    """Handlers returned by [async_kernel.interface.callable.CallableInterface][] when it is started."""

    handle_msg: Callable[[T, BuffersType], None]
    """
    Handle messages from the client.
    
    The handler requires two positional arguments
        
    1. The message serialized as a JSON string. The channel ("shell" or "control" ) 
        should also be included in the Message under the key "channel". 
    2. A list of buffers if there are any, or None if there are no buffers.
    """

    stop: Callable[[], Any]
    "Stop the kernel."


async def start_kernel_callable_interface(
    *,
    send: Callable[[T, BuffersType, bool], Any],
    stopped: Callable[[], Any],
    settings: dict | None = None,
    pack_unpack: tuple[Callable[[Message], T], Callable[[T], Message]] = (pack_json_str, unpack_json),
) -> Handlers[T]:
    """Start the interface using functions for passing serialised messages.

    Args:
        send: A function for the interface to send the packed message.
        stopped: A callback that is called when the interface has stopped.
        settings: Additional settings to configure the interface/kernel/shell etc using traitlets config conventions.
            The settings are converted to argv using [async_kernel.kernelspec.make_argv][]. All settings,
            including aliases and flags are accepted. _flags_ should be passed as `'flags': [<flag1>, <flag2>, ...]`.
        pack_unpack: A pair of methods to serialize and unserialize messages.

    Returns: The connection instance.
    """
    settings = settings or {}
    interface_class = settings.get("interface_class") or "async_kernel.interface.Interface"
    cls: type[Interface] = import_item(interface_class)
    # A patch to avoid duplicate cell output when using LiteKernelClient which already sends iopub messages to all clients.

    argv = make_argv(command=(), connection_file="", **settings)[1:]
    app = cls(argv)
    assert issubclass(cls, Interface)
    app.start()
    await app.started
    handle_msg = create_interface_messge_callback_handler(app, send, pack_unpack)
    app.stopped.add_done_callback(lambda _: stopped())
    return Handlers(stop=app.stop, handle_msg=handle_msg)


def create_interface_messge_callback_handler(
    interface: Interface,
    send: Callable[[T, BuffersType, bool], Any],
    pack_unpack: tuple[Callable[[Message], T], Callable[[T], Message]] = (pack_json_str, unpack_json),
) -> Callable[[T, BuffersType], None]:
    ""
    cache: dict[str, Connection] = {}
    lock = BinarySemaphore()
    session_calls = set()
    pack, unpack = pack_unpack

    def handle_msg(packed_msg: T, buffers: BuffersType = None) -> None:
        """Handle a packed message."""
        msg: Message = unpack(packed_msg)
        msg["buffers"] = buffers
        session: str = msg["header"]["session"]
        session_calls.add(session)
        conn: Connection | None

        if (conn := cache.get(session)) is None or conn.stopping.done():
            with lock:
                if (conn := cache.get(session)) is None or conn.stopping.done():
                    conn = Connection(interface.caller, session_id=session)

                    def transmit_msg(msg: Message, ident: list[bytes]) -> None:
                        """Pack and send a message."""
                        # `ident` is not sent.
                        buffers: BuffersType = msg.pop("buffers", None)  # pyright: ignore[reportAssignmentType]
                        reply = send(pack(msg), buffers, blocking_reply := msg["channel"] == Channel.stdin)
                        if blocking_reply:
                            conn.handle_incoming_msg(unpack(reply), [conn.bsession])

                    conn.transmit_msg = transmit_msg
                    conn.stopped.add_done_callback(lambda _: delattr(conn, "transmit_msg"))
                    conn.start()
                    conn.stopping.add_done_callback(lambda _: cache.pop(session))

                    cache[session] = conn
        conn.handle_incoming_msg(msg, [conn.bsession])

    return handle_msg
