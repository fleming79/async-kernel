from __future__ import annotations

from typing import TYPE_CHECKING

from async_kernel.comm import Comm, CommManager

if TYPE_CHECKING:
    from async_kernel.kernel import Kernel


async def test_comm(kernel: Kernel) -> None:
    assert isinstance(kernel.comm_manager, CommManager)
    c = Comm(target_name="bar")
    msgs = []

    def on_close(msg):
        msgs.append(msg)

    def on_message(msg):
        msgs.append(msg)

    c.publish_msg("foo")
    c.open({})
    c.on_msg(on_message)
    c.on_close(on_close)
    c.handle_msg({})
    c.handle_close({})
    c.close()
    assert len(msgs) == 2
    assert c.target_name == "bar"


async def test_comm_manager(kernel: Kernel, mocker) -> None:
    manager = kernel.comm_manager
    msgs = []

    def foo(comm, msg):
        msgs.append(msg)

    def fizz(comm, msg):
        msg = "hi"
        raise RuntimeError(msg)

    def on_close(msg):
        msgs.append(msg)

    def on_msg(msg):
        msgs.append(msg)

    manager.register_target("foo", foo)
    manager.register_target("fizz", fizz)

    publish_msg = mocker.patch.object(Comm, "publish_msg")

    c = Comm()
    c.on_msg(on_msg)
    c.on_close(on_close)
    manager.register_comm(c)
    assert publish_msg.call_count == 1

    assert manager.get_comm(c.comm_id) == c
    assert manager.get_comm("foo") is None

    msg = {"content": {"comm_id": c.comm_id, "target_name": "foo"}}
    manager.comm_open(None, None, msg)  # pyright: ignore[reportArgumentType]
    assert len(msgs) == 1
    msg["content"]["target_name"] = "bar"
    manager.comm_open(None, None, msg)  # pyright: ignore[reportArgumentType]
    assert len(msgs) == 1
    msg = {"content": {"comm_id": c.comm_id, "target_name": "fizz"}}
    manager.comm_open(None, None, msg)  # pyright: ignore[reportArgumentType]
    assert len(msgs) == 1

    manager.register_comm(c)
    assert manager.get_comm(c.comm_id) == c
    msg = {"content": {"comm_id": c.comm_id}}
    manager.comm_msg(None, None, msg)  # pyright: ignore[reportArgumentType]
    assert len(msgs) == 2
    msg["content"]["comm_id"] = "foo"
    manager.comm_msg(None, None, msg)  # pyright: ignore[reportArgumentType]
    assert len(msgs) == 2

    manager.register_comm(c)
    assert manager.get_comm(c.comm_id) == c
    msg = {"content": {"comm_id": c.comm_id}}

    manager.comm_close(None, None, msg)  # pyright: ignore[reportArgumentType]
    assert len(msgs) == 3

    assert c._closed  # pyright: ignore[reportPrivateUsage]

    # Leave some comm open for the kernel to close with do_shutdown
    for i in range(10):
        msg = {"content": {"comm_id": str(i), "target_name": "foo"}}
        manager.comm_open(None, None, msg)  # pyright: ignore[reportArgumentType]
    assert manager.comms
