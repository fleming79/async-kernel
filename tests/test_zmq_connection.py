from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

import pytest
import zmq
from aiologic.lowlevel import create_async_waiter

from async_kernel import Pending
from async_kernel.event_loop.zmq_poll import ZMQPoll, ZMQPollSocket
from async_kernel.interface import Interface
from async_kernel.typing import Channel, MsgType
from tests import utils

if TYPE_CHECKING:
    from async_kernel import Kernel
    from async_kernel.messaging.zmq import ZMQClient
    from async_kernel.shell import IPShell


@pytest.fixture(params=["zmq"], scope="module")
def connection_name(request):
    return request.param


async def test_execute_request_success(client: ZMQClient):
    reply = await client.execute("1 + 1")
    assert reply["header"]["msg_type"] == MsgType.execute_reply
    assert reply["content"]["status"] == "ok"


async def test_simple_print(kernel: Kernel, client: ZMQClient):
    """Simple print statement in kernel."""
    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute("print('🌈')")
        await anext(reader)
        await anext(reader)
        msg = await anext(reader)
        assert msg["content"]["text"] == "🌈\n"
        assert msg["header"]["msg_type"] == MsgType.iopub_stream


async def test_print_non_caller_thread(kernel: Kernel, client: ZMQClient):

    async with client.iopub_subscribe() as queue:
        t = threading.Thread(target=print, args=["-non_caller_thread-"])
        t.start()
        async for msg in queue:
            assert msg["content"]["text"] == "-non_caller_thread-\n"
            break
        t.join()


async def test_interrupt_request_not_blocked(client: ZMQClient, kernel: Kernel):
    pen: Any = Pending()
    kernel.active_execute_requests.add(pen)
    reply = await client.send_message(client.msg(MsgType.interrupt_request, None, Channel.control))
    assert reply["header"]["msg_type"] == MsgType.interrupt_reply
    assert reply["content"] == {"status": "ok"}
    assert pen.cancelled()


@pytest.mark.parametrize(
    "code",
    argvalues=[
        "%connect_info",
        "%callers",
        "%subshell",
        "%pip -V",
        "%uv -V",
        "%mkdir test\n%rmdir test\n%ls",
    ],
)
async def test_magic(client: ZMQClient, code: str, kernel: Kernel, monkeypatch):

    assert code
    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute(code)
        await utils.read_until_msg_type(reader, msg_type=MsgType.iopub_execute_input)
        msg = await utils.read_until_msg_type(reader, msg_type=MsgType.iopub_stream)
    text = msg["content"]["text"]
    assert text
    match code:
        case "%uv -V":
            assert "uv" in text
        case _:
            pass


@pytest.mark.parametrize(
    "code",
    argvalues=[
        "%thread\nprint('okay')",
        """%%thread name="Trio executor" backend=trio\nfrom async_kernel import Caller; assert Caller().name == "Trio executor";print('okay')""",
        "import asyncio\n%asyncio await asyncio.sleep(0)\nprint('okay')",
        "import trio\n%trio await trio.sleep(0)\nprint('okay')",
    ],
)
async def test_magic_async(client: ZMQClient, code: str, kernel: Kernel, monkeypatch):

    assert code
    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute(code)
        await utils.read_until_msg_type(reader, msg_type=MsgType.iopub_execute_input)
        msg = await utils.read_until_msg_type(reader, msg_type=MsgType.iopub_stream)
    text = msg["content"]["text"]
    assert text
    match code:
        case "%uv -V":
            assert "uv" in text
        case _:
            pass


async def test_magic_error(client: ZMQClient) -> None:

    reply = await client.execute("%%thread backend=trio\npass")
    assert reply["content"]["status"] == "error"
    assert "'name' must be specified when providing settings!" in reply["content"]["evalue"]
    reply = await client.execute("%%thread name=test not_an_option=True\npass")
    assert reply["content"]["status"] == "error"
    assert "One or more invalid options found" in reply["content"]["evalue"]


@pytest.mark.parametrize("code", argvalues=["%connect_info"])
async def test_magic_sync(client: ZMQClient, code: str, kernel: Kernel[Interface, IPShell], monkeypatch):
    result = kernel.main_shell.run_cell(code)
    assert result.success


async def test_shell_enable_gui(kernel: Kernel[Interface, IPShell]):
    # used by ipython AutoMagicChecker via is_shadowed (requires 'builitin')
    assert set(kernel.shell.ns_table) == {"user_global", "user_local", "builtin"}
    # U
    kernel.shell.enable_gui()
    with pytest.raises(RuntimeError):
        kernel.shell.enable_gui("not a gui")


async def test_launch_too_late(kernel: Kernel):
    with pytest.raises(RuntimeError, match="An interface already exists!"):
        Interface.launch_instance()


async def test_already_entered(kernel: Kernel):
    with pytest.raises(RuntimeError, match="has already been entered"):
        async with kernel.parent:
            pass


@pytest.mark.parametrize("topic", ["zmq", "kernel"])
async def test_iopub_welcome(topic: str, client: ZMQClient, connection_name: str):
    """Test iopub welcome message. https://jupyter-client.readthedocs.io/en/stable/messaging.html#welcome-message."""

    with ZMQPoll() as zmq_poll:
        ip, port, transport = client.ip, client.iopub_port, client.transport
        addr = f"tcp://{ip}:{port}" if transport == "tcp" else f"ipc://{ip}-{port}"
        sock = zmq_poll.socket(zmq.SocketType.SUB)
        msg, ident = None, None

        sock.connect(addr)
        sock.subscribe(topic)

        def read_iopub(sock: ZMQPollSocket, event: int) -> None:
            nonlocal ident, msg
            ident, msg = client.session.recv(sock)

        done = create_async_waiter()
        with zmq_poll.event_handler(sock, read_iopub, count=(1, done.wake), canceller=None):
            await done

        assert ident == [topic.encode()]
        assert msg
        assert msg["msg_type"] == "iopub_welcome"
        assert msg["content"]["subscription"] == topic
