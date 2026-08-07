from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

import pytest
import zmq
from aiologic.lowlevel import create_async_waiter

from async_kernel import Caller, Pending
from async_kernel.connection.zmq import ZMQKernelClient
from async_kernel.event_loop.zmq_poll import ZMQPoll, ZMQPollSocket
from async_kernel.interface import Interface
from async_kernel.typing import Channel, MsgType
from tests import utils
from tests.test_message_spec import read_until_msg_type

if TYPE_CHECKING:
    from async_kernel import Kernel
    from async_kernel.shell import IPShell


@pytest.fixture(params=["zmq"], scope="module")
def connection_name(request):
    return request.param


async def test_execute_request_success(client: ZMQKernelClient):
    reply = await client.execute("1 + 1")
    assert reply["header"]["msg_type"] == MsgType.execute_reply
    assert reply["content"]["status"] == "ok"


async def test_simple_print(kernel: Kernel, client: ZMQKernelClient):
    """Simple print statement in kernel."""
    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute("print('🌈')")
        await anext(reader)
        await anext(reader)
        msg = await anext(reader)
        assert msg["content"]["text"] == "🌈\n"
        assert msg["header"]["msg_type"] == MsgType.iopub_stream


async def test_print_non_caller_thread(kernel: Kernel, client: ZMQKernelClient):

    async with client.iopub_subscribe() as queue:
        t = threading.Thread(target=print, args=["-non_caller_thread-"])
        t.start()
        async for msg in queue:
            assert msg["content"]["text"] == "-non_caller_thread-\n"
            break
        t.join()


# @pytest.mark.parametrize("test_mode", ["reply", "interrupt", "allow_stdin=False"])
# @pytest.mark.parametrize("mode", ["input", "password"])
# async def test_input(
#     subprocess_kernel_client: ZMQKernelClient,
#     mode: Literal["input", "password"],
#     test_mode: Literal["interrupt", "reply", "allow_stdin=False"],
# ):

#     async def input_handler(content: Content) -> str:
#         ready.wake()
#         if test_mode == "interrupt":
#             await create_async_waiter()
#         return str(content)

#     ready, client, theprompt = create_async_waiter(), subprocess_kernel_client, "Enter a value >"
#     match mode:
#         case "input":
#             code = f"response = input('{theprompt}')"
#         case "password":
#             code = f"import getpass;response = getpass.getpass('{theprompt}')"

#     if test_mode == "allow_stdin=False":
#         reply = await client.execute(code)
#         assert reply["content"]["status"] == "error"
#         assert reply["content"].get("ename") == "RuntimeError"
#         return

#     await anyio.sleep(0.1)
#     pen = client.execute(code, input_handler=input_handler, user_expressions={"response": "response"})
#     await ready

#     if test_mode == "interrupt":
#         await client.send_message(client.msg(msg_type=MsgType.interrupt_request, channel=Channel.control))
#         reply = await pen
#         assert reply["content"]["status"] == "error"
#         assert reply["content"]["traceback"][0] == "async_kernel.common.KernelInterrupt\n"
#         # Check the interface is still working
#         assert (await client.execute("1+1"))["content"]["status"] == "ok"
#     else:
#         reply = await pen
#         assert reply["content"]["status"] == "ok"
#         val = reply["content"]["user_expressions"]["response"]["data"]["text/plain"]
#         val_ = eval(eval(val))
#         assert val_ == {"prompt": theprompt, "password": mode == "password"}


async def test_interrupt_request_not_blocked(client: ZMQKernelClient, kernel: Kernel):
    pen: Any = Pending()
    kernel.active_execute_requests.add(pen)
    reply = await client.send_message(client.msg(MsgType.interrupt_request, channel=Channel.control))
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
        # "%thread\nprint('okay')",
        # """%%thread name="Trio executor" backend=trio\nfrom async_kernel import Caller; assert Caller().name == "Trio executor";print('okay')""",
        # "import asyncio\n%asyncio await asyncio.sleep(0)\nprint('okay')",
        # "import trio\n%trio await trio.sleep(0)\nprint('okay')",
        "%mkdir test\n%rmdir test\n%ls",
    ],
)
async def test_magic(client: ZMQKernelClient, code: str, kernel: Kernel, monkeypatch):

    assert code
    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute(code)
        await read_until_msg_type(reader, msg_type=MsgType.iopub_execute_input)
        msg = await read_until_msg_type(reader, msg_type=MsgType.iopub_stream)
    text = msg["content"]["text"]
    assert text
    match code:
        case "%uv -V":
            assert "uv" in text
        case _:
            pass


async def test_magic_error(client: ZMQKernelClient) -> None:

    reply = await client.execute("%%thread backend=trio\npass")
    assert reply["content"]["status"] == "error"
    assert "'name' must be specified when providing settings!" in reply["content"]["evalue"]
    reply = await client.execute("%%thread name=test not_an_option=True\npass")
    assert reply["content"]["status"] == "error"
    assert "One or more invalid options found" in reply["content"]["evalue"]


@pytest.mark.parametrize("code", argvalues=["%connect_info"])
async def test_magic_sync(client: ZMQKernelClient, code: str, kernel: Kernel[Interface, IPShell], monkeypatch):
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


async def test_subprocess_kernel_monitor_heartbeat(anyio_backend, mocker):
    # This is the keyboard interrupt from a console app, not to be confused with 'interrupt_request'.
    client = ZMQKernelClient()
    log_error = mocker.patch.object(client.log, "error")
    started = create_async_waiter()
    async with client.subprocess_kernel():
        pen = Caller().call_soon(client.monitor_heartbeat, 0.1, started=started.wake)
        await started
        result = await client.execute("get_ipython().parent.connections[0]._sockets['hb'].close()")
        assert result["content"]["status"] == "ok"
        await pen.wait(timeout=utils.TIMEOUT)

    assert log_error


@pytest.mark.parametrize("topic", ["zmq", "kernel"])
async def test_iopub_welcome(topic: str, client: ZMQKernelClient, connection_name: str):
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
