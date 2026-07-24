from __future__ import annotations

import pathlib
import threading
from typing import TYPE_CHECKING, Any, Literal

import pytest
from aiologic.lowlevel import create_async_waiter

from async_kernel import Pending
from async_kernel.interface.zmq import ZMQInterface
from async_kernel.typing import Channel, Content, MsgType
from tests import utils

if TYPE_CHECKING:
    from async_kernel import Kernel
    from async_kernel.client.zmq import ZMQKernelClient
    from async_kernel.shell import IPShell


async def test_load_connection_info_error(kernel: Kernel):
    assert isinstance(kernel.parent, ZMQInterface)
    with pytest.raises(RuntimeError):
        kernel.parent.load_connection_info({})


async def test_execute_request_success(client: ZMQKernelClient):
    reply = await client.execute("1 + 1")
    assert reply["header"]["msg_type"] == "execute_reply"
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


async def test_print_non_caller_thread(kernel: Kernel[ZMQInterface], client: ZMQKernelClient):

    async with client.iopub_subscribe() as queue:
        t = threading.Thread(target=print, args=["-non_caller_thread-"])
        t.start()
        async for msg in queue:
            assert msg["content"]["text"] == "-non_caller_thread-\n"
            break


@pytest.mark.parametrize("test_mode", ["interrupt", "reply", "allow_stdin=False"])
@pytest.mark.parametrize("mode", ["input", "password"])
async def test_input(
    subprocess_kernels_client: ZMQKernelClient,
    mode: Literal["input", "password"],
    test_mode: Literal["interrupt", "reply", "allow_stdin=False"],
):

    async def input_handler(content: Content) -> str:
        ready.wake()
        if test_mode == "interrupt":
            await create_async_waiter()
        return str(content)

    ready = create_async_waiter()
    client = subprocess_kernels_client
    theprompt = "Enter a value >"
    match mode:
        case "input":
            code = f"response = input('{theprompt}')"
        case "password":
            code = f"import getpass;response = getpass.getpass('{theprompt}')"

    if test_mode == "allow_stdin=False":
        reply = await client.execute(code)
        assert reply["content"]["status"] == "error"
        assert reply["content"].get("ename") == "RuntimeError"
        return

    pen = client.execute(code, input_handler=input_handler, user_expressions={"response": "response"})
    await ready

    if test_mode == "interrupt":
        await client.send_message(client.msg(msg_type=MsgType.interrupt_request, channel=Channel.control))
        reply = await pen
        assert reply["content"]
    else:
        reply = await pen
        assert reply["content"]["status"] == "ok"
        val = reply["content"]["user_expressions"]["response"]["data"]["text/plain"]
        val_ = eval(val)
        assert val_


async def test_interrupt_request_not_blocked(client: ZMQKernelClient, kernel: Kernel):
    pen: Any = Pending()
    kernel.active_execute_requests.add(pen)
    reply = await client.send_message(client.msg(MsgType.interrupt_request))
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"] == {"status": "ok"}
    assert pen.cancelled()


@pytest.mark.parametrize("mode", ["exec_request_sync", "caller", "exec_request_async"])
async def test_interrupt_request(
    subprocess_kernels_client: ZMQKernelClient, mode: Literal["exec_request_sync", "exec_request_async", "caller"]
):

    client = subprocess_kernels_client
    if mode == "exec_request_async":
        code = f"import anyio\nprint('started')\nawait anyio.sleep({utils.TIMEOUT * 4})"
    elif mode == "exec_request_sync":
        code = f"import time\nprint('started')\ntime.sleep({utils.TIMEOUT})"
    elif mode == "caller":
        code = f"""
    import time
    pen_timeout= get_ipython().kernel.caller.call_soon(lambda: [print('started'), time.sleep({utils.TIMEOUT * 2})])
    await pen_timeout
    """
    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        pen = client.execute(code)
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_status, execution_state="busy")
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_execute_input)
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_stream, text="started\n")
        client.send_message(client.msg(MsgType.interrupt_request))
        reply = await pen

        assert reply["content"]["status"] == "error"
        assert reply["content"].get("ename") == "KernelInterrupt"
        if mode == "caller":
            code = "assert pen_timeout.done()"
            user_expressions = {"result": "pen_timeout.exception()"}
            reply = await client.execute(code, user_expressions=user_expressions)
            assert "KernelInterrupt" in reply["content"]["user_expressions"]["result"]["data"]["text/plain"]


@pytest.mark.parametrize(
    "code",
    argvalues=[
        "%connect_info",
        "%callers",
        "%subshell",
        "%pip -V",
        "%uv -V",
        "%thread\nprint('okay')",
        """%%thread name="Trio executor" backend=trio\nfrom async_kernel import Caller; assert Caller().name == "Trio executor";print('okay')""",
        "import asyncio\n%asyncio await asyncio.sleep(0)\nprint('okay')",
        "import trio\n%trio await trio.sleep(0)\nprint('okay')",
        "%mkdir test\n%rmdir test\n%ls",
    ],
)
async def test_magic(client: ZMQKernelClient, code: str, kernel: Kernel, monkeypatch):

    assert isinstance(kernel.parent, ZMQInterface)
    monkeypatch.setenv("JUPYTER_RUNTIME_DIR", str(pathlib.Path(kernel.parent.connection_file).parent))
    assert code
    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute(code)
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_status, execution_state="busy")
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_execute_input)
        msg = utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_stream)
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_status, execution_state="idle")
    text = msg["content"]["text"]
    assert text
    match code:
        case "%connect_info":
            assert "Paste the above JSON into a file" in text
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
async def test_magic_sync(client: ZMQKernelClient, code: str, kernel: Kernel[ZMQInterface, IPShell], monkeypatch):
    result = kernel.main_shell.run_cell(code)
    assert result.success


async def test_shell_enable_gui(kernel: Kernel[ZMQInterface, IPShell]):
    # used by ipython AutoMagicChecker via is_shadowed (requires 'builitin')
    assert set(kernel.shell.ns_table) == {"user_global", "user_local", "builtin"}
    # U
    kernel.shell.enable_gui()
    with pytest.raises(RuntimeError):
        kernel.shell.enable_gui("not a gui")


async def test_load_connection_file_too_late(kernel: Kernel):
    assert isinstance(kernel.parent, ZMQInterface)
    with pytest.raises(RuntimeError, match="It is too late to set the connection file"):
        kernel.parent.connection_file = "too_late.json"


async def test_launch_too_late(kernel: Kernel):
    with pytest.raises(RuntimeError, match="An interface already exists!"):
        ZMQInterface.launch_instance()


async def test_already_entered(kernel: Kernel):
    with pytest.raises(RuntimeError, match="has already been entered"):
        async with kernel.parent:
            pass
