from __future__ import annotations

import os
import signal
import sys
from typing import TYPE_CHECKING, Literal

import pytest
from aiologic.lowlevel import create_async_event, create_async_waiter

from async_kernel.messaging.zmq import ZMQClient
from async_kernel.typing import Channel, Content, MsgType
from tests import utils

if TYPE_CHECKING:
    import pathlib


# @pytest.mark.parametrize("test_mode", ["reply", "interrupt", "allow_stdin=False"])
@pytest.mark.parametrize("test_mode", ["reply", "allow_stdin=False"])
@pytest.mark.parametrize("mode", ["input", "password"])
async def test_input(
    subprocess_kernel_client: ZMQClient,
    mode: Literal["input", "password"],
    test_mode: Literal["interrupt", "reply", "allow_stdin=False"],
):

    async def input_handler(content: Content) -> str:
        ready.set()
        if test_mode == "interrupt":
            await create_async_waiter()
        return str(content)

    ready, client, theprompt = create_async_event(), subprocess_kernel_client, "Enter a value >"
    await client.kernel_info()

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
        await client.send_message(client.msg(MsgType.interrupt_request, None, Channel.control))
        reply = await pen
        assert reply["content"]["status"] == "error"
        assert reply["content"]["traceback"][0] == "async_kernel.common.KernelInterrupt\n"
        # Check the interface is still working
        assert (await client.execute("1+1"))["content"]["status"] == "ok"
    else:
        reply = await pen
        assert reply["content"]["status"] == "ok"
        val = reply["content"]["user_expressions"]["response"]["data"]["text/plain"]
        val_ = eval(eval(val))
        assert val_ == {"prompt": theprompt, "password": mode == "password"}


@pytest.mark.parametrize("mode", ["exec_request_sync", "caller", "exec_request_async"])
async def test_interrupt_request(
    subprocess_kernel_client: ZMQClient, mode: Literal["exec_request_sync", "exec_request_async", "caller"]
):

    client = subprocess_kernel_client
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
        client.send_message(client.msg(MsgType.interrupt_request, None, Channel.control))
        reply = await pen

        assert reply["content"]["status"] == "error"
        assert reply["content"].get("ename") == "KernelInterrupt"
        if mode == "caller":
            code = "assert pen_timeout.done()"
            user_expressions = {"result": "pen_timeout.exception()"}
            reply = await client.execute(code, user_expressions=user_expressions)
            assert "KernelInterrupt" in reply["content"]["user_expressions"]["result"]["data"]["text/plain"]


async def test_subprocess_kernel_monitor_heartbeat(anyio_backend, mocker):
    # This is the keyboard interrupt from a console app, not to be confused with 'interrupt_request'.
    client = ZMQClient()
    log_error = mocker.patch.object(client.log, "error")
    with pytest.raises(RuntimeError, match="Heartbeat not detected"):  # noqa: PT012
        async with client.subprocess_kernel(heartbeat_interval=0.1):
            result = await client.execute("get_ipython().parent.connections[0]._sockets['hb'].close()")
            assert result["content"]["status"] == "ok"
            await create_async_waiter().with_(timeout=utils.TIMEOUT)
    assert log_error


@pytest.mark.skipif(sys.platform == "win32", reason="Can't simulate keyboard interrupt on windows.")
async def test_subprocess_kernel_keyboard_interrupt(tmp_path: pathlib.Path, anyio_backend):
    # This is the keyboard interrupt from a console app, not to be confused with 'interrupt_request'.
    client = ZMQClient()
    okay = False
    with pytest.raises(RuntimeError, match="Heartbeat not detected"):  # noqa: PT012
        async with client.subprocess_kernel(heartbeat_interval=0.1) as process:
            # Simulate a keyboard interrupt from the console.
            result = await client.execute("import os\npid=os.getpid()", user_expressions={"pid": "pid"})
            pid = int(result["content"]["user_expressions"]["pid"]["data"]["text/plain"])
            assert pid == process.pid
            assert os.getpid() != process.pid
            os.kill(process.pid, signal.SIGINT)
            okay = True
            await create_async_waiter().with_(timeout=utils.TIMEOUT)
    assert okay, "Code did not reach interrupted before heartbeat error."
