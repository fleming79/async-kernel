from __future__ import annotations

import pathlib
import threading
from typing import TYPE_CHECKING, Any, Literal

import anyio
import pytest

from async_kernel.interface.zmq import ZMQKernelInterface
from async_kernel.typing import MsgType
from tests import utils

if TYPE_CHECKING:
    from collections.abc import Mapping

    from jupyter_client.asynchronous.client import AsyncKernelClient

    from async_kernel import Kernel


# pyright: reportPrivateUsage=false


async def test_load_connection_info_error(kernel: Kernel):
    assert isinstance(kernel.interface, ZMQKernelInterface)
    with pytest.raises(RuntimeError):
        kernel.interface.load_connection_info({})


async def test_execute_request_success(client: AsyncKernelClient):
    reply: dict[Any, Any] | Mapping[str, Mapping[str, Any]] = await utils.send_shell_message(
        client, MsgType.execute_request, {"code": "1 + 1", "silent": False}
    )
    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "ok"


async def test_simple_print(kernel: Kernel, client: AsyncKernelClient):
    """Simple print statement in kernel."""
    await utils.clear_iopub(client)
    client.execute("print('🌈')")
    stdout, stderr = await utils.assemble_output(client)
    assert stdout == "🌈\n"
    assert stderr == ""


@pytest.mark.parametrize("test_mode", ["interrupt", "reply", "allow_stdin=False"])
@pytest.mark.parametrize("mode", ["input", "password"])
async def test_input(
    subprocess_kernels_client,
    mode: Literal["input", "password"],
    test_mode: Literal["interrupt", "reply", "allow_stdin=False"],
):
    client = subprocess_kernels_client
    client.input("Some input that should be discarded")
    theprompt = "Enter a value >"
    match mode:
        case "input":
            code = f"response = input('{theprompt}')"
        case "password":
            code = f"import getpass;response = getpass.getpass('{theprompt}')"
    # allow_stdin=False
    if test_mode == "allow_stdin=False":
        _, reply = await utils.execute(client, code, allow_stdin=False)
        assert reply["status"] == "error"
        assert reply["ename"] == "StdinNotImplementedError"
        return
    msg_id = client.execute(code, allow_stdin=True, user_expressions={"response": "response"})
    msg = await client.get_stdin_msg()
    assert msg["header"]["msg_type"] == "input_request"
    content = msg["content"]
    assert content["prompt"] == theprompt
    # interrupt
    if test_mode == "interrupt":
        await utils.send_control_message(client, MsgType.interrupt_request)
        reply = await utils.get_reply(client, msg_id, clear_pub=False)
        assert reply["content"]["status"] == "error"
        return
    # reply
    text = "some text"
    client.input(text)
    reply = await utils.get_reply(client, msg_id)
    assert reply["content"]["status"] == "ok"
    assert text in reply["content"]["user_expressions"]["response"]["data"]["text/plain"]


async def test_interrupt_request(client: AsyncKernelClient, kernel: Kernel):
    event = threading.Event()
    kernel.interface.interrupts.add(event.set)
    reply = await utils.send_control_message(client, MsgType.interrupt_request)
    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"] == {"status": "ok"}
    assert event


async def test_interrupt_request_async_request(subprocess_kernels_client: AsyncKernelClient):
    await utils.clear_iopub(subprocess_kernels_client)
    client = subprocess_kernels_client
    msg_id = client.execute(f"import anyio;await anyio.sleep({utils.TIMEOUT * 4})")
    await utils.check_pub_message(client, msg_id, execution_state="busy")
    await utils.check_pub_message(client, msg_id, msg_type="execute_input")
    await anyio.sleep(0.5)
    reply = await utils.send_control_message(client, MsgType.interrupt_request)
    reply = await utils.get_reply(client, msg_id)
    assert reply["content"]["status"] == "error"


async def test_interrupt_request_direct_exec_request(subprocess_kernels_client: AsyncKernelClient):
    await utils.clear_iopub(subprocess_kernels_client)
    client = subprocess_kernels_client
    msg_id = client.execute(f"import time\nprint('started')\ntime.sleep({utils.TIMEOUT * 2})")
    await utils.check_pub_message(client, msg_id, execution_state="busy")
    await utils.check_pub_message(client, msg_id, msg_type="execute_input")
    await utils.check_pub_message(client, msg_id, msg_type="stream", text="started")
    await utils.send_control_message(client, MsgType.interrupt_request)
    reply = await utils.get_reply(client, msg_id)
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "KernelInterrupt"


async def test_interrupt_request_direct_task(subprocess_kernels_client: AsyncKernelClient):
    await utils.clear_iopub(subprocess_kernels_client)
    code = f"""
    import time
    from async_kernel import Caller
    await Caller().call_soon(lambda: [print('started'), time.sleep({utils.TIMEOUT * 2})])
    """
    client = subprocess_kernels_client
    msg_id = client.execute(code)
    await utils.check_pub_message(client, msg_id, execution_state="busy")
    await utils.check_pub_message(client, msg_id, msg_type="execute_input")
    await utils.check_pub_message(client, msg_id, msg_type="stream", text="started")
    await utils.send_control_message(client, MsgType.interrupt_request)
    reply = await utils.get_reply(client, msg_id)
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "KernelInterrupt"


@pytest.mark.parametrize(
    "code",
    argvalues=[
        "%connect_info",
        "%callers",
        "%subshell",
        "%pip install anyio",
        "%uv pip install anyio",
        "%thread\nprint('okay')",
        """%%thread name="Trio executor" backend=trio\nfrom async_kernel import Caller; assert Caller().name == "Trio executor";print('okay')""",
        "import asyncio\n%asyncio await asyncio.sleep(0)\nprint('okay')",
        "import trio\n%trio await trio.sleep(0)\nprint('okay')",
        "%mkdir test\n%rmdir test\n%ls",
    ],
)
async def test_magic(client: AsyncKernelClient, code: str, kernel: Kernel, monkeypatch):
    await utils.clear_iopub(client)
    assert isinstance(kernel.interface, ZMQKernelInterface)
    monkeypatch.setenv("JUPYTER_RUNTIME_DIR", str(pathlib.Path(kernel.interface.connection_file).parent))
    assert code
    _, reply = await utils.execute(client, code, clear_pub=False)
    assert reply["status"] == "ok"
    stdout, _ = await utils.assemble_output(client)
    assert stdout


async def test_magic_error(client: AsyncKernelClient):
    _, reply = await utils.execute(client, "%%thread backend=trio\npass")
    assert reply["status"] == "error"
    assert "'name' must be specified when providing settings!" in reply["evalue"]
    _, reply = await utils.execute(client, "%%thread name=test not_an_option=True\npass")
    assert reply["status"] == "error"
    assert "One or more invalid options found" in reply["evalue"]


@pytest.mark.parametrize("code", argvalues=["%connect_info"])
async def test_magic_sync(client: AsyncKernelClient, code: str, kernel: Kernel, monkeypatch):
    result = kernel.main_shell.run_cell(code)
    assert result.success


async def test_shell_enable_gui(kernel: Kernel):
    # used by ipython AutoMagicChecker via is_shadowed (requires 'builitin')
    assert set(kernel.shell.ns_table) == {"user_global", "user_local", "builtin"}
    # U
    kernel.shell.enable_gui()
    with pytest.raises(RuntimeError):
        kernel.shell.enable_gui("not a gui")


async def test_load_connection_file_too_late(kernel: Kernel):
    assert isinstance(kernel.interface, ZMQKernelInterface)
    with pytest.raises(RuntimeError, match="It is too late to set the connection file"):
        kernel.interface.connection_file = "too_late.json"


async def test_already_initialized(kernel: Kernel):
    assert isinstance(kernel.interface, ZMQKernelInterface)
    assert kernel.config is kernel.interface.config
    config = kernel.config.copy()
    kernel.interface.initialize(["prog", "--quiet=False"])
    assert kernel.config == config


async def test_launch_too_late(kernel: Kernel):
    with pytest.raises(RuntimeError, match="An instance has already been created"):
        ZMQKernelInterface.launch_instance()


async def test_already_entered(kernel: Kernel):
    with pytest.raises(RuntimeError, match="this ZMQKernelInterface has already been entered"):
        async with ZMQKernelInterface():
            pass
