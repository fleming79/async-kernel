from __future__ import annotations

import pathlib
import threading
from typing import TYPE_CHECKING, Any, Literal

import anyio
import pytest
from IPython.core import page

import async_kernel.utils
from async_kernel import Kernel, Pending
from async_kernel.asyncshell import AsyncInteractiveShell, AsyncInteractiveSubshell, SubshellManager
from async_kernel.caller import Caller
from async_kernel.comm import Comm
from async_kernel.typing import Channel, MsgType, RunMode, Tags
from tests import utils

if TYPE_CHECKING:
    from collections.abc import Mapping

    from jupyter_client.asynchronous.client import AsyncKernelClient


# pyright: reportPrivateUsage=false


async def test_load_connection_info_error(kernel: Kernel, tmp_path):
    with pytest.raises(RuntimeError):
        kernel.load_connection_info({})


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


async def test_caller(kernel: Kernel):
    assert isinstance(kernel.caller, Caller)


@pytest.mark.parametrize("mode", ["shell_timeout", "tags"])
async def test_execute_shell_timeout(client: AsyncKernelClient, kernel: Kernel, mode: str):
    await utils.clear_iopub(client)
    if mode == "shell_timeout":
        kernel.shell.timeout = 0.1
        metadata = {}
    else:
        metadata = {"tags": ["timeout=0.1"]}
    last_stop_time = kernel.shell._stop_on_error_info
    try:
        code = "\n".join(["import anyio", "await anyio.sleep_forever()"])
        msg_id, content = await utils.execute(client, code=code, metadata=metadata, clear_pub=False)
        assert last_stop_time == kernel.shell._stop_on_error_info, "Should not cause cancellation"
        assert content["status"] == "ok"
        await utils.check_pub_message(client, msg_id, execution_state="busy")
        await utils.check_pub_message(client, msg_id, msg_type="execute_input")
        expected = {"traceback": [], "ename": "TimeoutError", "evalue": "Cell execute timeout"}
        await utils.check_pub_message(client, msg_id, msg_type="error", **expected)
        await utils.check_pub_message(client, msg_id, execution_state="idle")
    finally:
        kernel.shell.timeout = 0.0


async def test_bad_message(client: AsyncKernelClient):
    await utils.send_shell_message(client, "bad_message", reply=False)  # pyright: ignore[reportArgumentType]
    await utils.send_control_message(client, "bad_message", reply=False)  # pyright: ignore[reportArgumentType]
    await utils.execute(client, "")


async def test_reset_shell(kernel: Kernel, client: AsyncKernelClient):
    kernel.shell.reset()
    assert kernel.shell.execution_count == 0
    await utils.execute(client, "")
    assert kernel.shell.execution_count == 1
    kernel.shell.reset()
    assert kernel.shell.execution_count == 0


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


async def test_save_history(client: AsyncKernelClient, tmp_path):
    file = tmp_path.joinpath("hist.out")
    client.execute("a=1")
    await utils.wait_for_idle(client)
    client.execute('b="abcþ"')
    await utils.wait_for_idle(client)
    _, reply = await utils.execute(client, f"%hist -f {file}")
    assert reply["status"] == "ok"
    with file.open("r", encoding="utf-8") as f:
        content = f.read()
    assert "a=1" in content
    assert 'b="abcþ"' in content


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("2+2", "complete"),
        ("raise = 2", "invalid"),
        ("a = [1,\n2,", "incomplete"),
        ("%%timeit\na\n\n", "complete"),
    ],
)
async def test_is_complete(client: AsyncKernelClient, code: str, status: str):
    # There are more test cases for this in core - here we just check
    # that the kernel exposes the interface correctly.
    client.is_complete(code)
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == status


async def test_message_order(client: AsyncKernelClient):
    N = 10  # number of messages to test

    _, reply = await utils.execute(client, "a = 1")
    offset = reply["execution_count"] + 1
    cell = "a += 1\na"

    # submit N executions as fast as we can
    msg_ids = [client.execute(cell) for _ in range(N)]
    # check message-handling order
    for i, msg_id in enumerate(msg_ids, offset):
        reply = await client.get_shell_msg()
        assert reply["content"]["execution_count"] == i
        assert reply["parent_header"]["msg_id"] == msg_id


@pytest.mark.parametrize("run_mode", RunMode)
@pytest.mark.parametrize(
    "code",
    [
        "some invalid code",
        """
        from async_kernel.caller import PendingCancelled,
        async def fail():,
            raise PendingCancelled,
        await fail()""",
    ],
)
async def test_execute_request_error(client: AsyncKernelClient, code: str, run_mode: RunMode):
    reply = await utils.send_shell_message(client, MsgType.execute_request, {"code": code, "silent": False})
    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "error"


async def test_execute_request_stop_on_error(client: AsyncKernelClient):
    client.execute("import anyio;await anyio.sleep(0.1);stop-here")
    _, content = await utils.execute(client)
    assert content["evalue"] == "Aborting due to prior exception"


async def test_complete_request(client: AsyncKernelClient):
    reply = await utils.send_shell_message(client, MsgType.complete_request, {"code": "hello", "cursor_pos": 0})
    assert reply["header"]["msg_type"] == "complete_reply"
    assert reply["content"]["status"] == "ok"


async def test_inspect_request(client: AsyncKernelClient):
    reply = await utils.send_shell_message(client, MsgType.inspect_request, {"code": "hello", "cursor_pos": 0})
    assert reply["header"]["msg_type"] == "inspect_reply"
    assert reply["content"]["status"] == "ok"


async def test_history_request(client: AsyncKernelClient, kernel: Kernel):
    assert kernel.shell
    # assert kernel.shell.history_manager

    # kernel.shell.history_manager.db = DummyDB()
    reply = await utils.send_shell_message(
        client, MsgType.history_request, {"hist_access_type": "", "output": "", "raw": ""}
    )
    assert reply["header"]["msg_type"] == "history_reply"
    assert reply["content"]["status"] == "ok"
    reply = await utils.send_shell_message(
        client, MsgType.history_request, {"hist_access_type": "tail", "output": "", "raw": ""}
    )
    assert reply["header"]["msg_type"] == "history_reply"
    assert reply["content"]["status"] == "ok"
    reply = await utils.send_shell_message(
        client, MsgType.history_request, {"hist_access_type": "range", "output": "", "raw": ""}
    )
    assert reply["header"]["msg_type"] == "history_reply"
    assert reply["content"]["status"] == "ok"
    reply = await utils.send_shell_message(
        client, MsgType.history_request, {"hist_access_type": "search", "output": "", "raw": ""}
    )
    assert reply["header"]["msg_type"] == "history_reply"
    assert reply["content"]["status"] == "ok"


async def test_comm_info_request(client: AsyncKernelClient):
    reply = await utils.send_shell_message(client, MsgType.comm_info_request)
    assert reply["header"]["msg_type"] == "comm_info_reply"
    assert reply["content"]["status"] == "ok"


async def test_comm_open_msg_close(client: AsyncKernelClient, kernel, mocker):
    pen = Pending[Comm]()
    handle_msg = Pending()
    handle_close = Pending()

    def cb(comm, _):
        pen.set_result(comm)

    kernel.comm_manager.register_target("my target", cb)
    # open a comm
    await utils.send_shell_message(
        client, MsgType.comm_open, {"content": {}, "comm_id": "comm id", "target_name": "my target"}, reply=False
    )
    comm = await pen
    reply = await utils.send_shell_message(client, MsgType.comm_info_request)
    assert reply["header"]["msg_type"] == "comm_info_reply"
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["comms"].get("comm id") == {"target_name": "my target"}

    comm.handle_msg = handle_msg.set_result  # pyright: ignore[reportAttributeAccessIssue]
    await utils.send_shell_message(client, MsgType.comm_msg, {"comm_id": comm.comm_id}, reply=False)
    await handle_msg
    assert isinstance(handle_msg.result(), dict)
    # close comm

    comm.handle_close = handle_close.set_result  # pyright: ignore[reportAttributeAccessIssue]
    await utils.send_shell_message(client, MsgType.comm_close, {"comm_id": comm.comm_id}, reply=False)
    await handle_close
    assert isinstance(handle_close.result(), dict)
    kernel.comm_manager.unregister_target("my target", cb)


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


@pytest.mark.parametrize("response", ["y", ""])
async def test_user_exit(client: AsyncKernelClient, kernel: Kernel, mocker, response: Literal["y", ""]):
    stop = mocker.patch.object(kernel.interface, "stop")
    raw_input = mocker.patch.object(kernel.interface, "raw_input", return_value=response)
    await utils.execute(client, "quit()")
    assert raw_input.call_count == 1
    assert stop.call_count == (1 if response == "y" else 0)


async def test_is_complete_request(client: AsyncKernelClient):
    reply = await utils.send_shell_message(client, MsgType.is_complete_request, {"code": "hello"})
    assert reply["header"]["msg_type"] == "is_complete_reply"


@pytest.mark.parametrize("command", ["debugInfo", "inspectVariables", "modules", "dumpCell", "source"])
async def test_debug_static(client: AsyncKernelClient, command: str, mocker):
    # These are tests on the debugger that don't required the debugger to be connected.
    code = "my_variable=123"
    if command == "debugInfo":
        mocker.patch.object(async_kernel.utils, "LAUNCHED_BY_DEBUGPY", new=True)
        assert async_kernel.utils.LAUNCHED_BY_DEBUGPY
    reply = await utils.send_control_message(
        client, MsgType.debug_request, {"type": "request", "seq": 1, "command": command, "arguments": {"code": code}}
    )
    assert reply["content"]["status"] == "ok"
    if command == "dumpCell":
        path = reply["content"]["body"]["sourcePath"]
        reply = await utils.send_control_message(
            client,
            MsgType.debug_request,
            {"type": "request", "seq": 1, "command": "source", "arguments": {"source": {"path": path}}},
        )
        assert reply["content"]["status"] == "ok"
        assert reply["content"]["body"] == {"content": code}


async def test_debug_raises_no_socket(kernel: Kernel):
    debugger = kernel.shell.debugger
    assert debugger
    with pytest.raises(RuntimeError):
        await debugger.debugpy_client.send_request({})


async def test_debug_not_connected(client: AsyncKernelClient):
    reply = await utils.send_control_message(
        client, MsgType.debug_request, {"type": "request", "seq": 1, "command": "disconnect", "arguments": {}}
    )
    assert reply["content"]["status"] == "error"
    assert reply["content"]["evalue"] == "Debugy client not connected."


@pytest.mark.parametrize("variable_name", ["my_variable", "invalid variable name", "special variables"])
async def test_debug_static_richInspectVariables(client: AsyncKernelClient, variable_name: str):
    # These are tests on the debugger that don't required the debugger to be connected.
    reply = await utils.send_control_message(
        client,
        MsgType.debug_request,
        {
            "type": "request",
            "seq": 1,
            "command": "richInspectVariables",
            "arguments": {"code": "my_variable=123", "variableName": variable_name},
        },
    )
    assert reply["content"]["status"] == "ok"


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
    ],
)
async def test_magic(client: AsyncKernelClient, code: str, kernel: Kernel, monkeypatch):
    await utils.clear_iopub(client)
    monkeypatch.setenv("JUPYTER_RUNTIME_DIR", str(pathlib.Path(kernel.connection_file).parent))
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


async def test_shell_can_set_namespace(kernel: Kernel):
    kernel.shell.user_ns["extra"] = "Something extra"
    kernel.shell.user_ns = {}
    assert set(kernel.shell.user_ns) == {"Out", "_oh", "In", "exit", "_dh", "open", "get_ipython", "_ih", "quit"}


async def test_shell_display_hook_reg(kernel: Kernel):
    val: None | dict = None

    def my_hook(msg):
        nonlocal val
        val = msg

    kernel.shell.display_pub.register_hook(my_hook)
    assert my_hook in kernel.shell.display_pub._hooks
    kernel.shell.display_pub.publish({"test": True})
    kernel.shell.display_pub.unregister_hook(my_hook)
    assert my_hook not in kernel.shell.display_pub._hooks
    assert val


@pytest.mark.parametrize("mode", RunMode)
async def test_header_mode(client: AsyncKernelClient, mode: RunMode):
    code = f"""
{mode}
import time
time.sleep(0.1)
print("{mode.name}")
"""
    await utils.clear_iopub(client)
    _, reply = await utils.execute(client, code, clear_pub=False)
    assert reply["status"] == "ok"
    stdout, _ = await utils.assemble_output(client)
    assert mode.name in stdout


@pytest.mark.parametrize(
    "code",
    [
        "from async_kernel import Caller; Caller().call_later(str, 0, 123)",
        "from async_kernel import Caller; Caller().call_soon(print, 'hello')",
    ],
)
async def test_namespace_default(client: AsyncKernelClient, code: str):
    assert code
    _, reply = await utils.execute(client, code)
    assert reply["status"] == "ok"


@pytest.mark.parametrize("channel", [Channel.shell, Channel.control])
async def test_invalid_message(client: AsyncKernelClient, channel: Literal[Channel.shell, Channel.control]):
    f = utils.send_control_message if channel == "control" else utils.send_shell_message
    await f(client, "test_invalid_message", reply=False)  # pyright: ignore[reportArgumentType]
    await anyio.sleep(0.1)


async def test_run_mode_tag(client: AsyncKernelClient):
    metadata = {"tags": [RunMode.thread]}
    _, content = await utils.execute(
        client,
        "import threading;thread_name=threading.current_thread().name",
        metadata=metadata,
        user_expressions={"thread_name": "thread_name"},
    )
    assert content["status"] == "ok"
    assert "async_kernel_caller" in content["user_expressions"]["thread_name"]["data"]["text/plain"]


async def test_cell_top_line_to_thread(client: AsyncKernelClient):
    _, content = await utils.execute(
        client,
        "# thread\nimport threading;thread_name=threading.current_thread().name",
        user_expressions={"thread_name": "thread_name"},
    )
    assert content["status"] == "ok"
    assert "async_kernel_caller" in content["user_expressions"]["thread_name"]["data"]["text/plain"]


async def test_cell_top_line_to_thread_named(client: AsyncKernelClient):
    _, content = await utils.execute(
        client,
        "# thread name='My thread'\nimport threading;thread_name=threading.current_thread().name",
        user_expressions={"thread_name": "thread_name"},
    )
    assert content["status"] == "ok"
    assert "My thread" in content["user_expressions"]["thread_name"]["data"]["text/plain"]


@pytest.mark.parametrize("mode", ["raises", "not raised"])
async def test_tag_raises_exception(client: AsyncKernelClient, mode: Literal["raises", "not raised"]):
    match mode:
        case "raises":
            code = f'raise RuntimeError("{mode}")'
        case "not raised":
            code = "pass"
    _, content = await utils.execute(client, code, metadata={"tags": [Tags.raises_exception]})
    assert content["status"] == "error"
    assert mode in content["evalue"]


@pytest.mark.parametrize(("value", "expected"), [("stop-on-error=True", "error"), ("stop-on-error=False", "ok")])
async def test_tag_stop_on_error(kernel: Kernel, client: AsyncKernelClient, value: str, expected: str):
    try:
        kernel.shell.stop_on_error_time_offset = float(utils.TIMEOUT)
        _, content = await utils.execute(client, "fail", metadata={"tags": [Tags.raises_exception, value]})
        assert content["status"] == "error"
        _, content = await utils.execute(client, "a=10")
        assert content["status"] == expected
    finally:
        kernel.shell.stop_on_error_time_offset = 0
        kernel.shell._stop_on_error_info.clear()


async def test_get_parent(client: AsyncKernelClient, kernel: Kernel):
    assert kernel.get_parent() is None
    code = "assert 'header' in get_ipython().kernel.get_parent()"
    await utils.execute(client, code)


def test_subshell_manager():
    with pytest.raises(RuntimeError):
        SubshellManager()


async def test_subshell(client: AsyncKernelClient, kernel: Kernel):
    subshell_id = kernel.subshell_manager.create_subshell(protected=True).subshell_id
    subshell = kernel.subshell_manager.subshells[subshell_id]

    assert repr(kernel.main_shell) == "<IPythonAsyncInteractiveShell  kernel_name: 'async' subshell_id: None>"
    assert repr(subshell) == f"<IPythonInteractiveSubshell kernel_name: 'async'  subshell_id: {subshell_id}>"

    assert kernel.main_shell.user_ns is kernel.main_shell.user_global_ns
    assert subshell.user_ns is not kernel.main_shell.user_ns
    assert subshell.user_global_ns is kernel.main_shell.user_global_ns
    kernel.main_shell.user_ns["a"] = 1
    await utils.execute(client, code="a=10", subshell_id=subshell_id)
    assert subshell.user_ns["a"] == 10
    await utils.execute(client, code="b=20", header_extras={"subshell_id": subshell_id})
    assert subshell.user_ns["b"] == 20

    # Switch subshell using context manager.
    with async_kernel.utils.subshell_context(subshell.subshell_id):
        assert async_kernel.utils.get_subshell_id() == subshell.subshell_id
        assert kernel.shell is subshell
        with async_kernel.utils.subshell_context(None):
            assert kernel.shell is kernel.main_shell
            assert async_kernel.utils.get_subshell_id() is None
        # Test reset
        pen = Caller().call_soon(anyio.sleep_forever)
        shell = await Caller().call_soon(lambda: async_kernel.utils.get_kernel().shell)
        assert shell is subshell
        kernel.shell.reset()
        assert pen.cancelled()

    # delete
    assert subshell_id in kernel.subshell_manager.subshells
    kernel.subshell_manager.delete_subshell(subshell_id)
    assert subshell_id in kernel.subshell_manager.subshells, "Protected should not stop when deleted"
    SubshellManager.stop_all_subshells(force=True)
    assert kernel.main_shell.user_ns["a"] == 1
    with pytest.raises(KeyError), async_kernel.utils.subshell_context(subshell.subshell_id):
        pass


async def test_page(client: AsyncKernelClient, kernel: Kernel):
    await utils.clear_iopub(client)
    msg_id = client.execute("?", allow_stdin=True)
    await utils.check_pub_message(client, msg_id, execution_state="busy")
    await utils.check_pub_message(client, msg_id, msg_type="execute_input")
    msg = await utils.check_pub_message(client, msg_id, msg_type="stream")
    assert msg["header"]["msg_type"] == "stream"
    assert list(msg["content"]) == ["name", "text"]
    await utils.check_pub_message(client, msg_id, execution_state="idle")
    page.page({"data": {"text/plain": "hello, world"}, "metadata": {}})
    await utils.check_pub_message(client, "", msg_type="display_data")


async def test_do_complete(kernel: Kernel):
    content = await kernel.do_complete("dir", None)
    assert list(content) == ["matches", "cursor_end", "cursor_start", "metadata", "status"]


async def test_do_inspect(kernel: Kernel):
    content = await kernel.do_inspect("dir()", 4)
    assert content["found"]


async def test_do_history(kernel: Kernel):
    content = await kernel.do_history(hist_access_type="", output="", raw="")
    assert list(content) == ["history", "status"]


async def test_do_execute(kernel: Kernel):
    (cell_id,) = "3"
    content = await kernel.do_execute(
        "from async_kernel import utils\ncell_id = utils.get_cell_id()",
        silent=False,
        store_history=False,
        cell_id=cell_id,
        user_expressions={"cell_id": "cell_id"},
    )
    assert list(content) == ["status", "execution_count", "user_expressions"]
    assert cell_id in content["user_expressions"]["cell_id"]["data"]["text/plain"]


async def test_get_input(kernel: Kernel, mocker):
    requester = mocker.patch.object(kernel.interface, "input_request")
    kernel.raw_input()
    kernel.getpass()
    assert requester.call_count == 2


async def test_AsyncInteractiveShell_subclass(kernel):
    with pytest.raises(RuntimeError, match="too late"):

        class MyShell(AsyncInteractiveShell):  # pyright: ignore[reportUnusedClass]
            pass

    with pytest.raises(RuntimeError, match="too late"):

        class MySubshell(AsyncInteractiveSubshell):  # pyright: ignore[reportUnusedClass]
            pass
