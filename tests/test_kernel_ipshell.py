from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING, Literal

import anyio
import pytest
from IPython.core import page

import async_kernel.utils
from async_kernel import Kernel, Pending
from async_kernel.caller import Caller
from async_kernel.comm import Comm
from async_kernel.common import MethodNotSupported
from async_kernel.typing import Channel, Content, Message, MsgType, RunMode, Tags
from tests import utils

if TYPE_CHECKING:
    from async_kernel.client.zmq import ZMQKernelClient
    from async_kernel.interface import BaseInterface
    from async_kernel.interface.zmq import ZMQInterface
    from async_kernel.shell import IPShell


# pyright: reportPrivateUsage=false


@pytest.mark.parametrize("mode", ["shell_timeout", "tags"])
async def test_execute_shell_timeout(client: ZMQKernelClient, kernel: Kernel, mode: str):

    if mode == "shell_timeout":
        kernel.shell.timeout = 0.1
        metadata = {}
    else:
        metadata = {"tags": ["timeout=0.1"]}
    last_stop_time = kernel.shell._stop_on_error_info
    try:
        code = "\n".join(["import anyio", "await anyio.sleep_forever()"])
        reply = await client.execute(code, metadata=metadata)
        assert last_stop_time == kernel.shell._stop_on_error_info, "Should not cause cancellation"
        assert reply["content"]["status"] == "error"
    finally:
        kernel.shell.timeout = 0.0


async def test_bad_message(client: ZMQKernelClient):
    await client.send_message(client.msg(MsgType.execute_request))
    await client.send_message(client.msg(MsgType.execute_request, channel=Channel.control))
    await client.execute("")


async def test_reset_shell(kernel: Kernel, client: ZMQKernelClient):
    kernel.shell.reset()
    assert kernel.shell.execution_count == 0
    await client.execute("")
    assert kernel.shell.execution_count == 1
    kernel.shell.reset()
    assert kernel.shell.execution_count == 0


async def test_save_history(client: ZMQKernelClient, tmp_path):
    file = tmp_path.joinpath("hist.out")
    await client.execute("a=1")
    await client.execute('b="abcþ"')
    reply = await client.execute(f"%hist -f {file}")
    assert reply["content"]["status"] == "ok"
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
async def test_is_complete_2(client: ZMQKernelClient, code: str, status: str):
    # There are more test cases for this in core - here we just check
    # that the kernel exposes the interface correctly.
    reply = await client.is_complete(code)
    assert reply["content"]["status"] == status


async def test_noop(kernel: Kernel[ZMQInterface, IPShell]):
    with pytest.raises(MethodNotSupported):
        kernel.shell.init_prefilter()


async def test_message_order(kernel: Kernel, client: ZMQKernelClient):
    N = 10  # number of messages to test

    reply = await client.execute("a = 1")
    cnt = reply["content"].get("execution_count")
    assert isinstance(cnt, int)
    offset = cnt + 1
    cell = "a += 1\na"

    # submit N executions as fast as we can
    pending = [client.execute(cell) for _ in range(N)]
    # check message-handling order
    n = 0
    async for pen in kernel.caller.as_completed(pending):
        reply = pen.result()
        assert reply["content"].get("execution_count") == n + offset
        parent = reply["parent_header"]
        assert parent
        assert parent["msg_id"] == pending[n].msg_id
        n = n + 1


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
async def test_execute_request_error(client: ZMQKernelClient, code: str, run_mode: RunMode):
    reply = await client.execute(code, silent=False)
    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "error"


async def test_execute_request_stop_on_error(client: ZMQKernelClient):
    client.execute("import anyio;await anyio.sleep(0.1);stop-here")
    reply = await client.execute("1+1")
    assert reply["content"].get("evalue") == "Aborting due to prior exception"


async def test_complete_request(client: ZMQKernelClient):
    reply = await client.complete("hello", 0)
    assert reply["header"]["msg_type"] == "complete_reply"
    assert reply["content"]["status"] == "ok"


async def test_inspect_request(client: ZMQKernelClient):
    reply = await client.inspect("hello", 0)
    assert reply["header"]["msg_type"] == "inspect_reply"
    assert reply["content"]["status"] == "ok"


async def test_history_request(client: ZMQKernelClient, kernel: Kernel):
    assert kernel.shell
    reply = await client.history(hist_access_type="tail")
    assert reply["header"]["msg_type"] == "history_reply"
    assert reply["content"]["status"] == "ok"

    reply = await client.history(hist_access_type="range")
    assert reply["header"]["msg_type"] == "history_reply"
    assert reply["content"]["status"] == "ok"

    reply = await client.history(hist_access_type="search")
    assert reply["header"]["msg_type"] == "history_reply"
    assert reply["content"]["status"] == "ok"


async def test_comm_info_request(client: ZMQKernelClient):
    reply = await client.comm_info()
    assert reply["header"]["msg_type"] == "comm_info_reply"
    assert reply["content"]["status"] == "ok"


async def test_comm_open_msg_close(client: ZMQKernelClient, kernel: Kernel, mocker):
    pen = Pending[Comm]()
    handle_msg = Pending()
    handle_close = Pending()

    def cb(comm, _):
        pen.set_result(comm)

    kernel.comm_manager.register_target("my target", cb)
    # open a comm
    client.send_message_no_reply(
        client.msg(MsgType.comm_open, {"content": {}, "comm_id": "comm id", "target_name": "my target"})
    )
    comm = await pen
    reply = await client.comm_info()
    assert reply["header"]["msg_type"] == "comm_info_reply"
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["comms"].get("comm id") == {"target_name": "my target"}

    comm.handle_msg = handle_msg.set_result  # pyright: ignore[reportAttributeAccessIssue]
    client.send_message_no_reply(client.msg(MsgType.comm_msg, {"comm_id": comm.comm_id}))
    await handle_msg
    assert isinstance(handle_msg.result(), dict)
    # close comm

    comm.handle_close = handle_close.set_result  # pyright: ignore[reportAttributeAccessIssue]
    client.send_message_no_reply(client.msg(MsgType.comm_close, {"comm_id": comm.comm_id}))
    await handle_close
    assert isinstance(handle_close.result(), dict)
    kernel.comm_manager.unregister_target("my target", cb)


@pytest.mark.parametrize("response", ["y", ""])
async def test_user_exit(client: ZMQKernelClient, kernel: Kernel, mocker, response: Literal["y", ""]):
    stop = mocker.patch.object(kernel.parent, "stop")
    raw_input = mocker.patch.object(kernel, "raw_input", return_value=response)
    await client.execute("quit()")
    assert raw_input.call_count == 1
    assert stop.call_count == (1 if response == "y" else 0)


async def test_is_complete_request(client: ZMQKernelClient):
    reply = await client.is_complete("hello")
    assert reply["header"]["msg_type"] == "is_complete_reply"


async def test_shell_can_set_namespace(kernel: Kernel):
    kernel.shell.user_ns["extra"] = "Something extra"
    kernel.shell.user_ns = {}
    expected = {"_oh", "quit", "In", "_dh", "Out", "open", "_", "__", "___", "_ih", "exit"}
    assert set(kernel.shell.user_ns) == expected


async def test_shell_display_hook_reg(kernel: Kernel[ZMQInterface, IPShell]):
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
async def test_header_mode(client: ZMQKernelClient, mode: RunMode):
    code = f"""
{mode}
print("{mode.name}")
"""
    async with client.iopub_subscribe() as queue:
        reply = await client.execute(code)
        assert reply["content"]["status"] == "ok"
        async for msg in queue:
            if msg["header"]["msg_type"] == "stream":
                assert mode.value in msg["content"]["text"]
                break


@pytest.mark.parametrize(
    "code",
    [
        "from async_kernel import Caller; Caller().call_later(str, 0, 123)",
        "from async_kernel import Caller; Caller().call_soon(print, 'hello')",
    ],
)
async def test_namespace_default(client: ZMQKernelClient, code: str):
    assert code
    reply = await client.execute(code)
    assert reply["content"]["status"] == "ok"


async def test_run_mode_tag(client: ZMQKernelClient):
    metadata = {"tags": [RunMode.thread]}
    reply: Message[Content] = await client.execute(
        "import threading;thread_name=threading.current_thread().name",
        metadata=metadata,
        user_expressions={"thread_name": "thread_name"},
    )
    assert reply["content"]["status"] == "ok"
    assert "async_kernel_caller" in reply["content"]["user_expressions"]["thread_name"]["data"]["text/plain"]


async def test_cell_top_line_to_thread(client: ZMQKernelClient):
    reply = await client.execute(
        "# thread\nimport threading;thread_name=threading.current_thread().name",
        user_expressions={"thread_name": "thread_name"},
    )
    assert reply["content"]["status"] == "ok"
    assert "async_kernel_caller" in reply["content"]["user_expressions"]["thread_name"]["data"]["text/plain"]


async def test_cell_top_line_to_thread_named(client: ZMQKernelClient):
    reply = await client.execute(
        "# thread name='My thread'\nimport threading;thread_name=threading.current_thread().name",
        user_expressions={"thread_name": "thread_name"},
    )
    assert reply["content"]["status"] == "ok"
    assert "My thread" in reply["content"]["user_expressions"]["thread_name"]["data"]["text/plain"]


@pytest.mark.parametrize("mode", ["raises", "not raised"])
async def test_tag_raises_exception(client: ZMQKernelClient, mode: Literal["raises", "not raised"]):
    match mode:
        case "raises":
            code = f'raise RuntimeError("{mode}")'
        case "not raised":
            code = "pass"
    reply = await client.execute(code, metadata={"tags": [Tags.raises_exception]})
    assert reply["content"]["status"] == "error"
    assert mode in reply["content"]["evalue"]


@pytest.mark.parametrize(("value", "expected"), [("stop-on-error=True", "error"), ("stop-on-error=False", "ok")])
async def test_tag_stop_on_error(kernel: Kernel, client: ZMQKernelClient, value: str, expected: str):
    try:
        kernel.shell.stop_on_error_time_offset = float(utils.TIMEOUT)
        reply = await client.execute("fail", metadata={"tags": [Tags.raises_exception, value]})
        assert reply["content"]["status"] == "error"
        reply = await client.execute("a=10")
        assert reply["content"]["status"] == expected
    finally:
        kernel.shell.stop_on_error_time_offset = 0
        kernel.shell._stop_on_error_info.clear()


async def test_get_parent(client: ZMQKernelClient, kernel: Kernel):
    assert kernel.get_parent() is None
    code = "assert 'header' in get_ipython().kernel.get_parent()"
    await client.execute(code)


async def test_subshell(client: ZMQKernelClient, kernel: Kernel):
    subshell = kernel.create_subshell(protected=True)
    assert subshell.subshell_id

    assert repr(kernel.main_shell) == "<IPShell 🔐: Main Shell>"
    assert repr(subshell) == f"<IPShell 🔐: Subshell ({subshell.subshell_id})>"

    assert kernel.main_shell.user_ns is kernel.main_shell.user_global_ns
    assert subshell.user_ns is not kernel.main_shell.user_ns
    assert subshell.user_global_ns is kernel.main_shell.user_global_ns
    kernel.main_shell.user_ns["a"] = 1
    await client.execute("a=10", subshell_id=subshell.subshell_id)
    assert subshell.user_ns["a"] == 10
    await client.execute("b=20", subshell_id=subshell.subshell_id)
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
    assert subshell.subshell_id in kernel.subshells
    subshell.stop()
    assert subshell.subshell_id in kernel.subshells, "Protected should not stop when deleted"
    subshell.stop(force=True)
    assert subshell.subshell_id not in kernel.subshells, "Protected should not stop when deleted"


async def test_page(client: ZMQKernelClient, kernel: Kernel):
    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute("?")
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_status, execution_state="busy")
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_execute_input)
        msg = utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_stream)
        assert msg["header"]["msg_type"] == "stream"
        assert list(msg["content"]) == ["name", "text"]
        utils.check_pub_message(await anext(reader), execution_state="idle")
        page.page({"data": {"text/plain": "hello, world"}, "metadata": {}})
        utils.check_pub_message(await anext(reader), msg_type=MsgType.iopub_display_data)


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
    requester = mocker.patch.object(kernel.parent, "input_request")
    kernel.raw_input()
    kernel.getpass()
    assert requester.call_count == 2


async def test_redirect_stdout(kernel: Kernel):

    with async_kernel.utils.redirect_stdout(io.StringIO()) as f:
        print("hello")
        print("world")
    assert f.getvalue() == "hello\nworld\n"


async def test_redirect_stderr(kernel: Kernel):

    with async_kernel.utils.redirect_stderr(io.StringIO()) as f:
        sys.stderr.write("hello")
        sys.stderr.flush()
    assert f.getvalue() == "hello"


async def test_extension_manager(kernel: Kernel[BaseInterface, IPShell]):
    subshell = kernel.create_subshell()
    assert kernel.main_shell.extension_manager is not subshell.extension_manager

    result = subshell.extension_manager.load_extension("IPython.extensions.autoreload")
    assert result is None
    assert subshell.extension_manager.shell is subshell
    assert "autoreload" in subshell.magics_manager.magics["line"]
    with subshell.context():
        # a
        result = await kernel.do_execute("a = 1+1", silent=False)
        assert subshell.user_ns["a"] == 2
