from __future__ import annotations

import inspect
import logging
import pathlib
import sys
import threading
import time
from typing import TYPE_CHECKING, Any, Literal, cast

import anyio
import pytest
import zmq

import async_kernel.utils
from async_kernel.caller import Caller
from async_kernel.comm import Comm
from async_kernel.compiler import murmur2_x86
from async_kernel.kernel import Kernel
from async_kernel.typing import ExecuteContent, Job, MsgType, RunMode, SocketID, Tags
from tests import utils

if TYPE_CHECKING:
    from collections.abc import Mapping

    from jupyter_client.asynchronous.client import AsyncKernelClient


from async_kernel.kernel import bind_socket

# pyright: reportPrivateUsage=false


@pytest.fixture(scope="module", params=["tcp", "ipc"] if sys.platform == "linux" else ["tcp"])
def transport(request):
    return request.param


def test_bind_socket(transport: Literal["tcp", "ipc"], tmp_path):
    ctx = zmq.Context()
    ip = tmp_path / "mypath" if transport == "ipc" else "0.0.0.0"
    try:
        socket = ctx.socket(zmq.SocketType.ROUTER)
        try:
            port = bind_socket(socket, transport, ip)  # pyright: ignore[reportArgumentType]
        finally:
            socket.close(linger=0)
        socket = ctx.socket(zmq.SocketType.ROUTER)
        try:
            assert bind_socket(socket, transport, ip, port) == port  # pyright: ignore[reportArgumentType]
            if transport == "tcp":
                with pytest.raises(RuntimeError):
                    bind_socket(socket, transport, ip, max_attempts=0)  # pyright: ignore[reportArgumentType]
                with pytest.raises(ValueError, match="Invalid transport"):
                    bind_socket(socket, "", ip, max_attempts=1)  # pyright: ignore[reportArgumentType]
        finally:
            socket.close(linger=0)
    finally:
        ctx.term()


@pytest.mark.parametrize("mode", ["direct", "proxy"])
async def test_iopub(kernel: Kernel, mode: Literal["direct", "proxy"]) -> None:
    def pubio_subscribe():
        """Consume messages."""

        socket = ctx.socket(zmq.SocketType.SUB)
        socket.connect(url)
        socket.setsockopt(zmq.SocketOption.SUBSCRIBE, b"")
        try:
            i = 0
            while i < n:
                msg = socket.recv_multipart()
                if msg[0] == b"0":
                    assert int(msg[1]) == i
                    i += 1
            # Also test iopub from a thread that doesn't have a socket works via control thread.
            print("done")
            msg = socket.recv_multipart()
            assert msg[-1] == b'{"name": "stdout", "text": "done"}'
        finally:
            socket.close(linger=0)

    n = 10
    socket = kernel._sockets[SocketID.iopub]
    url = socket.get_string(zmq.SocketOption.LAST_ENDPOINT)
    assert url.endswith(str(kernel._ports[SocketID.iopub]))
    ctx = zmq.Context()
    try:
        thread = threading.Thread(target=pubio_subscribe)
        thread.start()
        time.sleep(0.05)
        if mode == "proxy":
            socket = Caller.iopub_sockets[kernel.callers[SocketID.control].thread]
        for i in range(n):
            socket.send_multipart([b"0", f"{i}".encode()])
        thread.join()
    finally:
        ctx.term()


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
    client.input("Some input that should be discardes")
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


async def test_unraisablehook(kernel: Kernel, mocker):
    handler = logging.Handler()
    kernel.log.logger.addHandler(handler)  # pyright: ignore[reportAttributeAccessIssue]

    class Unraiseable:
        def __init__(self) -> None:
            self.exc_type = BaseException
            self.exc_value = BaseException()
            self.exc_traceback = None
            self.err_msg = "my error message"
            self.object = ""

    emit = mocker.patch.object(handler, "emit")
    kernel.unraisablehook(Unraiseable())  # pyright: ignore[reportArgumentType]
    assert emit.call_count == 1
    kernel.log.logger.removeHandler(handler)  # pyright: ignore[reportAttributeAccessIssue]


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


async def test_execute_request_error_tag_ignore_error(client: AsyncKernelClient):
    await utils.clear_iopub(client)
    metadata = {"tags": [Tags.suppress_error]}
    await utils.execute(client, "stop - suppress me", metadata=metadata, clear_pub=False)
    stdout, _ = await utils.assemble_output(client)
    assert "⚠" in stdout


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
    comm = None

    def cb(comm_, _):
        nonlocal comm
        comm = comm_

    kernel.comm_manager.register_target("my target", cb)
    # open a comm
    with anyio.move_on_after(0.1):
        await utils.send_shell_message(
            client, MsgType.comm_open, {"content": {}, "comm_id": "comm id", "target_name": "my target"}
        )
    assert isinstance(comm, Comm)
    comm = cast("Comm", comm)
    reply = await utils.send_shell_message(client, MsgType.comm_info_request)
    assert reply["header"]["msg_type"] == "comm_info_reply"
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["comms"].get("comm id") == {"target_name": "my target"}

    msg_received = mocker.patch.object(comm, "handle_msg")
    with anyio.move_on_after(0.1):
        await utils.send_shell_message(client, MsgType.comm_msg, {"comm_id": comm.comm_id})
    assert msg_received.call_count == 1
    # close comm
    closed = mocker.patch.object(comm, "handle_close")
    with anyio.move_on_after(0.1):
        await utils.send_shell_message(client, MsgType.comm_close, {"comm_id": comm.comm_id})
    assert closed.call_count == 1
    kernel.comm_manager.unregister_target("my target", cb)


async def test_interrupt_request(client: AsyncKernelClient, kernel: Kernel):
    event = threading.Event()
    kernel.interrupts.add(event.set)
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
    msg_id = client.execute(f"import time;time.sleep({utils.TIMEOUT * 4})")
    await utils.check_pub_message(client, msg_id, execution_state="busy")
    await utils.check_pub_message(client, msg_id, msg_type="execute_input")
    await anyio.sleep(0.5)
    reply = await utils.send_control_message(client, MsgType.interrupt_request)
    with anyio.fail_after(utils.TIMEOUT):
        reply = await utils.get_reply(client, msg_id)
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "KernelInterruptError"


async def test_interrupt_request_direct_task(subprocess_kernels_client: AsyncKernelClient):
    await utils.clear_iopub(subprocess_kernels_client)
    code = f"""
    import time
    from async_kernel import Caller
    await Caller().call_soon(time.sleep, {utils.TIMEOUT * 2})
    """
    client = subprocess_kernels_client
    msg_id = client.execute(code)
    await utils.check_pub_message(client, msg_id, execution_state="busy")
    await utils.check_pub_message(client, msg_id, msg_type="execute_input")
    await anyio.sleep(0.5)
    await utils.send_control_message(client, MsgType.interrupt_request)
    reply = await utils.get_reply(client, msg_id)
    assert reply["content"]["status"] == "error"
    assert reply["content"]["ename"] == "KernelInterruptError"


@pytest.mark.parametrize("response", ["y", ""])
async def test_user_exit(client: AsyncKernelClient, kernel: Kernel, mocker, response: Literal["y", ""]):
    stop = mocker.patch.object(kernel, "stop")
    raw_input = mocker.patch.object(kernel, "raw_input", return_value=response)
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
    with pytest.raises(RuntimeError):
        await kernel.debugger.debugpy_client.send_request({})


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


@pytest.mark.parametrize("code", argvalues=["%connect_info", "%callers", "%subshell"])
async def test_magic(client: AsyncKernelClient, code: str, kernel: Kernel, monkeypatch):
    await utils.clear_iopub(client)
    monkeypatch.setenv("JUPYTER_RUNTIME_DIR", str(pathlib.Path(kernel.connection_file).parent))
    assert code
    _, reply = await utils.execute(client, code, clear_pub=False)
    assert reply["status"] == "ok"
    stdout, _ = await utils.assemble_output(client)
    assert stdout


async def test_shell_required_properites(kernel: Kernel):
    # used by ipython AutoMagicChecker via is_shadowed (requires 'builitin')
    assert set(kernel.shell.ns_table) == {"user_global", "user_local", "builtin"}
    # U
    kernel.shell.enable_gui()
    with pytest.raises(NotImplementedError):
        kernel.shell.enable_gui("tk")


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
    await anyio.sleep(0.02)


@pytest.mark.parametrize("channel", ["shell", "control"])
async def test_invalid_message(client: AsyncKernelClient, channel: Literal["shell", "control"]):
    f = utils.send_control_message if channel == "control" else utils.send_shell_message
    response = None
    with anyio.move_on_after(0.1):
        response = await f(client, "test_invalid_message")  # pyright: ignore[reportArgumentType]
    assert response is None


async def test_kernel_get_handler(kernel: Kernel):
    with pytest.raises(TypeError):
        kernel.get_handler("invalid mode")  # pyright: ignore[reportArgumentType]
    for msg_type in MsgType:
        handler = kernel.get_handler(msg_type)
        assert inspect.iscoroutinefunction(handler)
        sig = inspect.signature(handler)
        assert len(sig.parameters) == 1
        param = sig.parameters["job"]
        assert param.kind == param.POSITIONAL_ONLY


@pytest.mark.parametrize(
    ("code", "silent", "socket_id", "expected"),
    [
        (f"{RunMode.task}", False, SocketID.shell, RunMode.task),
        (f" {RunMode.task}", False, SocketID.shell, RunMode.task),
        ("print(1)", False, SocketID.shell, RunMode.queue),
        ("", True, SocketID.shell, RunMode.task),
        (f"{RunMode.thread}\nprint('hello')", False, SocketID.shell, RunMode.thread),
        ("", False, SocketID.control, RunMode.queue),
        ("threads", False, SocketID.shell, RunMode.queue),
        ("Task", False, SocketID.shell, RunMode.queue),
        ("RunMode.direct", False, SocketID.shell, RunMode.direct),
    ],
)
async def test_get_run_mode(
    kernel: Kernel, code: str, silent: bool, socket_id, expected: RunMode, job: Job[ExecuteContent]
):
    job["msg"]["content"]["code"] = code
    job["msg"]["content"]["silent"] = silent
    mode = kernel.get_run_mode(MsgType.execute_request, socket_id=socket_id, job=job)
    assert mode is expected


async def test_get_run_mode_tag(client: AsyncKernelClient):
    metadata = {"tags": [RunMode.thread]}
    _, content = await utils.execute(
        client,
        "import threading;thread_name=threading.current_thread().name",
        metadata=metadata,
        user_expressions={"thread_name": "thread_name"},
    )
    assert content["status"] == "ok"
    assert "async_kernel_caller" in content["user_expressions"]["thread_name"]["data"]["text/plain"]


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
        kernel.shell.stop_on_error_time_offset = utils.TIMEOUT
        _, content = await utils.execute(client, "fail", metadata={"tags": [Tags.raises_exception, value]})
        assert content["status"] == "error"
        _, content = await utils.execute(client, "a=10")
        assert content["status"] == expected
    finally:
        kernel.shell.stop_on_error_time_offset = 0
        kernel.shell._stop_on_error_info.clear()


async def test_all_concurrency_run_modes(kernel: Kernel):
    data = kernel.all_concurrency_run_modes()
    # Regen the hash as required
    assert murmur2_x86(str(data), 1) == 387578838


async def test_get_parent(client: AsyncKernelClient, kernel: Kernel):
    assert kernel.get_parent() is None
    code = "assert 'header' in get_ipython().kernel.get_parent()"
    await utils.execute(client, code)


async def test_subshell(client: AsyncKernelClient, kernel: Kernel):
    subshell_id = kernel.subshell_manager.create_subshell(protected=True).subshell_id
    subshell = kernel.subshell_manager.subshells[subshell_id]

    assert repr(kernel.main_shell) == "<AsyncInteractiveShell  kernel_name: 'async' subhsell_id: None>"
    assert repr(subshell) == f"<AsyncInteractiveSubshell kernel_name: 'async'  subshell_id: {subshell_id}>"

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
        assert await Caller().call_soon(lambda: async_kernel.utils.get_kernel().shell) is subshell
        kernel.shell.reset()
        assert pen.cancelled()

    # delete
    assert subshell_id in kernel.subshell_manager.subshells
    kernel.subshell_manager.delete_subshell(subshell_id)
    assert subshell_id in kernel.subshell_manager.subshells, "Protected should not stop when deleted"
    kernel.subshell_manager.stop_all_subshells(force=True)
    assert kernel.main_shell.user_ns["a"] == 1
    with (
        pytest.raises(RuntimeError, match="does not exist!"),
        async_kernel.utils.subshell_context(subshell.subshell_id),
    ):
        pass
