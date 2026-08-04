from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pytest

from async_kernel.typing import Channel, MsgType
from tests import utils

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from async_kernel.client.zmq import ZMQKernelClient
    from async_kernel.interface.zmq import ZMQInterface
    from async_kernel.kernel import Kernel
    from async_kernel.shell import IPShell

    ClientType = ZMQKernelClient[ZMQInterface[IPShell]]
# pyright: reportGeneralTypeIssues=false, reportOptionalMemberAccess=false


async def read_until_msg_type(reader: AsyncGenerator, msg_type: MsgType, **content_checks):
    while (msg := await anext(reader))["header"]["msg_type"] != msg_type:
        continue
    utils.check_pub_message(msg, msg_type=msg_type, **content_checks)
    return msg


async def test_execute(client: ClientType, kernel: Kernel):
    reply = await client.execute(code="x=1")
    utils.validate_message(reply, MsgType.execute_reply)
    assert reply["content"]["status"] == "ok"
    assert kernel.shell.user_ns["x"] == 1


@pytest.mark.parametrize("mode", ["normal", "suppress"])
async def test_execute_suppress(client: ClientType, kernel: Kernel, mode: Literal["normal", "suppress"]):

    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute("123" if mode == "normal" else "123;")
        await read_until_msg_type(reader, MsgType.iopub_execute_input)
        if mode == "normal":
            data = {"text/plain": "123"}
            await read_until_msg_type(reader, msg_type=MsgType.iopub_execute_result, data=data)


async def test_execute_control(client: ClientType, kernel: Kernel) -> None:
    await client.execute("y=10", channel=Channel.control)
    assert kernel.shell.user_ns["y"] == 10


async def test_execute_silent(client: ClientType, kernel: Kernel):
    before = kernel.shell.execution_count
    reply = await client.execute("x=1", silent=True)
    count = int(reply["content"]["execution_count"])
    assert count == before


async def test_execute_error(client: ClientType):

    async with client.iopub_subscribe() as queue:
        reply = await client.execute("1/0")
        assert reply["content"]["status"] == "error"
        assert reply["content"].get("ename") == "ZeroDivisionError"
        reader = aiter(queue)
        while (msg := await anext(reader))["header"]["msg_type"] != MsgType.iopub_error:
            continue
        utils.check_pub_message(msg, msg_type=MsgType.iopub_error)


async def test_execute_inc(client: ClientType):
    """Execute request should increment execution_count."""
    reply = await client.execute("x=1")
    count = reply["content"].get("execution_count")
    assert isinstance(count, int)

    reply = await client.execute("x=2")
    count_2 = reply["content"].get("execution_count")
    assert count_2 == count + 1


async def test_execute_stop_on_error(client: ClientType):
    """Execute request should not abort execution queue with stop_on_error False."""
    bad_code = "\n".join(
        [
            # sleep to ensure subsequent message is waiting in the queue to be aborted
            # async sleep to ensure coroutines are processing while this happens
            "import anyio",
            "await anyio.sleep(0.1)",
            "raise ValueError()",
        ]
    )

    pen_bad_code = client.execute(bad_code)
    pen_1 = client.execute('print("Hello")')
    pen_2 = client.execute('print("world")')
    reply = await pen_bad_code
    assert reply["content"]["status"] == "error"
    assert reply["content"].get("traceback")

    assert (await pen_1)["content"]["status"] == "error"
    assert (await pen_2)["content"]["status"] == "error"

    #  Test stop_on_error=False
    pen_3 = client.execute(bad_code, stop_on_error=False)
    pen_4 = client.execute('print("Hello")')

    assert (await pen_3)["content"]["status"] == "error"
    assert (await pen_4)["content"]["status"] == "ok"


async def test_execute_stop_on_error_task(client: ClientType):
    """Execute request should not abort execution queue with stop_on_error False."""
    bad_code = "\n".join(
        [
            # sleep to ensure subsequent message is waiting in the queue to be aborted
            # async sleep to ensure coroutines are processing while this happens
            "import anyio",
            "await anyio.sleep(0.1)",
            "raise ValueError()",
        ]
    )
    pen_1 = client.execute("# task\nimport anyio\nawait anyio.sleep_forever()")
    assert (await client.execute(bad_code))["content"]["status"] == "error"
    reply = await pen_1
    assert "Stop on error cancellation" in "".join(reply["content"].get("traceback", ()))


async def test_user_expressions(client: ClientType):
    reply = await client.execute(code="x=1", user_expressions={"foo": "x+1"})
    user_expressions = reply["content"].get("user_expressions")
    assert user_expressions == {
        "foo": {
            "status": "ok",
            "data": {"text/plain": "2"},
            "metadata": {},
        }
    }


async def test_user_expressions_fail(client: ClientType):
    reply = await client.execute("x=0", user_expressions={"foo": "nosuchname"})
    user_expressions = reply["content"].get("user_expressions")
    assert user_expressions
    foo = user_expressions["foo"]
    assert foo["status"] == MsgType.iopub_error
    assert foo["ename"] == "NameError"


async def test_oinfo(client: ClientType):
    reply = await client.inspect("a")
    assert reply["content"] == {"data": {}, "metadata": {}, "found": False, "status": "ok"}
    utils.validate_message(reply, MsgType.inspect_reply)


async def test_oinfo_found(client: ClientType) -> None:
    reply = await client.execute("a=5")

    reply = await client.inspect("a")
    assert reply["header"]["msg_type"] == MsgType.inspect_reply
    content = reply["content"]
    assert content["found"]
    text = content["data"]["text/plain"]
    assert "Type:" in text
    assert "Docstring:" in text


async def test_oinfo_detail(client: ClientType):
    reply = await client.execute("ip=get_ipython()")

    reply = await client.inspect("ip.object_inspect", cursor_pos=10, detail_level=1)
    utils.validate_message(reply, MsgType.inspect_reply)
    content = reply["content"]
    assert content["found"]
    text = content["data"]["text/plain"]
    assert "Signature:" in text
    assert "Source:" in text


async def test_oinfo_not_found(client: ClientType):
    reply = await client.inspect("does_not_exist")

    utils.validate_message(reply, MsgType.inspect_reply)
    content = reply["content"]
    assert not content["found"]


async def test_complete(client: ClientType):
    await client.execute("alpha = albert = 5")
    reply = await client.complete("al", 2)
    utils.validate_message(reply, MsgType.complete_reply)
    matches = reply["content"]["matches"]
    for name in ("alpha", "albert"):
        assert name in matches
    reply = await client.complete("al", None)
    utils.validate_message(reply, MsgType.complete_reply)


async def test_kernel_info_request(client: ClientType):
    reply = await client.kernel_info()
    utils.validate_message(reply, MsgType.kernel_info_reply)
    keys = list(reply["content"])
    assert keys == [
        "protocol_version",
        "implementation",
        "implementation_version",
        "language_info",
        "banner",
        "help_links",
        "debugger",
        "supported_features",
        "status",
    ]


async def test_comm_info_request(client: ClientType):
    reply = await client.comm_info()

    utils.validate_message(reply, MsgType.comm_info_reply)


async def test_is_complete(client: ClientType):
    reply = await client.is_complete("a = 1")
    utils.validate_message(reply, MsgType.is_complete_reply)


async def test_history_range(client: ClientType):
    await client.execute("x=1", store_history=True)
    reply = await client.history(hist_access_type="range", raw=True, output=True, start=1, stop=2, session=0)

    utils.validate_message(reply, MsgType.history_reply)
    content = reply["content"]
    assert len(content["history"]) == 1


async def test_history_tail(client: ClientType):
    await client.execute("x=1", store_history=True)
    reply = await client.history(hist_access_type="tail", raw=True, output=True, n=1, session=0, include_latest=True)
    utils.validate_message(reply, MsgType.history_reply)
    content = reply["content"]
    assert len(content["history"]) == 1


async def test_history_search(client: ClientType):
    await client.execute("x=1", store_history=True)
    reply = await client.history(hist_access_type="search", raw=True, output=True, n=1, pattern="*", session=0)
    utils.validate_message(reply, MsgType.history_reply)
    content = reply["content"]
    assert len(content["history"]) == 1


async def test_stream(client: ClientType):
    async with client.iopub_subscribe() as queue:
        await client.execute("print('hi')")
        reader = aiter(queue)
        await read_until_msg_type(reader, MsgType.iopub_stream, text="hi\n")


@pytest.mark.parametrize("clear", [True, False])
async def test_displayhook(kernel: Kernel, client: ClientType, clear: bool):

    #  Test the displayhook is set builtin_mod.__dict__["display"] = display
    async with client.iopub_subscribe() as queue:
        await client.execute(f"display(1, clear={clear})")
        reader = aiter(queue)
        await read_until_msg_type(reader, MsgType.iopub_execute_input)
        if clear:
            await read_until_msg_type(reader, MsgType.iopub_clear_output)
        await read_until_msg_type(reader, MsgType.iopub_display_data, data={"text/plain": "1"})


async def test_rich_display_data(kernel: Kernel, client: ClientType):

    async with client.iopub_subscribe() as queue:
        reader = aiter(queue)
        await client.execute("1 + 1")
        await read_until_msg_type(reader, MsgType.iopub_execute_input)
        await read_until_msg_type(reader, msg_type=MsgType.iopub_execute_result, data={"text/plain": "2"})


async def test_subshell(kernel: Kernel, client: ClientType):
    # Create
    reply = await client.send_message(client.msg(MsgType.create_subshell_request, {}, channel=Channel.control))
    utils.validate_message(reply, MsgType.comm_info_reply.create_subshell_reply)
    assert reply["content"]["status"] == "ok"
    subshell_id = reply["content"]["subshell_id"]
    assert subshell_id in kernel.subshells
    subshell = kernel.get_shell(subshell_id)
    assert not subshell.protected

    # List
    reply = await client.send_message(client.msg(MsgType.list_subshell_request, {}, channel=Channel.control))
    utils.validate_message(reply, MsgType.list_subshell_reply)
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["subshell_id"] == [subshell_id]

    # Delete
    content = {"subshell_id": subshell_id}
    reply = await client.send_message(client.msg(MsgType.delete_subshell_request, content, channel=Channel.control))

    utils.validate_message(reply, MsgType.delete_subshell_reply)
    assert reply["content"]["status"] == "ok"
    assert subshell_id not in kernel.subshells
