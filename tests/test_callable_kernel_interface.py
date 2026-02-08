from __future__ import annotations

import signal

import anyio
import orjson
import pytest
from aiologic import Event
from IPython.core.error import StdinNotImplementedError

import async_kernel
from async_kernel.asyncshell import KernelInterruptError
from async_kernel.interface import start_kernel_callable_interface
from async_kernel.interface.callable import CallableKernelInterface


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def interface(anyio_backend):
    # These are the functions that should be provided externally
    stopped = Event()

    def send(msg_string, buffers, requires_reply, /):
        assert isinstance(msg_string, str)
        if requires_reply:
            parent = orjson.loads(msg_string)
            msg = interface.msg("input_reply", parent=parent, content={"value": "reply"})
            return orjson.dumps(msg, option=CallableKernelInterface.ORJSON_OPTION).decode()
        return None

    callbacks = await start_kernel_callable_interface(send=send, stopped=stopped.set)
    interface = async_kernel.Kernel().interface
    assert isinstance(interface, CallableKernelInterface)
    try:
        yield interface
    finally:
        callbacks["stop"]()
        await stopped


class TestCallableInterface:
    async def test_start(self, interface: CallableKernelInterface):
        assert interface.kernel.event_started

    async def test_msg(self, interface: CallableKernelInterface, mocker):
        sender = mocker.patch.object(interface, "_send")
        code = "import async_kernel\nassert async_kernel.utils.get_job()['msg']['buffers'] == [b'123']"
        msg = interface.msg("execute_request", content={"code": code})
        msg["header"]["session"] = "test session"
        buffers = [b"123"]
        async with interface.kernel.caller.create_pending_group() as pg:
            interface._handle_msg(orjson.dumps(msg).decode(), buffers)  # pyright: ignore[reportPrivateUsage]
            assert len(pg.pending) == 1

        assert sender.call_count == 4
        reply = orjson.loads(sender.call_args_list[2][0][0])
        assert reply["header"]["msg_type"] == "execute_reply"
        assert reply["content"]["status"] == "ok"

    async def test_kernel_info(self, interface: CallableKernelInterface, mocker):
        sender = mocker.patch.object(interface, "_send")
        msg = interface.msg("kernel_info_request")
        msg["header"]["session"] = "test session"
        interface._handle_msg(orjson.dumps(msg).decode())  # pyright: ignore[reportPrivateUsage]
        while sender.call_count != 3:
            await anyio.sleep(0.1)
        reply = orjson.loads(sender.call_args_list[1][0][0])
        assert reply["header"]["msg_type"] == "kernel_info_reply"
        assert reply["content"]["status"] == "ok"

    async def test_input(self, interface: CallableKernelInterface, job):
        token = async_kernel.utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        try:
            with pytest.raises(StdinNotImplementedError):
                interface.input_request("test")
            job["msg"]["content"]["allow_stdin"] = True
            assert interface.input_request("test") == "reply"
        finally:
            async_kernel.utils._job_var.reset(token)  # pyright: ignore[reportPrivateUsage]

    async def test_prevent_multiple_instances(self, interface):
        with pytest.raises(RuntimeError):
            CallableKernelInterface()

    async def test_keyboard_interrupt(self, interface):
        with pytest.raises(KernelInterruptError):
            signal.raise_signal(signal.SIGINT)
