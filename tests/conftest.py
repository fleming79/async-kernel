import asyncio
import gc
import importlib.util
import os
import sys
import threading
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

import anyio
import pytest
from jupyter_client.asynchronous.client import AsyncKernelClient
from sniffio import current_async_library

import async_kernel.utils
from async_kernel import Caller
from async_kernel.interface.zmq import ZMQInterface
from async_kernel.kernel import Kernel
from async_kernel.kernelspec import make_argv
from async_kernel.typing import Backend, Channel, ExecuteContent, Job, Message, MsgHeader, MsgType
from tests import utils

if TYPE_CHECKING:
    import pathlib

    pytest_plugins = ["anyio.pytest_plugin"]


@pytest.hookimpl
def pytest_configure(config):
    os.environ["PYTEST_TIMEOUT"] = str(1e6) if async_kernel.utils.LAUNCHED_BY_DEBUGPY else str(utils.TIMEOUT)


if importlib.util.find_spec("winloop") or importlib.util.find_spec("uvloop"):
    params = [pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop")]
else:
    params = [pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio")]


def check_anyio_backend(anyio_backend):
    "Checks the running backend is loaded"
    assert current_async_library() == anyio_backend[0]
    if anyio_backend[0] == "asyncio":
        loop = asyncio.get_running_loop()
        if anyio_backend[1]["use_uvloop"]:
            if sys.platform == "win32":
                import winloop  # noqa: PLC0415

                assert isinstance(loop, winloop.Loop)
            else:
                import uvloop  # noqa: PLC0415

                assert isinstance(loop, uvloop.Loop)
        else:
            assert isinstance(loop, asyncio.BaseEventLoop)


@pytest.fixture(params=params, scope="module")
def anyio_backend(request):
    return request.param


@pytest.fixture(scope="module")
def transport():
    return "ipc" if sys.platform == "linux" else "tcp"


@pytest.fixture(scope="module", params=["MainThread", "ShellThread"])
async def kernel(anyio_backend, transport: str, request, tmp_path_factory):
    # Set a blank connection_file
    connection_file: pathlib.Path = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
    os.environ["IPYTHONDIR"] = str(tmp_path_factory.mktemp("ipython_config"))
    interface = ZMQInterface()
    interface.connection_file = connection_file
    interface.transport = transport
    try:
        if request.param == "MainThread":
            async with interface:
                await interface.kernel.caller.call_soon(check_anyio_backend, anyio_backend)
                assert os.environ["MPLBACKEND"] == utils.MATPLOTLIB_INLINE_BACKEND
                yield interface.kernel

        else:
            if anyio_backend[0] == "asyncio" and not anyio_backend[1]["use_uvloop"]:
                interface.backend_options = {}
            thread = threading.Thread(target=interface.start, name="ShellThread")
            thread.start()
            interface.event_started.wait()
            assert os.environ["MPLBACKEND"] == utils.MATPLOTLIB_INLINE_BACKEND
            await interface.kernel.caller.call_soon(check_anyio_backend, anyio_backend)
            try:
                yield interface.kernel
            finally:
                interface.stop()
                thread.join()
    finally:
        del interface
        for _ in range(3):
            gc.collect()


@pytest.fixture(scope="module")
async def client(kernel: Kernel) -> AsyncGenerator[AsyncKernelClient, Any]:
    assert isinstance(kernel.parent, ZMQInterface)
    if kernel.parent.backend is Backend.trio:
        pytest.skip("AsyncKernelClient needs asyncio")
    client = AsyncKernelClient()
    client.load_connection_info(kernel.parent.get_connection_info())
    client.start_channels()
    try:
        yield client
    finally:
        await utils.clear_iopub(client, timeout=0.1)
        client.stop_channels()
        await anyio.sleep(0)


@pytest.fixture(scope="module", params=["async", "async-trio"])
def name(request):
    return request.param


@pytest.fixture(scope="module")
async def subprocess_kernels_client(anyio_backend, tmp_path_factory, name, transport: str):
    """
    Starts a kernel in a subprocess and returns an AsyncKernelCient that is connected to it.
    """
    assert anyio_backend[0] == "asyncio", "Asyncio is required for the client"
    connection_file = tmp_path_factory.mktemp("async_kernel") / f"kernel-{os.getpid()}.json"
    backend = Backend.trio if "trio" in name else Backend.asyncio
    command = make_argv(connection_file=connection_file, name=name, transport=transport, backend=backend)
    process = await anyio.open_process([*command])
    async with process:
        while not connection_file.exists() or not connection_file.stat().st_size:
            await anyio.sleep(0.1)
        await anyio.sleep(0.01)
        client = AsyncKernelClient()
        client.load_connection_file(connection_file)
        client.start_channels()
        try:
            msg_id = client.kernel_info()
            await utils.get_reply(client, msg_id)
            await utils.clear_iopub(client, timeout=0.1)
            yield client
        finally:
            client.shutdown()
            client.stop_channels()
    for _ in range(2):
        if connection_file.exists():
            await anyio.sleep(1)
    assert not connection_file.exists(), "cleanup_connection_file not called by atexit ..."


@pytest.fixture
def job() -> Job[ExecuteContent]:
    "An execute dummy job"
    content = ExecuteContent(
        code="", silent=True, store_history=True, user_expressions={}, allow_stdin=False, stop_on_error=True
    )
    header = MsgHeader(msg_id="", session="", username="", date="", msg_type=MsgType.execute_request, version="1")
    msg = Message(header=header, parent_header=header, metadata={}, buffers=[], content=content, channel=Channel.shell)
    return Job(msg=msg, ident=[b""], received_time=0.0)


@pytest.fixture
async def caller(anyio_backend: Backend):
    async with Caller("manual") as caller:
        yield caller
