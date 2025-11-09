import os
import subprocess
import sys
import threading
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

import anyio
import pytest
from jupyter_client.asynchronous.client import AsyncKernelClient

import async_kernel.utils
from async_kernel.kernel import Kernel
from async_kernel.kernelspec import Backend, KernelName, make_argv
from async_kernel.typing import ExecuteContent, Job, Message, MsgHeader, MsgType, SocketID
from tests import utils

if TYPE_CHECKING:
    import pathlib

    pytest_plugins = ["anyio.pytest_plugin"]


@pytest.hookimpl
def pytest_configure(config):
    os.environ["PYTEST_TIMEOUT"] = str(1e6) if async_kernel.utils.LAUNCHED_BY_DEBUGPY else str(utils.TIMEOUT)


@pytest.fixture(scope="module")
def anyio_backend(request):
    return "asyncio"


@pytest.fixture(scope="module")
def transport():
    return "ipc" if sys.platform == "linux" else "tcp"


@pytest.fixture(scope="module", params=["MainThread", "ShellThread"])
async def kernel(anyio_backend, transport: str, request, tmp_path_factory):
    # Set a blank connection_file
    connection_file: pathlib.Path = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
    os.environ["IPYTHONDIR"] = str(tmp_path_factory.mktemp("ipython_config"))
    kernel = Kernel()
    kernel.connection_file = connection_file
    os.environ["MPLBACKEND"] = utils.MATPLOTLIB_INLINE_BACKEND  # Set this implicitly
    kernel.transport = transport
    if request.param == "MainThread":
        async with kernel:
            yield kernel
    else:
        thread = threading.Thread(target=kernel.run, name="ShellThread")
        thread.start()
        kernel.event_started.wait()
        try:
            yield kernel
        finally:
            kernel.stop()
            thread.join()


@pytest.fixture(scope="module")
async def client(kernel: Kernel) -> AsyncGenerator[AsyncKernelClient, Any]:
    if kernel.anyio_backend is Backend.trio:
        pytest.skip("AsyncKernelClient needs asyncio")
    client = AsyncKernelClient()
    client.load_connection_info(kernel.get_connection_info())
    client.start_channels()
    try:
        yield client
    finally:
        await utils.clear_iopub(client)
        client.stop_channels()
        await anyio.sleep(0)


@pytest.fixture(scope="module", params=KernelName)
def kernel_name(request):
    return request.param


@pytest.fixture(scope="module")
async def subprocess_kernels_client(anyio_backend, tmp_path_factory, kernel_name: KernelName, transport):
    """
    Starts a kernel in a subprocess and returns an AsyncKernelCient that is connected to it.

    This is primarily provided for testing the debugger making it more convenient
    connect a debugger to the test.
    """
    assert anyio_backend == "asyncio", "Asyncio is required for the client"
    connection_file = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
    command = make_argv(connection_file=connection_file, kernel_name=kernel_name, transport=transport)
    process = subprocess.Popen(command)
    try:
        client = AsyncKernelClient()
        while not connection_file.exists() or not connection_file.stat().st_size:
            await anyio.sleep(0.1)
        await anyio.sleep(0.01)
        client.load_connection_file(connection_file)
        client.start_channels()
        msg_id = client.kernel_info()
        await utils.get_reply(client, msg_id)
        await utils.clear_iopub(client, timeout=0.1)
        try:
            yield client
        finally:
            client.shutdown()
            client.stop_channels()
            assert process.wait(utils.TIMEOUT) == 0
    finally:
        if process.returncode is None:
            process.kill()

    assert not connection_file.exists(), "cleanup_connection_file not called by atexit ..."


@pytest.fixture
def job() -> Job[ExecuteContent]:
    "An execute dummy job"
    content = ExecuteContent(
        code="", silent=True, store_history=True, user_expressions={}, allow_stdin=False, stop_on_error=True
    )
    header = MsgHeader(msg_id="", session="", username="", date="", msg_type=MsgType.execute_request, version="1")
    msg = Message(header=header, parent_header=header, metadata={}, buffers=[], content=content)
    return Job(msg=msg, socket_id=SocketID.shell, ident=[b""], socket=None, received_time=0.0)  # pyright: ignore[ reportArgumentType]
