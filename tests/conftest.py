import asyncio
import gc
import importlib.util
import logging
import os
import subprocess
import sys
import threading
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

import anyio
import pytest
import zmq
from aiologic.lowlevel import current_async_library
from jupyter_client.asynchronous.client import AsyncKernelClient

import async_kernel
from async_kernel import Caller
from async_kernel.interface.zmq import ZMQInterface
from async_kernel.kernel import Kernel
from async_kernel.kernelspec import make_argv
from async_kernel.typing import Backend, Channel, ExecuteContent, Job, Message, MsgHeader, MsgType
from tests import utils

if TYPE_CHECKING:
    import pathlib


assert "IPython" not in sys.modules

from async_kernel.interface.ip_app import IPApp  # noqa: E402

if importlib.util.find_spec("winloop") or importlib.util.find_spec("uvloop"):
    params = [pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop")]
else:
    params = [pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio")]


if utils.CI_DEBUGGING or async_kernel.utils.LAUNCHED_BY_DEBUGPY:
    logging.basicConfig(level=10)


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

    # We test both `IPApp` and `ZMQInterface` but doesn't warrant separate tests
    interface_class = IPApp if anyio_backend[0] == "asyncio" else ZMQInterface
    interface = (interface_class)(connection_file=connection_file.as_posix(), transport=transport)
    if utils.CI_DEBUGGING or async_kernel.utils.LAUNCHED_BY_DEBUGPY:
        interface.log_level = 10
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
            await interface.started
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
        with anyio.fail_after(utils.TIMEOUT):
            await client.wait_for_ready()
        yield client
    finally:
        client.stop_channels()


@pytest.fixture(scope="module", params=["async", "async-trio"])
def name(request):
    return request.param


@pytest.fixture(scope="module")
def encryption(request):
    return ""


@pytest.fixture(scope="module")
async def subprocess_kernels_client(anyio_backend, tmp_path_factory, name: str, transport: str, encryption: str):
    """
    Starts a kernel in a subprocess and returns an AsyncKernelCient that is connected to it.
    """
    assert anyio_backend[0] == "asyncio", "Asyncio is required for the client"

    tmpdir: pathlib.Path = tmp_path_factory.mktemp("async_kernel")
    os.chdir(tmpdir)

    backend = Backend.trio if "trio" in name else Backend.asyncio
    curve_publickey, curve_secretkey = zmq.curve_keypair() if encryption == "curve" else (None, None)

    # Start the client
    client = AsyncKernelClient(
        connection_file=str(tmpdir.joinpath(f"kernel-{os.getpid()}.json")),
        curve_publickey=curve_publickey,
        curve_secretkey=curve_secretkey,
        transport=transport,
        kernel_name=name,
    )
    client.write_connection_file()
    client.start_channels()

    # Start the interface
    command = make_argv(connection_file=client.connection_file, name=name, backend=backend)
    if utils.CI_DEBUGGING or async_kernel.utils.LAUNCHED_BY_DEBUGPY:
        command.append("--debug")
    process = subprocess.Popen(command)
    try:
        with anyio.fail_after(utils.TIMEOUT):
            await client.wait_for_ready()

        yield client
        await utils.get_reply(client, client.shutdown(), channel=Channel.control)
        assert process.wait() == 0
    finally:
        process.terminate()
        client.stop_channels()


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
