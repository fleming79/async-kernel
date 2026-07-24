import logging
import os
import subprocess
import sys
import threading
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

import anyio
import pytest
import traitlets.config
import zmq
from aiologic.lowlevel import current_async_library

# from async_kernel.client.zmq import AsyncKernelClient
import async_kernel
from async_kernel import Caller
from async_kernel.client.zmq import ZMQKernelClient
from async_kernel.interface.zmq import ZMQInterface
from async_kernel.kernel import Kernel
from async_kernel.kernelspec import make_argv
from async_kernel.typing import Backend, Channel, ExecuteContent, Job, Message, MsgHeader, MsgType
from tests import utils

if TYPE_CHECKING:
    import pathlib


assert "IPython" not in sys.modules

from async_kernel.interface.ip_app import IPApp  # noqa: E402

debug = False

if async_kernel.utils.LAUNCHED_BY_DEBUGPY:
    debug = True
    logging.basicConfig(level=10)


@pytest.hookimpl
def pytest_configure(config):
    global debug  # noqa: PLW0603
    if config.getini("log_cli_level") == "DEBUG":
        debug = True
    if debug:
        traitlets.config.Application.log_level.default_value = 10  # pyright: ignore[reportAttributeAccessIssue]
        logging.basicConfig(level=10)


def check_anyio_backend(anyio_backend):
    """Checks the running backend is loaded."""
    assert current_async_library() == anyio_backend


@pytest.fixture(params=Backend, scope="module")
def anyio_backend(request):
    return request.param


@pytest.fixture(scope="module")
def transport():
    return "ipc" if sys.platform == "linux" else "tcp"


@pytest.fixture(scope="module", params=["MainThread", "ShellThread"])
async def kernel(anyio_backend: Backend, transport: str, request, tmp_path_factory):
    # Set a blank connection_file
    connection_file: pathlib.Path = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
    os.environ["IPYTHONDIR"] = str(tmp_path_factory.mktemp("ipython_config"))

    # We test both `IPApp` and `ZMQInterface` but doesn't warrant separate tests
    interface_class = IPApp if anyio_backend[0] == "asyncio" else ZMQInterface
    interface = (interface_class)(
        connection_file=connection_file.as_posix(),
        transport=transport,
        backend=anyio_backend,
    )
    if debug:
        interface.log_level = 10
    if request.param == "MainThread":
        async with interface:
            await interface.kernel.caller.call_soon(check_anyio_backend, anyio_backend)
            assert os.environ["MPLBACKEND"] == utils.MATPLOTLIB_INLINE_BACKEND
            yield interface.kernel

    else:
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


@pytest.fixture(scope="module")
async def client(kernel: Kernel, anyio_backend: Backend) -> AsyncGenerator[ZMQKernelClient, Any]:
    assert isinstance(kernel.parent, ZMQInterface)
    client = ZMQKernelClient()
    client.load_connection_info(kernel.parent.get_connection_info())
    async with client:
        yield client


@pytest.fixture(scope="module")
def encryption(request):
    return ""


@pytest.fixture(scope="module")
async def subprocess_kernels_client(anyio_backend, tmp_path_factory, transport: str, encryption: str):
    """Starts a kernel in a subprocess and returns an AsyncKernelCient that is connected to it."""
    check_anyio_backend(anyio_backend)
    tmpdir: pathlib.Path = tmp_path_factory.mktemp("async_kernel")
    os.chdir(tmpdir)

    curve_publickey, curve_secretkey = zmq.curve_keypair() if encryption == "curve" else (None, None)
    name = f"async-{anyio_backend}"

    # Start the client
    client = ZMQKernelClient(
        connection_file=str(tmpdir.joinpath(f"kernel-{os.getpid()}.json")),
        curve_publickey=curve_publickey,
        curve_secretkey=curve_secretkey,
        transport=transport,
        kernel_name=name,
    )
    client.write_connection_file()

    # Start the interface
    command = make_argv(connection_file=client.connection_file, name=name, backend=anyio_backend)
    if debug:
        command.append("--debug")
    process = subprocess.Popen(command)
    try:
        async with client:
            try:
                yield client
            finally:
                await client.shutdown()
        assert process.wait() == 0
    finally:
        process.terminate()


@pytest.fixture
def job() -> Job[ExecuteContent]:
    """An execute dummy job."""
    content = ExecuteContent(
        code="", silent=True, store_history=True, user_expressions={}, allow_stdin=False, stop_on_error=True
    )
    header = MsgHeader(msg_id="", session="", username="", date="", msg_type=MsgType.execute_request, version="1")
    msg = Message(header=header, parent_header=header, metadata={}, buffers=[], content=content, channel=Channel.shell)
    return Job(msg=msg, ident=[b""], received_time=0.0)


@pytest.fixture
async def caller(anyio_backend: Backend):
    async with Caller() as caller:
        yield caller
