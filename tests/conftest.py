import logging
import os
import sys
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Literal

import pytest

import async_kernel
from async_kernel import Caller, Kernel
from async_kernel.connection.base import LocalClient
from async_kernel.connection.zmq import ZMQConnection, ZMQKernelClient
from async_kernel.interface import Interface
from async_kernel.typing import Backend, Channel, ExecuteContent, Job, Message, MessageProtocol, MsgHeader, MsgType
from tests import utils

if TYPE_CHECKING:
    import pathlib


assert "IPython" not in sys.modules


if async_kernel.utils.LAUNCHED_BY_DEBUGPY:
    async_kernel.utils.PYTEST_LOG_CLI_DEBUG = True
    os.environ.setdefault("PYTEST_LOG_CLI_DEBUG", "1")
    logging.basicConfig(level=10)


@pytest.hookimpl
def pytest_configure(config):

    if config.getini("log_cli_level") == "DEBUG":
        async_kernel.utils.PYTEST_LOG_CLI_DEBUG = True
        os.environ.setdefault("PYTEST_LOG_CLI_DEBUG", "1")
        logging.basicConfig(level=10)


# anyio_backends = [("asyncio", {"use_uvloop": False}), ("trio", {})]
# if importlib.util.find_spec("winloop") or importlib.util.find_spec("uvloop"):
#     anyio_backends.append(("asyncio", {"use_uvloop": True}))


@pytest.fixture(params=[Backend.asyncio, Backend.trio], scope="module")
def anyio_backend(request):
    return request.param


@pytest.fixture(params=["local", "zmq"], scope="module")
def connection_name(request):
    return request.param


@pytest.fixture(scope="module")
async def kernel(anyio_backend: Backend, connection_name: Literal["local", "zmq"], tmp_path_factory):
    os.environ["IPYTHONDIR"] = str(tmp_path_factory.mktemp("ipython_config"))
    if connection_name == "zmq":
        connection_file: pathlib.Path = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
        async with Interface([f"--connection_file={connection_file}"]) as interface:
            assert interface.connections
            assert isinstance(interface.connections[0], ZMQConnection)
            yield interface.kernel
    else:
        async with Interface(autostart_connections=[]) as interface:
            yield interface.kernel


@pytest.fixture(scope="module")
async def client(
    kernel: Kernel, connection_name: Literal["local", "zmq"]
) -> AsyncGenerator[LocalClient | ZMQKernelClient]:
    if connection_name == "zmq":
        connection = kernel.parent.connections[0]
        assert isinstance(connection, ZMQConnection)
        client = ZMQKernelClient()
        client.load_connection_info(connection.get_connection_info())
        async with client:
            yield client
    else:
        async with LocalClient() as client:
            yield client


@pytest.fixture(scope="module")
async def subprocess_kernel_client(anyio_backend: Backend):
    # Launching the subprocess from a fixture enables coverage to be patched correctly by pytest coverage.

    started = False
    while not started:
        # On occasion the client fails to connect for no obvious reason.
        client = ZMQKernelClient(encryption="curve")
        try:
            async with client.subprocess_kernel(startup_delay=0.5, start_timeout=utils.TIMEOUT, backend=anyio_backend):
                started = True
                yield client
        except TimeoutError:
            client.log.warning("Failed to start subprocess client. Trying to start a new client...")


@pytest.fixture
def job() -> Job:
    """An execute dummy job."""
    content = ExecuteContent(
        code="", silent=True, store_history=True, user_expressions={}, allow_stdin=False, stop_on_error=True
    )
    header = MsgHeader(msg_id="", session="", username="", date="", msg_type=MsgType.execute_request, version="1")
    msg = Message(header=header, parent_header=header, metadata={}, buffers=[], content=content, channel=Channel.shell)
    return Job(msg=msg, ident=[b""], received_time=0.0, owner=MessageProtocol)


@pytest.fixture
async def caller(anyio_backend: Backend):
    async with Caller() as caller:
        yield caller
