import logging
import os
import sys
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Literal

import pytest

import async_kernel
from async_kernel import Caller, Kernel
from async_kernel.connection.base import LocalClient
from async_kernel.connection.zmq import ZMQClient, ZMQConnection
from async_kernel.interface import Interface
from async_kernel.typing import Backend, Channel, ExecuteContent, Job, Message, MessageProtocol, MsgHeader, MsgType

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
async def client(
    anyio_backend: Backend, connection_name: Literal["local", "zmq"], tmp_path_factory
) -> AsyncGenerator[LocalClient | ZMQClient]:

    os.environ["IPYTHONDIR"] = str(tmp_path_factory.mktemp("ipython_config"))
    if connection_name == "zmq":
        connection_file: pathlib.Path = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
        async with Interface([f"--connection_file={connection_file}"]).start() as interface:
            assert interface.connections
            connection = interface.connections[0]
            assert isinstance(connection, ZMQConnection)
            client = ZMQClient()
            client.load_connection_info(connection.get_connection_info())
            async with client.start():
                yield client
    else:
        async with Interface().start(), LocalClient().start() as client:
            yield client


@pytest.fixture(scope="module")
async def kernel(client: ZMQClient | LocalClient) -> Kernel:
    return async_kernel.utils.get_kernel()


@pytest.fixture(scope="module")
async def subprocess_kernel_client(anyio_backend: Backend):
    # Launching the subprocess from a fixture enables coverage to be patched correctly by pytest coverage.
    client = ZMQClient(encryption="curve")
    async with client.subprocess_kernel(heartbeat_interval=None, backend=anyio_backend):
        yield client


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
