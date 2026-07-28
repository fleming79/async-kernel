import logging
import os
import sys
from typing import TYPE_CHECKING

import pytest

# from async_kernel.client.zmq import AsyncKernelClient
import async_kernel
from async_kernel import Caller, Kernel
from async_kernel.client.base import BaseKernelClient
from async_kernel.typing import Backend, Channel, ExecuteContent, Job, Message, MsgHeader, MsgType

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
        logging.basicConfig(level=10)


# anyio_backends = [("asyncio", {"use_uvloop": False}), ("trio", {})]
# if importlib.util.find_spec("winloop") or importlib.util.find_spec("uvloop"):
#     anyio_backends.append(("asyncio", {"use_uvloop": True}))


@pytest.fixture(params=[Backend.asyncio, Backend.trio], scope="module")
def anyio_backend(request):
    return request.param


@pytest.fixture(scope="module")
async def kernel(anyio_backend: Backend, tmp_path_factory):
    # Set a blank connection_file
    connection_file: pathlib.Path = tmp_path_factory.mktemp("async_kernel") / "temp_connection.json"
    os.environ["IPYTHONDIR"] = str(tmp_path_factory.mktemp("ipython_config"))

    # We test both `IPApp` and `ZMQInterface` but doesn't warrant separate tests
    interface = IPApp(
        connection_file=connection_file.as_posix(),
        transport="ipc" if sys.platform == "linux" else "tcp",
        backend=anyio_backend,
    )
    async with interface:
        yield interface.kernel


@pytest.fixture(scope="module")
async def client(kernel: Kernel) -> BaseKernelClient:
    return kernel.parent.client


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
