from __future__ import annotations

import gc
import importlib.util
import json
import signal
import sys
import weakref
from typing import TYPE_CHECKING, Literal

import anyio
import pytest
from aiologic import Event
from typing_extensions import override

import async_kernel
from async_kernel.command import args_to_dict, command_line
from async_kernel.interface.zmq import ZMQInterface
from async_kernel.kernelspec import make_argv
from async_kernel.typing import Backend, Hosts
from tests import utils

if TYPE_CHECKING:
    import pathlib

    from jupyter_client.asynchronous.client import AsyncKernelClient

    from async_kernel.interface.base import BaseInterface
    from async_kernel.kernel import Kernel
    from async_kernel.shell import IPShell


@pytest.fixture(scope="module", params=["tcp", "ipc"] if sys.platform == "linux" else ["tcp"])
def transport(request):
    return request.param


@pytest.fixture
def fake_kernel_dir(tmp_path, monkeypatch):
    kernel_dir = tmp_path / "share/jupyter/kernels"
    kernel_dir.mkdir(parents=True)
    monkeypatch.setattr(sys, "prefix", str(tmp_path))
    return kernel_dir


def test_args_to_dict():
    unknown_args = [
        "--display_name='my kernel'",
        "--dict_value",
        "option_A=False",
        "--dict_value",
        "Some other value='142'",
        "--start_interface=start_kernel_zmq_interface",
        "--shell.timeout=0.01",
        "--shell.timeout",
        "2",
        "-quiet",
        "--automagic",
        "--no-automagic",  # not a flag
    ]
    settings = args_to_dict(unknown_args)
    assert settings == {
        "display_name": "my kernel",
        "dict_value": {"option_A": False, "Some other value": "142"},
        "start_interface": "start_kernel_zmq_interface",
        "shell.timeout": 2,
        "quiet": True,
        "automagic": False,
    }
    with pytest.raises(ValueError, match="Invalid arg detected"):
        args_to_dict(["no-prefix"])


def test_prints_help_when_no_args(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["prog"])
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0
    out = capsys.readouterr().out
    assert "usage:" in out


def test_prints_version_info(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["prog", "-V"])
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0
    out = capsys.readouterr().out
    assert f"async-kernel {async_kernel.__version__}" in out


def test_prints_help_all(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["prog", "--help-all"])
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0
    out = capsys.readouterr().out
    assert "aliases" in out


def test_add_kernel_start_zmq_app(monkeypatch, fake_kernel_dir: pathlib.Path, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "-a", "async-trio", "--display_name='my kernel'", "--BaseShell.timeout=0.01"],
    )
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0
    out = capsys.readouterr().out
    assert "Added kernel spec" in out
    kernel_dir = fake_kernel_dir.joinpath("async-trio")
    assert (kernel_dir).exists()
    spec = json.loads(kernel_dir.joinpath("kernel.json").read_bytes())
    assert spec == {
        "argv": [
            sys.executable,
            "-m",
            "async_kernel",
            "start",
            "--connection_file={connection_file}",
            "--start_interface=launch_zmq_kernel",
            "--name=async-trio",
            "--BaseShell.timeout=0.01",
        ],
        "env": {},
        "display_name": "my kernel",
        "language": "python",
        "interrupt_mode": "message",
        "metadata": {"debugger": True, "concurrent": True},
        "kernel_protocol_version": "5.5",
    }


def test_no_args(monkeypatch, fake_kernel_dir: pathlib.Path, capsys):
    monkeypatch.setattr(sys, "argv", ["prog"])
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0
    out = capsys.readouterr().out
    assert out.startswith("usage: async-kernel")


@pytest.mark.parametrize("mode", ["folder", "prefix", "default"])
def test_remove_existing_kernel(monkeypatch, fake_kernel_dir, capsys, mode: Literal["folder", "prefix", "default"]):
    name = "asyncio"
    (fake_kernel_dir / name).mkdir()
    if mode == "folder":
        monkeypatch.setattr(sys, "argv", ["prog", "-r", name, f"--folder={fake_kernel_dir}"])
    elif mode == "prefix":
        monkeypatch.setattr(sys, "argv", ["prog", "-r", name, f"--prefix={sys.prefix}"])
    else:
        monkeypatch.setattr(sys, "argv", ["prog", "-r", name])
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0
    out = capsys.readouterr().out
    assert "removed" in out
    assert not (fake_kernel_dir / name).exists()


def test_remove_nonexistent_kernel(monkeypatch, fake_kernel_dir, capsys):
    name = "not a kernel"
    monkeypatch.setattr(sys, "argv", ["prog", "-r", name])
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0
    out = capsys.readouterr().out
    assert "not found!" in out


def test_command_start_zmq_app(monkeypatch, fake_kernel_dir: pathlib.Path):
    class EventSet(Event):
        @override
        def set(self):
            super().set()
            kernel: Kernel[BaseInterface[IPShell], IPShell] = async_kernel.utils.get_kernel()  # pyright: ignore[reportAssignmentType]
            assert isinstance(kernel.parent, ZMQInterface)
            assert kernel.parent.backend_options == {"use_uv": False}
            assert kernel.shell.timeout == 0.123
            assert kernel.shell.automagic is False
            kernel.parent.event_stopped.set()

    cmd = [
        "prog",
        "start",
        f"--connection_file={fake_kernel_dir.joinpath('test_start_kernel_zmq_interface.json')}",
        "--backend_options",
        "use_uv=False",
        "--BaseShell.timeout=0.123",
        "--no-automagic",
        "--start_interface=launch_zmq_kernel",
    ]
    event_started = EventSet()
    monkeypatch.setattr(ZMQInterface, "event_started", event_started)
    monkeypatch.setattr(sys, "argv", cmd)
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0


def test_start_kernel_zmq_interface(mocker, monkeypatch, fake_kernel_dir: pathlib.Path):

    class EventSet(Event):
        @override
        def set(self):
            kernel = async_kernel.utils.get_kernel()
            assert isinstance(kernel.parent, ZMQInterface)
            assert kernel.parent.backend_options == {"use_uv": False}
            assert kernel.main_shell.timeout == 2.0
            assert kernel.parent.quiet is False
            super().set()
            kernel.parent.event_stopped.set()

    cmd = [
        "prog",
        "start",
        f"--connection_file={fake_kernel_dir.joinpath('test_start_kernel_zmq_interface.json')}",
        "--display_name='my kernel'",
        "--backend_options",
        "use_uv=False",
        "--start_interface=start_kernel_zmq_interface",
        "--kernel.main_shell.timeout",
        "2",
        "-no-quiet",
    ]
    event_started = EventSet()
    monkeypatch.setattr(ZMQInterface, "event_started", event_started)
    monkeypatch.setattr(sys, "argv", cmd)
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0


# Avoid matplotlib tests generally to avoid flaky tests on ci.
@pytest.mark.skipif(not importlib.util.find_spec("matplotlib"), reason="Requires matplotlib")
@pytest.mark.parametrize("backend", Backend)
@pytest.mark.parametrize("host", [Hosts.tk, Hosts.qt, None])
def test_command_start_kernel_enable_matplotlib(mocker, monkeypatch, backend, host):
    if host is Hosts.tk:
        if not importlib.util.find_spec("_tkinter"):
            pytest.skip("_tkinter not installed")
    elif host is Hosts.qt and not importlib.util.find_spec("PySide6"):
        pytest.skip("PySide6 not installed")

    cmd = ["prog", "start", f"--name=async-{host}", f"--host={host}", f"--backend={backend}"]
    monkeypatch.setattr(sys, "argv", cmd)

    class EventSet(Event):
        @override
        def set(self):
            kernel = async_kernel.utils.get_kernel()
            assert kernel.parent.host == host
            assert kernel.parent.backend == backend
            super().set()
            kernel.parent.event_stopped.set()

    event_started = EventSet()

    monkeypatch.setattr(ZMQInterface, "event_started", event_started)

    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0


async def test_subprocess_kernels_client(subprocess_kernels_client: AsyncKernelClient, name, transport):
    # Start & Stop a kernel
    backend = Backend.trio if "trio" in name.lower() else Backend.asyncio
    _, reply = await utils.execute(
        subprocess_kernels_client,
        "interface = get_ipython().parent",
        user_expressions={
            "name": "interface.name",
            "backend": "interface.backend",
            "transport": "interface.transport",
        },
    )
    assert name in reply["user_expressions"]["name"]["data"]["text/plain"]
    assert backend in reply["user_expressions"]["backend"]["data"]["text/plain"]
    assert transport in reply["user_expressions"]["transport"]["data"]["text/plain"]


@pytest.mark.skipif(sys.platform == "win32", reason="Can't simulate keyboard interrupt on windows.")
async def test_subprocess_kernel_keyboard_interrupt(tmp_path, anyio_backend):
    # This is the keyboard interrupt from a console app, not to be confused with 'interrupt_request'.
    connection_file = tmp_path / "connection_file.json"
    command = make_argv(connection_file=connection_file)
    process = await anyio.open_process(command)
    async with process:
        while not connection_file.exists():
            await anyio.sleep(0.1)
        await anyio.sleep(0.1)
        # Simulate a keyboard interrupt from the console.
        process.send_signal(signal.SIGINT)
    assert process.returncode == 0


async def test_ZMQInterface_gc(anyio_backend: Backend):
    collected = Event()
    async with ZMQInterface() as interface:
        weakref.finalize(interface, collected.set)
        ref = weakref.ref(interface)
        del interface

    with anyio.move_on_after(2):
        while not collected:
            gc.collect()
            await anyio.sleep(0)
    if obj := ref():
        referrers = gc.get_referrers(obj)
        assert not referrers
