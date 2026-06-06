from __future__ import annotations

import gc
import importlib.util
import json
import signal
import sys
import weakref
from typing import TYPE_CHECKING, Literal, cast

import anyio
import pytest
from aiologic import Event
from typing_extensions import override

import async_kernel
from async_kernel import Kernel
from async_kernel.command import command_line, to_flags_and_settings
from async_kernel.interface.ip_app import IPApp
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
        "--quiet",
        "--display_name='my kernel'",
        "--dict_value",
        "option_A=False",
        "--dict_value",
        "Some other value='142'",
        "--launcher=start_kernel_zmq_interface",
        "--timeout",
        "2",
        "--automagic",
        "--debug",
    ]
    flags, settings = to_flags_and_settings(unknown_args)
    assert flags == ["quiet", "automagic", "debug"]
    assert settings == {
        "display_name": "my kernel",
        "dict_value": {"option_A": False, "Some other value": "142"},
        "launcher": "start_kernel_zmq_interface",
        "timeout": 2,
    }
    with pytest.raises(ValueError, match="Invalid arg detected"):
        to_flags_and_settings(["no-prefix"])


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


def test_show_config(monkeypatch, capsys):
    for option in ["--show-config", "--show-config-json"]:
        monkeypatch.setattr(sys, "argv", ["prog", option, "-no-quiet"])
        with pytest.raises(SystemExit) as e:
            command_line()
        assert e.value.code == 0
        out = capsys.readouterr().out
        assert "IPApp" in out


def test_install_kernel_start_zmq_interface(monkeypatch, fake_kernel_dir: pathlib.Path, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "install",
            "--name=async-trio",
            "--display_name='my kernel'",
            "--BaseShell.timeout=0.01",
            "--interface_class=async_kernel.interface.zmq.ZMQInterface",
        ],
    )
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0
    kernel_dir = fake_kernel_dir.joinpath("async-trio")
    assert (kernel_dir).exists()
    out = capsys.readouterr().out
    msg = f"Installed kernelspec {str(kernel_dir)!r}"
    assert out.startswith(msg)
    spec = json.loads(kernel_dir.joinpath("kernel.json").read_bytes())
    assert spec == {
        "argv": [
            sys.executable,
            "-m",
            "async_kernel",
            "start",
            "--connection_file={connection_file}",
            "--launcher=launch_interface",
            "--name=async-trio",
            "--BaseShell.timeout=0.01",
            "--interface_class=async_kernel.interface.zmq.ZMQInterface",
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
def test_remove_kernelspec(monkeypatch, fake_kernel_dir, capsys, mode: Literal["folder", "prefix", "default"]):
    name = f"async-{mode}"
    kernel_dir = fake_kernel_dir / name
    (kernel_dir).mkdir()
    if mode == "folder":
        args = (f"--name={name}", f"--folder={fake_kernel_dir}")
    elif mode == "prefix":
        args = (f"--name={name}", f"--prefix={sys.prefix}")
    else:
        args = (f"--name={name}",)

    for command in ["install", "uninstall"]:
        monkeypatch.setattr(sys, "argv", ["prog", command, *args])
        with pytest.raises(SystemExit) as e:
            command_line()
        assert e.value.code == 0

    out = capsys.readouterr().out
    assert "Installed kernelspec" in out
    assert "Uninstalled kernelspec" in out
    assert not (kernel_dir).exists()


@pytest.mark.parametrize("config", ["--user", "-no-user", "--prefix=this$won't^work"])
def test_list(monkeypatch, config: str, capsys):
    monkeypatch.setattr(sys, "argv", ["prog", "-l", config])
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0
    assert capsys.readouterr().out


def test_command_launch_interface(monkeypatch, fake_kernel_dir: pathlib.Path):
    class EventSet(Event):
        @override
        def set(self):
            super().set()
            kernel: Kernel[BaseInterface[IPShell], IPShell] = async_kernel.utils.get_kernel()  # pyright: ignore[reportAssignmentType]
            assert kernel.parent.backend_options == {"use_uv": False}
            assert kernel.shell.timeout == 0.123
            assert kernel.shell.automagic is False
            kernel.caller.call_direct(kernel.parent.stop)

    cmd = [
        "prog",
        "start",
        f"--connection_file={fake_kernel_dir.joinpath('connection_file.json')}",
        "--backend_options",
        "use_uv=False",
        "--BaseShell.timeout=0.123",
        "--no-automagic",
        "--launcher=launch_interface",
    ]
    event_started = EventSet()
    monkeypatch.setattr(ZMQInterface, "event_started", event_started)
    monkeypatch.setattr(sys, "argv", cmd)
    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0


# We check for matplotlib which is NOT a dev dependency. The test doesn't import matplotlib
@pytest.mark.skipif(not importlib.util.find_spec("matplotlib"), reason="Gui tests fail on CI")
@pytest.mark.parametrize("backend", Backend)
@pytest.mark.parametrize("host", [Hosts.tk, Hosts.qt, None])
def test_command_launch_ZMQInterface_with_host(mocker, monkeypatch, backend, host):
    if host is Hosts.tk:
        if not importlib.util.find_spec("_tkinter"):
            pytest.skip("_tkinter not installed")
    elif host is Hosts.qt and not importlib.util.find_spec("PySide6"):
        pytest.skip("PySide6 not installed")

    cmd = [
        "prog",
        "start",
        f"--name=async-{host}",
        f"--backend={backend}",
        f"--host={host}",
        "--interface_class",
        "async_kernel.interface.zmq.ZMQInterface",
    ]
    monkeypatch.setattr(sys, "argv", cmd)
    kernel = cast("Kernel", None)  # pyright: ignore[reportInvalidCast]

    class EventSet(Event):
        @override
        def set(self):
            nonlocal kernel
            kernel = async_kernel.utils.get_kernel()
            super().set()
            kernel.caller.call_direct(kernel.parent.stop)

    event_started = EventSet()

    monkeypatch.setattr(ZMQInterface, "event_started", event_started)

    with pytest.raises(SystemExit) as e:
        command_line()
    assert e.value.code == 0
    assert isinstance(kernel.parent, ZMQInterface)
    assert kernel.parent.host == host
    assert kernel.parent.backend == backend


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


async def test_IPShellApp_gc(anyio_backend: Backend):
    collected = Event()
    async with IPApp() as interface:
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
