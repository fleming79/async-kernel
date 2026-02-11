from __future__ import annotations

import importlib.util
import json
import os
import signal
import sys
import types
from typing import TYPE_CHECKING

import anyio
import pytest

import async_kernel
from async_kernel import kernel as kernel_module
from async_kernel.command import command_line
from async_kernel.interface.zmq import ZMQKernelInterface
from async_kernel.kernelspec import make_argv
from async_kernel.typing import Backend
from tests import utils

if TYPE_CHECKING:
    import pathlib

    from jupyter_client.asynchronous.client import AsyncKernelClient


@pytest.fixture(scope="module", params=["tcp", "ipc"] if sys.platform == "linux" else ["tcp"])
def transport(request):
    return request.param


@pytest.fixture
def fake_kernel_dir(tmp_path, monkeypatch):
    kernel_dir = tmp_path / "share/jupyter/kernels"
    kernel_dir.mkdir(parents=True)
    monkeypatch.setattr(kernel_module, "sys", types.SimpleNamespace(prefix=str(tmp_path)))
    monkeypatch.setattr(sys, "prefix", str(tmp_path))
    return kernel_dir


def test_prints_help_when_no_args(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["prog"])
    command_line()
    out = capsys.readouterr().out
    assert "usage:" in out


def test_prints_version_info(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["prog", "-V"])
    command_line()
    out = capsys.readouterr().out
    assert f"async-kernel {async_kernel.__version__}" in out


def test_add_kernel(monkeypatch, fake_kernel_dir: pathlib.Path, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "-a", "async-trio", "--display_name='my kernel'", "--start_interface=async_kernel.kernel.Kernel"],
    )
    command_line()
    out = capsys.readouterr().out
    assert "Added kernel spec" in out
    kernel_dir = fake_kernel_dir.joinpath("async-trio")
    assert (kernel_dir).exists()
    with kernel_dir.joinpath("kernel.json").open("rb") as f:
        spec = json.load(f)
    assert spec == {
        "argv": [
            "python",
            "-m",
            "async_kernel",
            "-f",
            "{connection_file}",
            "--start_interface=async_kernel.kernel.Kernel",
            "--kernel_name=async-trio",
        ],
        "env": {},
        "display_name": "my kernel",
        "language": "python",
        "interrupt_mode": "message",
        "metadata": {"concurrent": True, "debugger": True},
        "kernel_protocol_version": "5.5",
    }


def test_remove_existing_kernel(monkeypatch, fake_kernel_dir, capsys):
    kernel_name = "asyncio"
    (fake_kernel_dir / kernel_name).mkdir()
    monkeypatch.setattr(sys, "argv", ["prog", "-r", kernel_name])
    command_line()
    out = capsys.readouterr().out
    assert "removed" in out
    assert not (fake_kernel_dir / kernel_name).exists()


def test_remove_nonexistent_kernel(monkeypatch, fake_kernel_dir, capsys):
    kernel_name = "not a kernel"
    monkeypatch.setattr(sys, "argv", ["prog", "-r", kernel_name])
    command_line()
    out = capsys.readouterr().out
    assert "not found!" in out


backend_combinations = [
    ("asyncio", {}),
    ("trio", {}),
    ("async_kernel.eventloop.tk_trio_guest.run", {}),
]
if importlib.util.find_spec("PySide6"):
    backend_combinations.append(("async_kernel.eventloop.qt_trio_guest.run", {"module": "PySide6"}))


@pytest.mark.parametrize(("backend", "backend_options"), backend_combinations)
def test_start_kernel_enable_gui(monkeypatch, backend: str, backend_options: dict):
    started = False
    if "tk_trio_guest" in backend:
        if os.getenv("GITHUB_ACTIONS"):
            pytest.skip("Skip on CI")
        gui = "tk"
    elif "qt_trio_guest" in backend:
        if os.getenv("GITHUB_ACTIONS"):
            pytest.skip("Skip on CI")
        gui = "qt"
    else:
        gui = "ipympl"

    async_kernel.Kernel._instance = None  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "-f",
            ".",
            "--kernel_name=async",
            f"--interface.backend={backend}",
            f"--interface.backend_options={backend_options}",
            "--no-print_kernel_messages",
        ],
    )

    async def wait_exit():
        nonlocal started
        async_kernel.Kernel().shell.enable_gui(gui)
        started = True

    monkeypatch.setattr(ZMQKernelInterface, "wait_exit", wait_exit())
    try:
        with pytest.raises(SystemExit) as e:
            command_line()
        assert e.value.code == 0
        assert started
    finally:
        async_kernel.Kernel._instance = None  # pyright: ignore[reportPrivateUsage]


async def test_subprocess_kernels_client(subprocess_kernels_client: AsyncKernelClient, kernel_name, transport):
    # Start & Stop a kernel
    backend = Backend.trio if "trio" in kernel_name.lower() else Backend.asyncio
    _, reply = await utils.execute(
        subprocess_kernels_client,
        "kernel = get_ipython().kernel",
        user_expressions={
            "kernel_name": "kernel.kernel_name",
            "backend": "kernel.interface.anyio_backend",
            "transport": "kernel.interface.transport",
        },
    )
    assert kernel_name in reply["user_expressions"]["kernel_name"]["data"]["text/plain"]
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
