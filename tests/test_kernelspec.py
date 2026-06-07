from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from async_kernel.kernelspec import (
    get_kernel_dir,
    get_kernel_info,
    import_launcher,
    make_argv,
    remove_kernelspec,
    validate_name,
    write_kernel_spec,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("name", ["", "invalid name"])
def test_validate_name(name: str):
    with pytest.raises(ValueError, match="Invalid name="):
        validate_name(name)


def test_make_argv():
    argv = make_argv(command=("my command",), abc=10, flags=["my_flag"])
    assert argv == ["my command", "--connection_file={connection_file}", "--name=async", "--abc=10", "--my_flag"]


def test_install_kernel_spec(tmp_path: Path, monkeypatch):

    name = "my-kernel"
    kernel_path = tmp_path.joinpath("kernels", name)

    command = ("python", "-m", "async_kernel", "start")
    assert (
        write_kernel_spec(path=kernel_path, command=command, name=name, flags=("--debug", "--no-quiet")) == kernel_path
    )
    kernel_json = kernel_path.joinpath("kernel.json")
    assert kernel_json.exists()

    info = get_kernel_info(kernel_path.parent)
    assert info == {
        "my-kernel": {
            "argv": [
                "python",
                "-m",
                "async_kernel",
                "start",
                "--connection_file={connection_file}",
                f"--name={name}",
                "--debug",
                "--no-quiet",
            ],
            "env": {},
            "display_name": f"Python {sys.version.split()[0]} ({name})",
            "language": "python",
            "interrupt_mode": "message",
            "metadata": {"debugger": True, "concurrent": True, "supported_encryption": "curve"},
            "kernel_protocol_version": "5.5",
        }
    }

    with pytest.raises(FileNotFoundError, match="not a kernel"):
        remove_kernelspec(kernel_path.parent, name="not a kernel")
    remove_kernelspec(kernel_path.parent, name=kernel_path.name)
    assert not get_kernel_info(kernel_path.parent)


def test_write_kernel_spec_fails():
    with pytest.raises(ValueError, match="Invalid name="):
        write_kernel_spec(name="invalid name has spaces", launcher="not a factory")
    with pytest.raises(ValueError, match="not enough values to unpack"):
        write_kernel_spec(name="never-works", launcher="not a factory")
    with pytest.raises(ValueError, match="Invalid signature"):
        write_kernel_spec(name="bad_interface", launcher=lambda: None)  # pyright: ignore[reportArgumentType]


def test_get_kernel_dir():

    if sys.platform == "win32":
        folder = "%APPDATA%\\jupyter\\kernels"
    elif sys.platform == "darwin":
        folder = "~/Library/Jupyter/kernels"
    else:
        folder = "~/.local/share/jupyter/kernels"
    path = get_kernel_dir(folder=folder)
    assert path.is_absolute()
    assert path.as_posix().lower().endswith("/jupyter/kernels")

    assert get_kernel_dir(user=True).as_posix().lower().endswith("/jupyter/kernels")

    with pytest.raises(ValueError, match="ambiguous"):
        get_kernel_dir(user=True, folder="some folder")


def test_import_launcher(
    tmp_path: Path,
):
    def my_launcher(settings: dict):
        return settings

    write_kernel_spec(path=tmp_path, launcher=my_launcher)
    assert tmp_path.joinpath("launcher.py").exists()
    argv: list[str] = get_kernel_info(tmp_path.parent)[tmp_path.name]["argv"]
    launcher = next(v.removeprefix("--launcher=") for v in argv if v.startswith("--launcher="))
    launcher = import_launcher(launcher)
    assert launcher({"okay": True}) == {"okay": True}
    import_launcher("")
