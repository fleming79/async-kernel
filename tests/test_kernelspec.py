from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING

import pytest
from jupyter_client.kernelspec import KernelSpec

from async_kernel.kernelspec import (
    get_kernel_dir,
    get_kernel_info,
    import_launcher,
    remove_kernelspec,
    write_kernel_spec,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_install_kernel_spec(tmp_path: Path, monkeypatch):

    def my_launcher(settings: dict) -> None:
        # note: debug break points won't work here.
        msg = "passed"
        raise RuntimeError(msg)

    kernel_path = tmp_path.joinpath("kernels", "my-kernel")

    with pytest.raises(ValueError, match="`name` cannot be specified when path is provided!"):
        write_kernel_spec(path=kernel_path, name="some name")

    assert write_kernel_spec(path=kernel_path, launcher=my_launcher) == kernel_path
    kernel_json = kernel_path.joinpath("kernel.json")
    assert kernel_json.exists()
    data = json.loads(kernel_json.read_bytes())
    spec = KernelSpec(**data)
    start_interface_string = next(v.removeprefix("--launcher=") for v in spec.argv if v.startswith("--launcher="))
    launcher = import_launcher(start_interface_string)

    with pytest.raises(RuntimeError, match="passed"):
        launcher({})

    assert "my-kernel" in get_kernel_info(kernel_path.parent)
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
        folder = "~/library/jupyter/kernels"
    else:
        folder = "~/.local/share/jupyter/kernels"
    path = get_kernel_dir(folder=folder)
    assert path.is_absolute()
    assert path.as_posix().lower().endswith("/jupyter/kernels")

    assert path == get_kernel_dir(user=True)

    with pytest.raises(ValueError, match="ambiguous"):
        get_kernel_dir(user=True, folder="some folder")
