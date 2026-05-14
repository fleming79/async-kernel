from __future__ import annotations

import json
import sys

import pytest
from jupyter_client.kernelspec import KernelSpec

from async_kernel.kernelspec import get_kernel_dir, import_start_interface, write_kernel_spec


def test_write_kernel_spec(name, tmp_path, monkeypatch):

    def my_start_interface(settings: dict) -> None:
        # note: debug break points won't work here.
        msg = "passed"
        raise RuntimeError(msg)

    path = write_kernel_spec(tmp_path, name=name, start_interface=my_start_interface)
    kernel_json = path.joinpath("kernel.json")
    assert kernel_json.exists()
    data = json.loads(kernel_json.read_bytes())
    spec = KernelSpec(**data)
    start_interface_string = next(
        v.removeprefix("--start_interface=") for v in spec.argv if v.startswith("--start_interface=")
    )
    start_interface = import_start_interface(start_interface_string)

    with pytest.raises(RuntimeError, match="passed"):
        start_interface({})


def test_write_kernel_spec_fails():
    with pytest.raises(ValueError, match="not enough values to unpack"):
        write_kernel_spec(name="never-works", start_interface="not a factory")
    with pytest.raises(ValueError, match="Invalid signature"):
        write_kernel_spec(name="bad_interface", start_interface=lambda: None)  # pyright: ignore[reportArgumentType]


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
