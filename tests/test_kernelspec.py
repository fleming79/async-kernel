from __future__ import annotations

import json

import pytest
from aiologic import Event
from jupyter_client.kernelspec import KernelSpec

from async_kernel.interface.zmq import ZMQKernelInterface
from async_kernel.kernelspec import DEFAULT_START_INTERFACE, import_start_interface, write_kernel_spec


@pytest.mark.parametrize(
    ("kernel_name", "start_interface"),
    [
        ("trio", DEFAULT_START_INTERFACE),
        ("function_factory", "custom"),
    ],
)
def test_write_kernel_spec(kernel_name, start_interface, tmp_path, monkeypatch):
    if start_interface == "custom":

        def my_start_interface(settings: dict | None):
            from async_kernel.interface import start_kernel_zmq_interface  # noqa: PLC0415
            from async_kernel.kernel import Kernel  # noqa: PLC0415

            class MyKernel(Kernel):
                pass

            kernel = MyKernel()
            # This would normally block, however wait_exit has been patched.
            start_kernel_zmq_interface(settings)
            assert kernel.interface.kernel is kernel
            return "custom"

        start_interface = my_start_interface

    path = write_kernel_spec(tmp_path, kernel_name=kernel_name, start_interface=start_interface)
    kernel_json = path.joinpath("kernel.json")
    assert kernel_json.exists()
    data = json.loads(kernel_json.read_bytes())
    spec = KernelSpec(**data)
    start_interface_string = next(
        v.removeprefix("--start_interface=") for v in spec.argv if v.startswith("--start_interface=")
    )
    starter = import_start_interface(start_interface_string)
    wait_exit = Event()
    wait_exit.set()

    monkeypatch.setattr(ZMQKernelInterface, "wait_exit", wait_exit)
    result = starter({"a": None})
    if start_interface == "custom":
        assert result == "custom"


def test_write_kernel_spec_fails():
    with pytest.raises(ValueError, match="not enough values to unpack"):
        write_kernel_spec(kernel_name="never-works", start_interface="not a factory")
