from __future__ import annotations

import json
import shutil

import pytest
from jupyter_client.kernelspec import KernelSpec

from async_kernel.kernelspec import KernelName, write_kernel_spec


@pytest.mark.parametrize(
    ("kernel_name", "kernel_factory"),
    [
        (KernelName.trio, "async_kernel.Kernel"),
        ("function_factory", "function"),
    ],
)
def test_write_kernel_spec(kernel_name: KernelName, kernel_factory, tmp_path):
    if kernel_factory == "function":

        def my_kernel_factory(settings):
            from async_kernel import Kernel  # noqa: PLC0415

            class MyKernel(Kernel):
                pass

            return MyKernel(settings)

        kernel_factory = my_kernel_factory

    path = write_kernel_spec(tmp_path, kernel_name=kernel_name, kernel_factory=kernel_factory)
    kernel_json = path.joinpath("kernel.json")
    assert kernel_json.exists()
    with kernel_json.open("r") as f:
        data = json.load(f)
    KernelSpec(**data)
    shutil.rmtree(path)


def test_write_kernel_spec_fails():
    with pytest.raises(ModuleNotFoundError):
        write_kernel_spec(kernel_name="never-works", kernel_factory="not a factory")
