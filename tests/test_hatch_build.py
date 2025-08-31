import pathlib
import shutil
import subprocess

import anyio
import pytest


@pytest.mark.flaky
async def test_hatch_buid_hook(anyio_backend) -> None:
    base = pathlib.Path(__file__).parent.parent
    spec_folder = base.joinpath("data_kernelspec")
    shutil.rmtree(spec_folder, ignore_errors=True)
    folder = spec_folder.joinpath("async")
    assert not folder.exists()
    subprocess.call(["hatch", "build", "--hooks-only"])
    while not folder.exists():
        await anyio.sleep(0.1)
    await anyio.sleep(0.1)
    assert {f.name for f in folder.glob(pattern="*")} == {
        "logo-svg.svg",
        "kernel.json",
        "logo-32x32.png",
        "logo-64x64.png",
    }
