import pathlib
import shutil
import subprocess

import pytest


@pytest.mark.flaky
def test_hatch_buid_hook() -> None:
    base = pathlib.Path(__file__).parent.parent
    spec_folder = base.joinpath("data_kernelspec")
    shutil.rmtree(spec_folder, ignore_errors=True)
    folder = spec_folder.joinpath("async")
    assert not folder.exists()
    process = subprocess.Popen(["hatch", "build", "--hooks-only"])
    assert process.wait(timeout=60) == 0
    assert folder.exists()
    assert {f.name for f in folder.glob("*")} == {"logo-svg.svg", "kernel.json", "logo-32x32.png", "logo-64x64.png"}
