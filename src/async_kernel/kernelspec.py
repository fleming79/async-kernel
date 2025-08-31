"""Add and remove kernel specifications for Jupyter."""

from __future__ import annotations

import enum
import json
import shutil
import sys
from pathlib import Path

from jupyter_client.kernelspec import KernelSpec, _is_valid_kernel_name  # pyright: ignore[reportPrivateUsage]

# path to kernelspec resources
RESOURCES = Path(__file__).parent.joinpath("resources")


__all__ = ["Backend", "KernelName", "get_kernel_dir", "make_argv", "write_kernel_spec"]


class Backend(enum.StrEnum):
    asyncio = "asyncio"
    trio = "trio"


class KernelName(enum.StrEnum):
    asyncio = "async"
    trio = "async-trio"


def make_argv(
    *,
    connection_file: str = "{connection_file}",
    kernel_name: KernelName | str = KernelName.asyncio,
    kernel_factory: str = "async_kernel.Kernel",
    fullpath: bool = True,
    **kwargs,
) -> list[str]:
    """Returns an argument vector (argv) that can be used to start a `Kernel`.

    This function returns a list of arguments can be used directly start a kernel with [subprocess.Popen][].
    It will always call [async_kernel.command.command_line][] as a python module.

    Args:
        connection_file: The path to the connection file.
        kernel_factory: The string import path to a callable that returns a non-started kernel.
            It must accepts one positional argument the arguments passed as kwgs here.
        kernel_name: The name of the kernel to use.
        fullpath: If True the full path to the executable is used, otherwise 'python' is used.

    kwargs:
        Additional settings to pass when creating the kernel passed to `kernel_factory`.
        When the kernel factThe key should be the dotted path to the attribute. Or if using a

    Returns:
        list: A list of command-line arguments to launch the kernel module.
    """
    argv = [(sys.executable if fullpath else "python"), "-m", "async_kernel", "-f", connection_file]
    for k, v in ({"kernel_factory": kernel_factory, "kernel_name": kernel_name} | kwargs).items():
        argv.append(f"--{k}={v}")
    return argv


def write_kernel_spec(
    path: Path | str | None = None,
    *,
    kernel_factory: str = "async_kernel.Kernel",
    kernel_name: KernelName | str = KernelName.asyncio,
    fullpath: bool = False,
    display_name: str = "",
    connection_file: str = "{connection_file}",
    prefix: str = "",
    **kwargs,
) -> Path:
    """
    Write a kernel spec for launching a kernel.

    Args:
        path: The path where to write the spec.
        kernel_factory: The string import path to a callable that creates the Kernel.
        kernel_name: The name of the kernel to use.
        fullpath: If True the full path to the executable is used, otherwise 'python' is used.
        display_name: The display name for Jupyter to use for the kernel. The default is `"Python ({kernel_name})"`.
        connection_file: The path to the connection file.
        prefix: given, the kernelspec will be installed to PREFIX/share/jupyter/kernels/KERNEL_NAME.
            This can be sys.prefix for installation inside virtual or conda envs.

    kwargs:
        Additional settings to use on the instance of the Kernel.
        kwargs added to [KernelSpec.argv][jupyter_client.kernelspec.KernelSpec.argv]. When
        The arguments are used as Kernel settings when starting the kernel.
    """
    assert _is_valid_kernel_name(kernel_name)
    path = Path(path) if path else (get_kernel_dir(prefix) / kernel_name)
    # stage resources
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(RESOURCES, path, dirs_exist_ok=True)
    spec = KernelSpec()
    spec.argv = make_argv(
        kernel_factory=kernel_factory,
        connection_file=connection_file,
        kernel_name=kernel_name,
        fullpath=fullpath,
        **kwargs,
    )
    spec.name = kernel_name
    spec.display_name = display_name or f"Python ({kernel_name})"
    spec.language = "python"
    spec.interrupt_mode = "message"
    spec.metadata = {"debugger": True}

    # write kernel.json
    with path.joinpath("kernel.json").open("w") as f:
        json.dump(spec.to_dict(), f, indent=1)
    return path


def get_kernel_dir(prefix: str = "") -> Path:
    """The path to where kernel specs are stored for Jupyter.

    Args:
        prefix: Defaults to sys.prefix (installable for a particular environment).
    """
    return Path(prefix or sys.prefix) / "share/jupyter/kernels"
