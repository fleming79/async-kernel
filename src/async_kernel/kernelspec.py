"""Add and remove kernel specifications for Jupyter."""

from __future__ import annotations

import inspect
import json
import re
import shutil
import sys
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    InterfaceStartType = Callable[[dict[str, Any]], Any]

__all__ = [
    "DEFAULT_EXECUTABLE",
    "DEFAULT_START_INTERFACE",
    "PROTOCOL_VERSION",
    "get_kernel_dir",
    "import_start_interface",
    "make_argv",
    "write_kernel_spec",
]

# path to kernelspec resources
RESOURCES: Path = Path(__file__).parent.joinpath("resources")

CUSTOM_START_INTERFACE_SYMBOL: str = "↤"

PROTOCOL_VERSION: str = "5.5"
"The protocol that is supported by the kernel."

DEFAULT_START_INTERFACE: str = "start_zmq_app"
"An importable path to the default interface to start the kernel."

DEFAULT_EXECUTABLE: tuple[str, ...] = ("python", "-m", "async_kernel")
""


def make_argv(
    *,
    connection_file: str = "{connection_file}",
    name: str = "async",
    start_interface: str | InterfaceStartType = DEFAULT_START_INTERFACE,
    executable: tuple[str, ...] = DEFAULT_EXECUTABLE,
    **kwargs: Any,
) -> list[str]:
    """Returns an argument vector (argv) that can be used to start a `Kernel`.

    This function returns a list of arguments can be used directly start a kernel with [subprocess.Popen][].
    It will always call [command.command_line][] as a python module.

    Args:
        connection_file: The path to the connection file.
        start_interface:
            A self-contained function that accepts a dict of settings. The function
            is stored as a python file in the kernelspec folder.
            Or as string import path to a callable.
            be saved as a python file in the kernelspec folder.
            Or can be one of the names of the methods in the interface folder:
                - "start_zmq_app"
                - "start_kernel_zmq_interface"
        name: The name to use for the kernel.
        executable: The command line executable to call.
        **kwargs: Additional settings to pass when creating the kernel passed to `start_interface`.

    Returns:
        list: A list of command-line arguments to launch the kernel module.
    """
    argv = [*executable, "-f", connection_file]
    for k, v in ({"start_interface": start_interface, "name": name} | kwargs).items():
        argv.append(f"--{k}={v}")
    return list(map(str, argv))


def write_kernel_spec(
    path: Path | str | None = None,
    *,
    name: str = "async",
    display_name: str = "",
    executable: tuple[str, ...] = DEFAULT_EXECUTABLE,
    prefix: str = "",
    folder: str = "",
    start_interface: str | InterfaceStartType = DEFAULT_START_INTERFACE,
    connection_file: str = "{connection_file}",
    env: dict | None = None,
    metadata: dict | None = None,
    language="python",
    resources: Path | None = RESOURCES,
    **kwargs: Any,
) -> Path:
    """
    Write a kernel spec for launching a kernel [ref](https://jupyter-client.readthedocs.io/en/stable/kernels.html#kernel-specs).

    Args:
        path:
            The path where to write the spec.
        name:
            The name of the kernel to use.
        display_name:
            The display name for Jupyter to use for the kernel. The default is `"Python ({name})"`.
        executable:
            The first part of 'argv' to use.
        prefix:
            When provided the kernelspec will be installed to PREFIX/share/jupyter/kernels/KERNEL_NAME.
            This can be sys.prefix for installation inside virtual or conda envs.
        folder:
            A direct path the the kernel spec folder (must end with a folder named 'kernels').
        start_interface:
            The string import path to a callable that creates the Kernel or, a *self-contained*
            function that returns an instance of a `Kernel`.
        connection_file:
            The path to the connection file.
        env:
            A mapping environment variables for the kernel to set prior to starting.
        metadata:
            A mapping of additional attributes to aid the client in kernel selection.
        resources:
            The path to the resources folder to include with the kernel spec.
        **kwargs:
            Pass additional settings to set on the instance of the `Kernel` when it is instantiated.
            Each setting should correspond to the dotted path to the attribute relative to the kernel.
            For example `..., **{'shell.timeout'=0.1})`.
    """

    assert re.match(re.compile(r"^[a-z0-9._\-]+$", re.IGNORECASE), name)
    path = Path(path).expanduser() if path else (get_kernel_dir(folder=folder, prefix=prefix) / name)

    if callable(start_interface) and len(inspect.signature(start_interface).parameters) != 1:
        msg = "start_interface"
        raise RuntimeError(msg)
    # stage resources
    try:
        path.mkdir(parents=True, exist_ok=True)

        # start_interface
        if callable(start_interface):
            f = path.joinpath("start_interface.py")
            f.write_text(textwrap.dedent(inspect.getsource(start_interface)))
            start_interface = f"{f}{CUSTOM_START_INTERFACE_SYMBOL}{start_interface.__name__}"
        # validate
        if start_interface != DEFAULT_START_INTERFACE:
            assert len(inspect.signature(import_start_interface(start_interface)).parameters) == 1
        if resources:
            shutil.copytree(src=resources, dst=path, dirs_exist_ok=True)
        argv = make_argv(
            start_interface=start_interface,
            connection_file=connection_file,
            name=name,
            executable=executable,
            **kwargs,
        )
        spec: dict[str, list[Any] | Any | dict[Any, Any] | str | dict[str, bool]] = {
            "argv": argv,
            "env": env or {},
            "display_name": display_name or f"Python {sys.version_info.major}.{sys.version_info.minor} ({name})",
            "language": language,
            "interrupt_mode": "message",
            "metadata": metadata if metadata is not None else {"debugger": True, "concurrent": True},
            "kernel_protocol_version": PROTOCOL_VERSION,
        }
        # write kernel.json
        path.joinpath("kernel.json").write_text(json.dumps(spec, indent=2))
    except Exception:
        shutil.rmtree(path, ignore_errors=True)
        raise
    else:
        return path


def remove_kernel_spec(name: str, *, folder: str = "", prefix: str = "") -> bool:
    "Remove a kernelspec returning True if it was removed."
    if (path := get_kernel_dir(folder=folder, prefix=prefix).joinpath(name)).exists():
        shutil.rmtree(path, ignore_errors=True)
        return True
    return False


def get_kernel_dir(*, folder: str = "", prefix: str = "") -> Path:
    """
    The path to where kernel specs are stored for Jupyter.

    If folder is passed, it is assumed to be the full path ending in 'kernels', prefix is ignored.

    Args:
        folder: The path to 'kernels' (must end with 'kernels').
        prefix: Defaults to sys.prefix (installable for a particular environment).
    """
    if folder:
        pth = Path(folder).expanduser()
        assert pth.name.lower() == "kernels"
        return pth
    return Path(prefix or sys.prefix).expanduser() / "share/jupyter/kernels"


def import_start_interface(start_interface: str = "", /) -> InterfaceStartType:
    """
    Import the kernel interface starter as defined in a kernel spec.

    Args:
        start_interface: The name of the interface factory.

    Returns:
        The kernel factory.
    """
    start_interface = start_interface or DEFAULT_START_INTERFACE
    if CUSTOM_START_INTERFACE_SYMBOL in start_interface:
        name, factory_name = start_interface.split(CUSTOM_START_INTERFACE_SYMBOL)
        glbls = {}
        exec(Path(name).read_bytes(), glbls)
        return glbls[factory_name]
    from async_kernel.common import import_item  # noqa: PLC0415

    if start_interface in ["start_zmq_app", "start_kernel_zmq_interface"]:
        return import_item(f"async_kernel.interface.{start_interface}")

    return import_item(start_interface)
