"""Add and remove kernel specifications for Jupyter."""

from __future__ import annotations

import inspect
import json
import os
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
    "DEFAULT_COMMAND",
    "DEFAULT_LAUNCHER",
    "PROTOCOL_VERSION",
    "expand_path",
    "get_kernel_dir",
    "import_launcher",
    "make_argv",
    "write_kernel_spec",
]

# path to kernelspec resources
RESOURCES: Path = Path(__file__).parent.joinpath("resources")

CUSTOM_LAUNCHER_SEPARATOR: str = "↤"

PROTOCOL_VERSION: str = "5.5"
"The protocol that is supported by the kernel."

DEFAULT_LAUNCHER: str = "launch_zmq_kernel"
"An importable path to the default interface to start the kernel."

DEFAULT_COMMAND: tuple[str, ...] = (sys.executable, "-m", "async_kernel", "start")
""


def make_argv(
    *,
    connection_file: str = "{connection_file}",
    name: str = "async",
    launcher: str | InterfaceStartType = DEFAULT_LAUNCHER,
    command: tuple[str, ...] = DEFAULT_COMMAND,
    **kwargs: Any,
) -> list[str]:
    """Returns an argument vector (argv) that can be used to start a `Kernel`.

    This function returns a list of arguments can be used directly start a kernel with [subprocess.Popen][].
    It will always call [command.command_line][] as a python module.

    Args:
        connection_file: The path to the connection file.
        launcher:
            A self-contained function that accepts a dict of settings. The function
            is stored as a python file in the kernelspec folder.
            Or as string import path to a callable.
            be saved as a python file in the kernelspec folder.
            Or can be one of the names of the methods in the interface folder:
                - "launch_zmq_kernel"
                - "start_kernel_zmq_interface"
        name: The name to use for the kernel.
        command: The command line command to call.
        **kwargs: Additional settings to pass when creating the kernel passed to `launcher`.

    Returns:
        list: A list of command-line arguments to launch the kernel module.
    """
    argv = [
        *command,
        f"--connection_file={connection_file}",
    ]
    for k, v in ({"launcher": launcher, "name": name} | kwargs).items():
        argv.append(f"--{k}={v}")
    return list(map(str, argv))


def write_kernel_spec(
    path: Path | str | None = None,
    *,
    name: str = "async",
    display_name: str = "",
    command: tuple[str, ...] = DEFAULT_COMMAND,
    prefix: str = "",
    folder: str = "",
    launcher: str | InterfaceStartType = DEFAULT_LAUNCHER,
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
        command:
            The command to execute, passing .
        prefix:
            When provided the kernelspec will be installed to PREFIX/share/jupyter/kernels/KERNEL_NAME.
            This can be sys.prefix for installation inside virtual or conda envs.
        folder:
            A direct path the the kernel spec folder (must end with a folder named 'kernels').
        launcher:
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
            For example `..., **{'timeout'=0.1})`.
    """

    assert re.match(re.compile(r"^[a-z0-9._\-]+$", re.IGNORECASE), name)
    path = Path(path).expanduser() if path else (get_kernel_dir(folder=folder, prefix=prefix) / name)

    if callable(launcher) and len(inspect.signature(launcher).parameters) != 1:
        msg = "Invalid signature! `launcher` must accept exactly one argument (a dict of settings)."
        raise ValueError(msg)
    # stage resources
    try:
        path.mkdir(parents=True, exist_ok=True)

        # launcher
        if callable(launcher):
            f = path.joinpath("launcher.py")
            f.write_text(textwrap.dedent(inspect.getsource(launcher)))
            launcher = f"{f}{CUSTOM_LAUNCHER_SEPARATOR}{launcher.__name__}"
        # validate
        if launcher != DEFAULT_LAUNCHER:
            assert len(inspect.signature(import_launcher(launcher)).parameters) == 1
        if resources:
            shutil.copytree(src=resources, dst=path, dirs_exist_ok=True)
        argv = make_argv(
            launcher=launcher,
            connection_file=connection_file,
            name=name,
            command=command,
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

    Search locations: https://jupyter-client.readthedocs.io/en/latest/kernels.html#kernel-specs
    """
    if folder:
        path = expand_path(folder)
        assert path.name.lower() == "kernels"
        return path
    return expand_path(prefix or sys.prefix).joinpath("share", "jupyter", "kernels")


def expand_path(path: str | Path) -> Path:
    """
    Make the path absolute returning a new path object.

    Args:
        path: The path to process.

    Substitutions:
        - Windows environment variables are accepted such as `%APPDATA%` and `%PROGRAMDATA%`.
        -`~` is also substituted with  [pathlib.Path.expanduser][].
    """
    if sys.platform == "win32" and str(path).startswith("%"):
        key, base = str(path).split("%")[1:3]
        key = key.strip("%").upper()
        pth_ = Path(os.environ[key])
        assert pth_.exists()
        pth = pth_.joinpath(base.strip("\\/"))
        assert pth.parts[0] == pth_.parts[0]
        return pth
    if isinstance(path, str):
        path = Path(path)
    return path.expanduser().absolute()


def import_launcher(launcher: str = "", /) -> InterfaceStartType:
    """
    Import the kernel interface starter as defined in a kernel spec.

    Args:
        launcher: The name of the interface factory.

    Returns:
        The kernel factory.
    """
    launcher = launcher or DEFAULT_LAUNCHER
    if CUSTOM_LAUNCHER_SEPARATOR in launcher:
        name, factory_name = launcher.split(CUSTOM_LAUNCHER_SEPARATOR)
        glbls = {}
        exec(Path(name).read_bytes(), glbls)
        return glbls[factory_name]
    from async_kernel.common import import_item  # noqa: PLC0415

    if launcher in ["launch_zmq_kernel", "start_kernel_zmq_interface"]:
        return import_item(f"async_kernel.interface.{launcher}")

    return import_item(launcher)
