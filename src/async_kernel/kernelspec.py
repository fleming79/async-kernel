"""Add and remove kernel specifications for Jupyter."""

from __future__ import annotations

import inspect
import json
import os
import re
import sys
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    InterfaceStartType = Callable[[dict[str, Any]], Any]

__all__ = [
    "DEFAULT_COMMAND",
    "DEFAULT_LAUNCHER",
    "PROTOCOL_VERSION",
    "expand_path",
    "get_kernel_dir",
    "get_kernel_info",
    "import_launcher",
    "make_argv",
    "write_kernel_spec",
]

# path to kernelspec resources
RESOURCES: Path = Path(__file__).parent.joinpath("resources")

CUSTOM_LAUNCHER_SEPARATOR: str = "↤"

PROTOCOL_VERSION: str = "5.5"
"The protocol that is supported by the kernel."

DEFAULT_LAUNCHER: str = "launch_interface"
"An importable path to the default interface to start the kernel."

DEFAULT_COMMAND: tuple[str, ...] = (sys.executable, "-m", "async_kernel", "start")
""


def make_argv(
    *,
    connection_file: str = "{connection_file}",
    name: str = "async",
    launcher: str | InterfaceStartType = DEFAULT_LAUNCHER,
    command: tuple[str, ...] = DEFAULT_COMMAND,
    flags: Iterable[str] = (),
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
                - "launch_interface"
                - "launch_interface"
                - "start_kernel_zmq_interface"
        name: The name to use for the kernel.
        command: The command line command to call.
        flags: Any number of flags to insert in argv. Flags will be prefixed with '--'.
        **kwargs: Additional settings to pass when creating the kernel passed to `launcher`.

    Returns:
        list: A list of command-line arguments to launch the kernel module.
    """
    argv = [*command, f"--connection_file={connection_file}", *(f"--{f.strip('-')}" for f in flags)]
    for k, v in ({"launcher": launcher, "name": name} | kwargs).items():
        argv.append(f"--{k}={v}")
    return list(map(str, argv))


def get_kernel_dir(*, folder: str = "", prefix: str = "", user: bool = False) -> Path:
    """
    The path to where kernel specs are stored for Jupyter.

    If folder is passed, it is assumed to be the full path ending in 'kernels', prefix is ignored.

    Args:
        folder: The path to 'kernels' (must end with 'kernels').
        prefix: Defaults to sys.prefix (installable for a particular environment).
        user: Install for the user.

    Search locations: https://jupyter-client.readthedocs.io/en/latest/kernels.html#kernel-specs
    """

    if len([p for p in [folder, prefix, user] if p]) > 1:
        msg = "Providing more than one of [folder, prefix, user] is ambiguous"
        raise ValueError(msg)
    if user:
        from jupyter_core.paths import jupyter_data_dir  # noqa: PLC0415

        return Path(jupyter_data_dir()).joinpath("kernels")
    if folder:
        path = expand_path(folder)
        assert path.name.lower() == "kernels"
        return path
    return expand_path(prefix or sys.prefix).joinpath("share", "jupyter", "kernels")


def write_kernel_spec(
    *,
    path: Path | str | None = None,
    name: str = "async",
    display_name: str = "",
    user: bool = False,
    prefix: str = "",
    folder: str = "",
    launcher: str | InterfaceStartType = DEFAULT_LAUNCHER,
    command: tuple[str, ...] = DEFAULT_COMMAND,
    connection_file: str = "{connection_file}",
    env: dict | None = None,
    metadata: dict | None = None,
    language="python",
    resources: Path | None = RESOURCES,
    flags: Iterable[str] = (),
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
        user:
            To work with the user profile directory.
        prefix:
            When provided the kernelspec will be installed to PREFIX/share/jupyter/kernels/KERNEL_NAME.
            This can be sys.prefix for installation inside virtual or conda envs.
        folder:
            A direct path the the kernel spec folder (must end with a folder named 'kernels').
        launcher:
            The string import path to a callable that creates the Kernel or, a *self-contained*
            function that returns an instance of a `Kernel`.
        command:
            The command to execute to invoke the launcher.
        connection_file:
            The path to the connection file.
        env:
            A mapping environment variables for the kernel to set prior to starting.
        metadata:
            A mapping of additional attributes to aid the client in kernel selection.
        resources:
            The path to the resources folder to include with the kernel spec.
        flags:
            Flags to insert directly into the argv string.
        **kwargs:
            Pass additional settings to set on the instance of the `Kernel` when it is instantiated.
            Each setting should correspond to the dotted path to the attribute relative to the kernel.
            For example `..., **{'timeout'=0.1})`.
    """
    import shutil  # noqa: PLC0415

    if path:
        path = expand_path(path)
    else:
        if not name or not re.match(re.compile(r"^[a-z0-9._\-]+$", re.IGNORECASE), name):
            msg = f"Invalid {name=}!"
            raise ValueError(msg)
        path = get_kernel_dir(folder=folder, prefix=prefix, user=user).joinpath(name)

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
            flags=flags,
            **kwargs,
        )
        spec: dict[str, list[Any] | Any | dict[Any, Any] | str | dict[str, bool]] = {
            "argv": argv,
            "env": env or {},
            "display_name": display_name or f"Python {sys.version.split()[0]} ({name})",
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


def remove_kernelspec(kernel_dir: Path, name: str) -> None:
    "Remove a kernelspect."
    import shutil  # noqa: PLC0415

    kernels = get_kernel_info(kernel_dir)
    if name in kernels:
        shutil.rmtree(kernel_dir.joinpath(name))
        return
    msg = f"A kernelspec does not exist for {name=} in {str(kernel_dir)!r}"
    if kernels:
        msg = f"{msg}\nAvaliable names: {list(kernels)}"
    raise FileNotFoundError(msg)


def get_kernel_info(kernel_dir: Path) -> dict[str, dict[str, Any]]:
    """Get a dic of kernels installed in kernel_dir."""
    kernels = {}
    if kernel_dir.is_dir():
        for path in kernel_dir.iterdir():
            if path.is_dir() and (info_file := path.joinpath("kernel.json")).exists():
                kernels[path.name] = json.loads(info_file.read_bytes())
    return kernels


def expand_path(path: str | Path) -> Path:
    """
    Make the path absolute returning a new path object.

    Args:
        path: The path to process.

    Substitutions:
        - Windows environment variables are accepted such as `%APPDATA%` and `%PROGRAMDATA%`.
        -`~` is also substituted with [pathlib.Path.expanduser][].
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


def import_launcher(launcher: str = DEFAULT_LAUNCHER, /) -> InterfaceStartType:
    """
    Import the launcher as defined in a kernel spec.

    Args:
        launcher: The name of the interface factory.

    Returns:
        callable: The imported function responsible for launching the interface.
    """
    if CUSTOM_LAUNCHER_SEPARATOR in launcher:
        name, factory_name = launcher.split(CUSTOM_LAUNCHER_SEPARATOR)
        glbls = {}
        exec(Path(name).read_bytes(), glbls)
        return glbls[factory_name]
    from async_kernel.common import import_item  # noqa: PLC0415

    if not launcher or launcher == "launch_interface":
        launcher = "async_kernel.interface.launch_interface"

    return import_item(launcher)
