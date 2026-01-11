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

    InterfaceStartType = Callable[[None | dict[str, Any]], Any]

__all__ = ["PROTOCOL_VERSION", "get_kernel_dir", "import_start_interface", "make_argv", "write_kernel_spec"]

# path to kernelspec resources
RESOURCES = Path(__file__).parent.joinpath("resources")
PROTOCOL_VERSION = "5.5"
CUSTOM_KERNEL_MARKER = "↤"
DEFAULT_START_INTERFACE = "async_kernel.interface.start_kernel_zmq_interface"


def make_argv(
    *,
    connection_file: str = "{connection_file}",
    kernel_name: str = "async",
    start_interface: str | InterfaceStartType = DEFAULT_START_INTERFACE,
    fullpath: bool = True,
    **kwargs: dict[str, Any],
) -> list[str]:
    """Returns an argument vector (argv) that can be used to start a `Kernel`.

    This function returns a list of arguments can be used directly start a kernel with [subprocess.Popen][].
    It will always call [command.command_line][] as a python module.

    Args:
        connection_file: The path to the connection file.
        start_interface: Either the kernel factory object itself, or the string import path to a
            callable that returns a non-started kernel.
        kernel_name: The name of the kernel to use.
        fullpath: If True the full path to the executable is used, otherwise 'python' is used.
        **kwargs: Additional settings to pass when creating the kernel passed to `start_interface`.

    Returns:
        list: A list of command-line arguments to launch the kernel module.
    """
    argv = [(sys.executable if fullpath else "python"), "-m", "async_kernel", "-f", connection_file]
    for k, v in ({"start_interface": start_interface, "kernel_name": kernel_name} | kwargs).items():
        argv.append(f"--{k}={v}")
    return argv


def write_kernel_spec(
    path: Path | str | None = None,
    *,
    kernel_name: str = "async",
    display_name: str = "",
    fullpath: bool = False,
    prefix: str = "",
    start_interface: str | InterfaceStartType = DEFAULT_START_INTERFACE,
    connection_file: str = "{connection_file}",
    **kwargs: dict[str, Any],
) -> Path:
    """
    Write a kernel spec for launching a kernel.

    Args:
        path: The path where to write the spec.
        kernel_name: The name of the kernel to use.
        fullpath: If True the full path to the executable is used, otherwise 'python' is used.
        display_name: The display name for Jupyter to use for the kernel. The default is `"Python ({kernel_name})"`.
        start_interface: The string import path to a callable that creates the Kernel or,
            a *self-contained* function that returns an instance of a `Kernel`.
        connection_file: The path to the connection file.
        prefix: given, the kernelspec will be installed to PREFIX/share/jupyter/kernels/KERNEL_NAME.
            This can be sys.prefix for installation inside virtual or conda envs.
        **kwargs: Pass additional settings to set on the instance of the `Kernel` when it is instantiated.
            Each setting should correspond to the dotted path to the attribute relative to the kernel.
            For example `..., **{'shell.timeout'=0.1})`.

    Example passing a callable start_interface:

        When `start_interface` is passed as a callable, the callable is stored in the file
        'kernel_spec.py' inside the kernelspec folder.

        ```python
        import async_kernel.kernelspec


        def start_interface(settings):
            from async_kernel import Kernel

            class MyKernel(Kernel):
                async def execute_request(self, job):
                    print(job)
                    return await super().execute_request(job)

            return MyKernel(settings)


        async_kernel.kernelspec.write_kernel_spec(
            kernel_name="async-print-job", start_interface=start_interface
        )
        ```

        Warning:
            Moving the spec folder will break the import which is stored as an absolute path.
    """
    from jupyter_client.kernelspec import KernelSpec  # noqa: PLC0415

    assert re.match(re.compile(r"^[a-z0-9._\-]+$", re.IGNORECASE), kernel_name)
    path = Path(path) if path else (get_kernel_dir(prefix) / kernel_name)
    # stage resources
    try:
        path.mkdir(parents=True, exist_ok=True)
        if callable(start_interface):
            with path.joinpath("start_interface.py").open("w") as f:
                f.write(textwrap.dedent(inspect.getsource(start_interface)))
            start_interface = f"{path}{CUSTOM_KERNEL_MARKER}{start_interface.__name__}"
        # validate
        if start_interface != DEFAULT_START_INTERFACE:
            import_start_interface(start_interface)
        shutil.copytree(src=RESOURCES, dst=path, dirs_exist_ok=True)
        spec = KernelSpec()
        spec.argv = make_argv(
            start_interface=start_interface,
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
        spec.kernel_protocol_version = PROTOCOL_VERSION
        # write kernel.json
        with path.joinpath("kernel.json").open("w") as f:
            json.dump(spec.to_dict(), f, indent=1)
    except Exception:
        shutil.rmtree(path, ignore_errors=True)
        raise
    else:
        return path


def remove_kernel_spec(kernel_name: str) -> bool:
    "Remove a kernelspec returning True if it was removed."
    if (folder := get_kernel_dir().joinpath(kernel_name)).exists():
        shutil.rmtree(folder, ignore_errors=True)
        return True
    return False


def get_kernel_dir(prefix: str = "") -> Path:
    """
    The path to where kernel specs are stored for Jupyter.

    Args:
        prefix: Defaults to sys.prefix (installable for a particular environment).
    """
    return Path(prefix or sys.prefix) / "share/jupyter/kernels"


def import_start_interface(start_interface: str = "", /) -> InterfaceStartType:
    """
    Import the kernel interface starter as defined in a kernel spec.

    Args:
        start_interface: The name of the interface factory.

    Returns:
        The kernel factory.
    """

    if CUSTOM_KERNEL_MARKER in start_interface:
        path, factory_name = start_interface.split(CUSTOM_KERNEL_MARKER)
        try:
            sys.path.insert(0, path)
            import start_interface as kf  # noqa: PLC0415

            factory = getattr(kf, factory_name)
            assert len(inspect.signature(factory).parameters) == 1
            return factory
        finally:
            sys.path.remove(path)
    from async_kernel.common import import_item  # noqa: PLC0415

    return import_item(start_interface or DEFAULT_START_INTERFACE)
