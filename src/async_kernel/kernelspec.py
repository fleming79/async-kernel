"""Add and remove kernel specifications for Jupyter."""

from __future__ import annotations

import enum
import inspect
import json
import re
import shutil
import sys
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Any

import traitlets
from jupyter_client.kernelspec import KernelSpec

# path to kernelspec resources
RESOURCES = Path(__file__).parent.joinpath("resources")

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_kernel import Kernel

    KernelFactoryType = Callable[[dict[str, Any]], Kernel]

__all__ = ["Backend", "KernelName", "get_kernel_dir", "make_argv", "write_kernel_spec"]


class Backend(enum.StrEnum):
    asyncio = "asyncio"
    trio = "trio"


class KernelName(enum.StrEnum):
    asyncio = "async"
    trio = "async-trio"


CUSTOM_KERNEL_MARKER = "↤"


def make_argv(
    *,
    connection_file: str = "{connection_file}",
    kernel_name: KernelName | str = KernelName.asyncio,
    kernel_factory: str | KernelFactoryType = "async_kernel.Kernel",
    fullpath: bool = True,
    **kwargs,
) -> list[str]:
    """Returns an argument vector (argv) that can be used to start a `Kernel`.

    This function returns a list of arguments can be used directly start a kernel with [subprocess.Popen][].
    It will always call [async_kernel.command.command_line][] as a python module.

    Args:
        connection_file: The path to the connection file.
        kernel_factory: Either the kernel factory object itself, or the string import path to a
            callable that returns a non-started kernel.
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
    kernel_name: KernelName | str,
    display_name: str = "",
    fullpath: bool = False,
    prefix: str = "",
    kernel_factory: str | KernelFactoryType = "async_kernel.Kernel",
    connection_file: str = "{connection_file}",
    **kwargs,
) -> Path:
    """
    Write a kernel spec for launching a kernel.

    Args:
        path: The path where to write the spec.
        kernel_name: The name of the kernel to use.
        fullpath: If True the full path to the executable is used, otherwise 'python' is used.
        display_name: The display name for Jupyter to use for the kernel. The default is `"Python ({kernel_name})"`.
        kernel_factory: The string import path to a callable that creates the Kernel or,
            a *self-contained* function that returns an instance of a `Kernel`.

        connection_file: The path to the connection file.
        prefix: given, the kernelspec will be installed to PREFIX/share/jupyter/kernels/KERNEL_NAME.
            This can be sys.prefix for installation inside virtual or conda envs.

    kwargs:
        Pass additional settings to set on the instance of the `Kernel` when it is instantiated.
        Each setting should correspond to the dotted path to the attribute in the kernel.
        For example `kernel.shell.execute_request_timeout` should be passed as `shell.execute_request_timeout`.
        Unpack from a dict to avoid syntax errors.

        kwargs added to [KernelSpec.argv][jupyter_client.kernelspec.KernelSpec.argv].

    ??? example "Example: Passing a callable kernel_factory."

        When `kernel_factory` is passed as a callable, the callable is stored in the file
        'kernel_spec.py' inside the kernelspec folder.

        ```python
        import async_kernel.kernelspec


        def kernel_factory(settings):
            from IPython.display import display
            import anyio
            from ipywidgets import Button

            from async_kernel import AsyncEvent, Kernel

            class MyKernel(Kernel):
                async def execute_request(self, job):
                    event = AsyncEvent()
                    b = Button(description="Continue")
                    b.on_click(lambda _: event.set())
                    display(b)
                    await event.wait()
                    b.close()
                    return await super().execute_request(job)

            return MyKernel(settings)


        async_kernel.kernelspec.write_kernel_spec(
            kernel_name="async-button", kernel_factory=kernel_factory
        )
        ```

        !!! warning

            Moving the spec folder will break the import which is stored as an absolute path.


    """
    assert re.match(re.compile(r"^[a-z0-9._\-]+$", re.IGNORECASE), kernel_name)
    path = Path(path) if path else (get_kernel_dir(prefix) / kernel_name)
    # stage resources
    try:
        path.mkdir(parents=True, exist_ok=True)
        if callable(kernel_factory):
            with path.joinpath("kernel_factory.py").open("w") as f:
                f.write(textwrap.dedent(inspect.getsource(kernel_factory)))
            kernel_factory = f"{path}{CUSTOM_KERNEL_MARKER}{kernel_factory.__name__}"
        # validate
        if kernel_factory != "async_kernel.Kernel":
            import_kernel_factory(kernel_factory)
        shutil.copytree(src=RESOURCES, dst=path, dirs_exist_ok=True)
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


def import_kernel_factory(kernel_factory: str = "") -> KernelFactoryType:
    """
    Import the kernel factory as defined in a kernel spec.

    Args:
        kernel_factory: The name of the kernel factory.

    Returns:
        The kernel factory.
    """
    if CUSTOM_KERNEL_MARKER in kernel_factory:
        path, factory_name = kernel_factory.split(CUSTOM_KERNEL_MARKER)
        try:
            sys.path.insert(0, path)
            import kernel_factory as kf  # noqa: PLC0415

            factory = getattr(kf, factory_name)
            assert len(inspect.signature(factory).parameters) == 1
            return factory
        finally:
            sys.path.remove(path)
    return traitlets.import_item(kernel_factory or "async_kernel.Kernel")
