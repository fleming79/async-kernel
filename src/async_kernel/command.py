from __future__ import annotations

import argparse
import contextlib
import shutil
import sys
import traceback
from itertools import pairwise
from typing import TYPE_CHECKING, Any

import anyio
import traitlets

from async_kernel.kernel import Kernel
from async_kernel.kernelspec import Backend, KernelName, get_kernel_dir, write_kernel_spec

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    __all__ = ["command_line", "setattr_nested"]


def setattr_nested(obj: object, name: str, value: str | Any) -> None:
    """Set a nested attribute of an object.

    If the attribute name contains dots, it is interpreted as a nested attribute.
    For example, if name is "a.b.c", then the code will attempt to set obj.a.b.c to value.

    This is primarily intended for use with [async_kernel.command.command_line][]
    to set the nesteded attributes on on kernels.

    Args:
        obj: The object to set the attribute on.
        name: The name of the attribute to set.
        value: The value to set the attribute to.
    """
    if len(bits := name.split(".")) > 1:
        try:
            obj = getattr(obj, bits[0])
        except Exception:
            return
        setattr_nested(obj, ".".join(bits[1:]), value)
    if (isinstance(obj, traitlets.HasTraits) and obj.has_trait(name)) or hasattr(obj, name):
        try:
            setattr(obj, name, value)
        except Exception:
            setattr(obj, name, eval(value))


def command_line(wait_exit_context: Callable[[], Awaitable] = anyio.sleep_forever) -> None:
    """Parses command-line arguments to manage and start kernels.

    This function uses `argparse` to handle command-line arguments for
    various kernel operations, including:

    - Starting a kernel with a specified connection file.
    - Adding a new kernel specification.
    - Removing an existing kernel specification.

    The function determines the appropriate action based on the provided
    arguments and either starts a kernel, adds a kernel spec, or removes
    a kernel spec.  If no connection file is provided and no other action
    is specified, it prints the help message.

    When starting a kernel, it imports the specified kernel factory (or uses
    the default `Kernel` class) and configures the kernel instance with
    the provided arguments. It then starts the kernel within an `anyio`
    context, handling keyboard interrupts and other exceptions.

    Args:
        wait_exit_context: An optional asynchronous function or context manager
            that determines how long the kernel should run. Defaults to
            `anyio.sleep_forever`, which keeps the kernel running indefinitely
            until an external signal is received.

    Raises:
        SystemExit: If an error occurs during kernel execution or if the
            program is interrupted.
    """
    kernel_dir: Path = get_kernel_dir()
    parser = argparse.ArgumentParser(
        description="Kernel interface to start a kernel or add/remove a kernel spec. "
        + f"The Jupyter Kernel directory is: f'{kernel_dir}'"
    )
    parser.add_argument(
        "-f",
        "--file",
        dest="connection_file",
        help="Start a Kernel with a connection file. To start a Kernel without a file use a period `.`.",
    )
    parser.add_argument(
        "-a",
        "--add",
        dest="add",
        help=f"Add a kernel spec. Default kernel names are: {list(map(str, KernelName))}.\n"
        + "To specify a 'trio' backend, include 'trio' in the name. Other options are also permitted. See: `write_kernel_spec` for detail.",
    )
    kernels = [] if not kernel_dir.exists() else [item.name for item in kernel_dir.iterdir() if item.is_dir()]
    parser.add_argument(
        "-r",
        "--remove",
        dest="remove",
        help=f"remove existing kernel specs. Installed kernels: {kernels}",
    )

    args, unknownargs = parser.parse_known_args()
    for k, v in pairwise(unknownargs):
        if k.startswith("--"):
            setattr(args, k.removeprefix("--"), v)
    if args.add:
        if not hasattr(args, "kernel_name"):
            args.kernel_name = args.add
        for name in ["add", "remove"] + (["connection_file"] if args.connection_file is None else []):
            delattr(args, name)
        path = write_kernel_spec(**vars(args))
        print(f"Added kernel spec {path!s}")
    elif args.remove:
        for name in args.remove.split(","):
            folder = kernel_dir / str(name)
            if folder.exists():
                shutil.rmtree(folder, ignore_errors=True)
                print(f"Removed kernel spec: {name}")
            else:
                print(f"Kernel spec folder: '{name}' not found!")

    elif not args.connection_file:
        parser.print_help()
    else:
        kernel_factory = getattr(args, "kernel_factory", None)
        kernel_name: str = getattr(args, "kernel_name", None) or KernelName.asyncio
        factory: type[Kernel] = traitlets.import_item(kernel_factory) if kernel_factory else Kernel
        kernel = factory(kernel_name=kernel_name)
        for k, v in vars(args).items():
            if (k == "connection_file" and v == ".") or k in ["add", "remove"]:
                continue
            setattr_nested(kernel, k, v)

        async def _start() -> None:
            print("Starting kernel")
            async with kernel.start_in_context():
                with contextlib.suppress(kernel.CancelledError):
                    await wait_exit_context()

        try:
            backend = Backend.trio if "trio" in kernel_name.lower() else Backend.asyncio
            anyio.run(_start, backend=backend)
        except KeyboardInterrupt:
            pass
        except BaseException as e:
            traceback.print_exception(e, file=sys.stderr)
            if sys.__stderr__ is not sys.stderr:
                traceback.print_exception(e, file=sys.__stderr__)
            sys.exit(1)
        else:
            sys.exit(0)
        finally:
            print("Kernel stopped: ", kernel.connection_file)
