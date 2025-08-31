from __future__ import annotations

import argparse
import shutil
from collections.abc import Callable
from typing import TYPE_CHECKING

import anyio
import traitlets

import async_kernel
from async_kernel.kernel import Kernel, run_kernel
from async_kernel.kernelspec import KernelName, get_kernel_dir, write_kernel_spec

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    __all__ = ["command_line"]


def command_line(wait_exit_context: Callable[[], Awaitable] = anyio.sleep_forever) -> None:
    """Parses command-line arguments to manage kernel specs and start kernels.

    This function uses `argparse` to handle command-line arguments for
    various kernel operations, including:

    - Starting a kernel with a specified connection file.
    - Adding a new kernel specification.
    - Removing an existing kernel specification.
    - Print version.

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
    title = "Async kernel"
    kernel_dir: Path = get_kernel_dir()
    parser = argparse.ArgumentParser(
        description="=" * len(title)
        + f"\n{title}\n"
        + "=" * len(title)
        + "\n\n"
        + "With the async-kernel command line tool you can:\n\n"
        + "    - Add/remove kernel specs\n"
        + "    - start kernels\n\n"
        + "Online help: https://fleming79.github.io/async-kernel/latest/commands/#command-line \n\n"
        + f"Jupyter Kernel directory: '{kernel_dir}'",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-f",
        "--connection_file",
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
        help=f"Remove existing kernel specs. Installed kernels: {kernels}.",
    )
    parser.add_argument(
        "-v",
        "--version",
        dest="version",
        help="Print version",
        action="store_true",
    )
    args, unknownargs = parser.parse_known_args()
    cl_names = set(vars(args))

    # Convert unknownargs from flags to mappings
    for v in (v.lstrip("-") for v in unknownargs):
        if "=" in v:
            k, v_ = v.split("=", maxsplit=1)
            setattr(args, k, v_.strip("'\"").strip())
        else:
            # https://docs.python.org/3/library/argparse.html#argparse.BooleanOptionalAction
            setattr(args, v.removeprefix("no-"), False) if v.startswith("no-") else setattr(args, v, True)

    # Add kernel spec
    if args.add:
        if not hasattr(args, "kernel_name"):
            args.kernel_name = args.add
        for name in cl_names:
            delattr(args, name)
        path = write_kernel_spec(**vars(args))
        print(f"Added kernel spec {path!s}")

    # Remove kernel spec
    elif args.remove:
        for name in args.remove.split(","):
            folder = kernel_dir / str(name)
            if folder.exists():
                shutil.rmtree(folder, ignore_errors=True)
                print(f"Removed kernel spec: {name}")
            else:
                print(f"Kernel spec folder: '{name}' not found!")

    # Version
    elif args.version:
        print("async-kernel", async_kernel.__version__)

    # Start kernel
    elif args.connection_file:
        factory: type[Kernel] = traitlets.import_item(pth) if (pth := getattr(args, "kernel_factory", "")) else Kernel
        settings = vars(args)
        for k in cl_names.difference(["connection_file"]):
            settings.pop(k, None)
        if settings.get("connection_file") in {None, "", "."}:
            settings.pop("connection_file", None)
        run_kernel(factory(settings), wait_exit_context)

    # Default - print help
    else:
        parser.print_help()
