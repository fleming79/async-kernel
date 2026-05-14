from __future__ import annotations

import argparse
import ast
import logging
import sys
from enum import StrEnum
from typing import TYPE_CHECKING

import async_kernel
from async_kernel.kernelspec import get_kernel_dir, import_start_interface, remove_kernel_spec, write_kernel_spec

if TYPE_CHECKING:
    from async_kernel.kernelspec import InterfaceStartType

    __all__ = ["command_line"]


def command_line() -> None:
    """
    Parses command-line arguments to manage kernel specs and start kernels.

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

    Raises:
        SystemExit: If an error occurs during kernel execution or if the
            program is interrupted.
    """

    class Mode(StrEnum):
        add = "add"
        remove = "remove"
        version = "version"
        help_all = "help_all"
        help = "help"
        start = "start"

    def safe_eval(val: str):
        try:
            return ast.literal_eval(val.strip())
        except Exception:
            return val

    kernel_dir = get_kernel_dir()
    description = """
Subcommands:
    start: Start the kernel. See `help_all` for detail about configuration options available.
"""
    epilog = f"""
- online Help: https://fleming79.github.io/async-kernel/latest/usage/commands/
- Jupyter kernel directory: {str(kernel_dir)!r}
- Installed kernels {[] if not kernel_dir.exists() else [item.name for item in kernel_dir.iterdir() if item.is_dir()]}
"""

    parser = argparse.ArgumentParser(
        prog="async-kernel",
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-a",
        "--add",
        dest="add",
        help="Write a kernel spec with the corresponding name. This will overwrite existing kernel specs of the same name.",
    )
    parser.add_argument(
        "-r",
        "--remove",
        dest="remove",
        help="Remove existing kernel specs.",
    )
    parser.add_argument(
        "-V",
        "--version",
        dest="version",
        help="Print version.",
        action="store_true",
    )
    parser.add_argument(
        "--help-all",
        dest="help_all",
        help="Print help all.",
        action="store_true",
    )
    args, unknownargs = parser.parse_known_args()
    if unknownargs and unknownargs[0] == Mode.start:
        mode = Mode(unknownargs.pop(0))
    else:
        mode = Mode.help
        for mode_ in Mode:
            if getattr(args, mode_, None):
                mode = mode_
                break

    # Convert unknownargs from flags to mappings
    settings = {}
    while unknownargs:
        k = unknownargs.pop(0).strip("-")
        if "=" in k:
            a, v = k.split("=", maxsplit=1)
            settings[a] = safe_eval(v)
        elif unknownargs and not unknownargs[0].startswith("-"):
            v = unknownargs.pop(0)
            if "=" in v:
                a, v = v.split("=", maxsplit=1)
                settings[k] = {a: safe_eval(v)}
            else:
                settings[k] = safe_eval(v)
        # https://docs.python.org/3/library/argparse.html#argparse.BooleanOptionalAction
        else:
            settings[k.removeprefix("no-")] = not k.startswith("no-")

    match mode:
        case Mode.add:
            settings["name"] = settings.get("name") or args.add
            path = write_kernel_spec(**settings)
            print(f"Added kernel spec {path!s}")
        case Mode.remove:
            folder = getattr(settings, "folder", "")
            prefix = getattr(settings, "prefix", "")
            for name in args.remove.split(","):
                msg = "removed" if remove_kernel_spec(name, folder=folder, prefix=prefix) else "not found!"
                print(f"Kernel spec: '{name}' {msg}")
        case Mode.version:
            print("async-kernel", async_kernel.__version__)
        case Mode.start | Mode.help_all:
            if "connection_file" not in settings:
                logging.basicConfig(level=logging.INFO)
            start_interface: InterfaceStartType = import_start_interface(settings.get("start_interface", ""))
            try:
                start_interface(settings)
            except KeyboardInterrupt:
                sys.exit(0)
        case _:
            parser.print_help()
    sys.exit(0)
