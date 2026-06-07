from __future__ import annotations

import argparse
import ast
import sys
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import async_kernel
from async_kernel.kernelspec import (
    get_kernel_dir,
    get_kernel_info,
    import_launcher,
    remove_kernelspec,
    write_kernel_spec,
)

if TYPE_CHECKING:
    from async_kernel.kernelspec import InterfaceStartType

    __all__ = ["command_line"]


def to_flags_and_settings(args: list[str]) -> tuple[list[str], dict[str, Any]]:
    """
    Convert `argvs` to flags and settings.

    Args:
        args: A list of arguments, the first of which must start with '-' or '--'.

    Notes:
        - There is no distinction made between '-' and '--'.
        - Flags are returned without '--' prefix.
    """

    def safe_eval(val: str) -> Any:
        try:
            return ast.literal_eval(val.strip())
        except Exception:
            return val

    def add(k: str, v: Any) -> None:
        settings[k.strip()] = safe_eval(v)

    flags, settings = [], {}
    args = list(args)
    while args:
        k = args.pop(0)
        if not k.startswith("-"):
            msg = f"Invalid arg detected: {k!r} is not prefixed with '-' or '--'!"
            raise ValueError(msg)
        k = k.strip("-")
        if "=" in k:
            add(*k.split("=", maxsplit=1))
        elif args and not args[0].startswith("-"):
            v = args.pop(0)
            if "=" in v:
                # Value is a dict
                a, v = v.split("=", maxsplit=1)
                v = settings.get(k, {}) | {a: safe_eval(v)}
                add(k, v)
            else:
                add(k, safe_eval(v))
        else:
            flags.append(k)
    return flags, settings


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
        install = "install"
        uninstall = "uninstall"
        version = "version"
        help_all = "help_all"
        help = "help"
        start = "start"
        list = "list"
        show_config = "show_config"
        show_config_json = "show_config_json"

    description = """

Online Help: https://fleming79.github.io/async-kernel/latest/usage/commands/

Commands:
    - start: Start a kernel - configuration options are used when starting the kernel.
    - install: Install a kernelsepc - configuration options are stored in the kernel spec.
    - uninstall: Uninstall a kernelspec.

Tips:
    - Pass the flag '--user' to install/uninstall using the user profile. 
    - See `--help-all` for all configuration options available.
    - When starting from command line, add `--log-level=INFO` to show logged output. 
"""
    epilog = ""

    parser = argparse.ArgumentParser(
        prog="async-kernel",
        # usage="async-kernel <optional command> options ...",
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-l",
        "--list",
        dest="list",
        help="list the kernels installed. Accepts one of the config options `--user` `--path` `--prefix`",
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--name",
        dest="name",
        help="The kernel name.",
        default="async",
    )
    parser.add_argument(
        "-u",
        "--user",
        dest="user",
        help="Select the user directory.",
        action="store_true",
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
    parser.add_argument(
        "--show-config",
        dest="show_config",
        help="Prints configuration.",
        action="store_true",
    )
    parser.add_argument(
        "--show-config-json",
        dest="show_config_json",
        help="Print configuration in json format.",
        action="store_true",
    )

    args, unknownargs = parser.parse_known_args()

    # Check for the 'start' command
    if unknownargs and unknownargs[0] in [Mode.start, Mode.install, Mode.uninstall]:
        mode = Mode(unknownargs.pop(0))
    else:
        mode = Mode.help
        for mode_ in Mode:
            if getattr(args, mode_, None):
                mode = mode_
                break

    flags, settings = to_flags_and_settings(unknownargs)

    match mode:
        case Mode.install:
            path = write_kernel_spec(user=args.user, name=args.name, flags=flags, **settings)
            print(f"Installed kernelspec {str(path)!r}")

        case Mode.uninstall:
            kernel_dir = get_kernel_dir(user=args.user, **settings)
            remove_kernelspec(kernel_dir, args.name)
            print(f"Uninstalled kernelspec {str(args.name)!r} from {str(kernel_dir)!r}")

        case Mode.list:
            kernel_dir = get_kernel_dir(user=args.user, **settings)
            kernels = get_kernel_info(kernel_dir)
            if not kernels:
                print(f"No kernels found in the folder {str(kernel_dir)!r}")
            else:
                width = max(map(len, kernels))
                m = len(kernels)
                header = f"Found {m} kernel{'s' if m > 1 else ''} found in the directory"
                n = max((len(str(kernel_dir)), len(header))) + 5
                print(f"{'─'}" * n)
                print(header)
                print(repr(str(kernel_dir)))
                # print("─" * n)
                print(f"{'Name'.ljust(width)}   Display name")
                print(f"{'─'}" * n)
                for name, info in kernels.items():
                    print(f"{name.ljust(width)}   {info.get('display_name')}")

        case Mode.version:
            print("async-kernel", async_kernel.__version__)
        case Mode.start | Mode.help_all | Mode.show_config | Mode.show_config_json:
            launcher: InterfaceStartType = import_launcher(settings.get("launcher", ""))
            settings["flags"] = flags
            try:
                launcher(settings)
            except KeyboardInterrupt:
                sys.exit(0)
        case _:
            parser.print_help()
    sys.exit(0)
