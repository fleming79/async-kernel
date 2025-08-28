import sys
from pathlib import Path
from typing import TYPE_CHECKING

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomHook(BuildHookInterface):
    """The async_kernel build hook."""

    def initialize(self, version, build_data):  # pyright: ignore[reportImplicitOverride]
        """Initialize the hook."""
        here = Path(__file__).parent.resolve()

        sys.path.insert(0, str(here.joinpath("src", "async_kernel")))

        from kernelspec import KernelName, write_kernel_spec  # noqa: PLC0415

        if TYPE_CHECKING:
            from async_kernel.kernelspec import KernelName, write_kernel_spec  # noqa: PLC0415, TC004

        spec_folder = here.joinpath("data_kernelspec", "async")
        write_kernel_spec(spec_folder, kernel_name=KernelName.asyncio)
