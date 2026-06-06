"""A zmq interface that is an IPython shell app."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Generic, Self

from IPython.core.application import BaseIPythonApplication
from IPython.core.profiledir import ProfileDir
from IPython.core.shellapp import InteractiveShellApp, shell_aliases, shell_flags
from traitlets import traitlets
from typing_extensions import override

from async_kernel.interface.base import BaseInterface
from async_kernel.interface.zmq import ZMQInterface
from async_kernel.typing import Hosts, NoValue, T_ipshell_co

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


__all__ = ["ZMQInterfaceIP"]


BaseInterface.classes.append(ProfileDir)


class ZMQInterfaceIP(  # pyright: ignore[reportUnsafeMultipleInheritance, reportIncompatibleVariableOverride]
    ZMQInterface[T_ipshell_co], BaseIPythonApplication, InteractiveShellApp, Generic[T_ipshell_co]
):
    """
    A kernel zmq interface that is an IPython [shellapp][IPython.core.shellapp.InteractiveShellApp].
    """

    description = traitlets.Unicode(
        "async-kernel: A Jupyter kernel providing an asynchronous IPython shell.",
    ).tag(config=True)
    "A description to use for the command line interface."

    aliases = (
        ZMQInterface.aliases
        | {
            "profile": "BaseIPythonApplication.profile",
            "ipython-dir": "BaseIPythonApplication.ipython_dir",
            "config": "BaseIPythonApplication.extra_config_file",
        }
        | shell_aliases
    )
    ""

    flags = ZMQInterface.flags | shell_flags
    ""

    @property
    @override
    def user_ns(self) -> dict[str, Any]:
        return self.shell.user_ns

    @override
    def initialize(self, argv: None | list | NoValue = ...) -> None:  # pyright: ignore[reportInvalidTypeForm]
        super().initialize(argv)
        if self.host is None:
            for k in ["pylab", "gui", "matplotlib"]:
                if host := Hosts.from_gui(getattr(self, k, None)):
                    self.host = host

    @override
    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        try:
            async with super().__asynccontextmanager__():
                self.shell = self.kernel.main_shell
                self.init_path()
                self.init_gui_pylab()
                self.init_code()
                self.init_extensions()
                yield self
        finally:
            self._zmq_context.term()
