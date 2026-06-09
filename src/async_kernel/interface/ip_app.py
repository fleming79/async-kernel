"""An IPython application with a zmq interface."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Generic, Self

from IPython.core.application import BaseIPythonApplication
from IPython.core.profiledir import ProfileDir
from IPython.core.shellapp import InteractiveShellApp, shell_aliases, shell_flags
from traitlets import traitlets
from typing_extensions import override

from async_kernel.interface.zmq import ZMQInterface
from async_kernel.typing import Hosts, NoValue, T_ipshell_co

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


__all__ = ["IPApp"]


ZMQInterface.classes.append(ProfileDir)


class IPApp(  # pyright: ignore[reportUnsafeMultipleInheritance, reportIncompatibleVariableOverride]
    ZMQInterface[T_ipshell_co], BaseIPythonApplication, InteractiveShellApp, Generic[T_ipshell_co]
):
    """
    An IPython application with a zmq interface.
    """

    description = traitlets.Unicode(
        "async-kernel: A Jupyter kernel providing an asynchronous IPython shell.",
    ).tag(config=True)
    "A description to use for the command line interface."

    aliases = (
        ZMQInterface.aliases
        | {
            "profile-dir": "ProfileDir.location",
            "profile": "BaseIPythonApplication.profile",
            "ipython-dir": "BaseIPythonApplication.ipython_dir",
            "config": "BaseIPythonApplication.extra_config_file",
        }
        | shell_aliases
    )
    ""

    flags = (
        ZMQInterface.flags
        | {
            "automagic": (
                {"InteractiveShell": {"automagic": True}},
                "Turn on the auto calling of magic commands. Type %%magic at the IPython  prompt  for  more information.",
            ),
            "no-automagic": (
                {"InteractiveShell": {"automagic": False}},
                "Turn off the auto calling of magic commands.",
            ),
        }
        | shell_flags
    )
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
                    break

    @override
    @asynccontextmanager
    async def __asynccontextmanager__(self, *, set_started=True) -> AsyncGenerator[Self]:
        async with super().__asynccontextmanager__(set_started=False):
            self.shell = self.kernel.main_shell
            self.init_path()
            self.init_gui_pylab()
            self.init_code()
            self.init_extensions()
            if set_started:
                self._started()
            yield self
