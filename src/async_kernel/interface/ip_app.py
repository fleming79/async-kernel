"""An IPython application with a zmq interface."""

from __future__ import annotations

from typing import Any, Generic

from IPython.core.application import BaseIPythonApplication
from IPython.core.profiledir import ProfileDir
from IPython.core.shellapp import InteractiveShellApp, shell_aliases, shell_flags
from traitlets import traitlets
from typing_extensions import override

from async_kernel.interface.base import Interface
from async_kernel.typing import Hosts, NoValue, T_ipshell_co

__all__ = ["IPApp"]


Interface.classes.append(ProfileDir)


class IPApp(Interface[T_ipshell_co], BaseIPythonApplication, InteractiveShellApp, Generic[T_ipshell_co]):  # pyright: ignore[reportUnsafeMultipleInheritance, reportIncompatibleVariableOverride, reportIncompatibleMethodOverride]
    """An IPython application with a zmq interface."""

    description = traitlets.Unicode(
        "async-kernel: A Jupyter kernel providing an asynchronous IPython shell.",
    ).tag(config=True)
    "A description to use for the command line interface."

    aliases = (
        Interface.aliases
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
        Interface.flags
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
    def initialize(self, argv: list | NoValue | None = None) -> None:
        super().initialize(argv)
        if self.host is None:
            for k in ["pylab", "gui", "matplotlib"]:
                if host := Hosts.from_gui(getattr(self, k, None)):
                    self.host = host
                    break

    @override
    async def _pre_start(self) -> None:
        self.init_path()
        self.init_gui_pylab()
        self.init_code()
        self.init_extensions()
        await super()._pre_start()
