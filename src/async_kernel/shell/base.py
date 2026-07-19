"""Defines the shell base class."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Generic, Literal, Self

from traitlets import traitlets
from traitlets.config.configurable import LoggingConfigurable
from typing_extensions import override

import async_kernel
from async_kernel import utils
from async_kernel.common import Fixed
from async_kernel.interface import HasInterface
from async_kernel.pending import PendingManager
from async_kernel.typing import T_interface_co

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable

    from async_kernel import Kernel
    from async_kernel.typing import Content


__all__ = ["BaseShell"]


class ShellPendingManager(PendingManager):
    """A pending manager to track the active shell/subshell."""


class BaseShell(HasInterface[T_interface_co], LoggingConfigurable, Generic[T_interface_co]):
    """The base shell implementation.

    This should be the left most inherited class to be used.
    """

    kernel: Fixed[Self, Kernel[T_interface_co, Self]] = Fixed(lambda c: c["owner"].parent.kernel)  # pyright: ignore[reportAttributeAccessIssue]
    ""

    pending_manager = Fixed(ShellPendingManager)
    """
    Provides the `subshell_id` for the shell which add all consenting pending created in
    the context of the shell.
    """

    user_ns_hidden: Fixed[Self, dict] = Fixed(dict)
    ""

    timeout = traitlets.CFloat(0.0).tag(config=True)
    "A timeout in seconds to complete execute requests."

    stop_on_error_time_offset = traitlets.Float(0.0).tag(config=True)
    "An offset to add to the cancellation time to catch late arriving execute requests."

    protected = traitlets.Bool(read_only=True)
    "Protect from accidental deletion."

    is_mainshell = traitlets.Bool(False, read_only=True)
    "Set by the mainshell to indicate it is the main shell."

    subshell_id: Fixed[Self, str | None] = Fixed(
        lambda c: None if c["owner"].is_mainshell else c["owner"].pending_manager.id
    )
    "Used to identify the subshell."

    _execution_count = 0
    _resetting = False
    _user_ns: Fixed[Self, dict] = Fixed(dict)
    _stop_on_error_info: Fixed[Self, dict[Literal["time", "execution_count"], Any]] = Fixed(dict)

    @property
    def user_global_ns(self) -> dict:
        if self.is_mainshell:
            return self.user_ns
        return (
            self.kernel.main_shell.user_global_ns.copy() if self._resetting else self.kernel.main_shell.user_global_ns
        )

    @property
    def banner(self) -> str:
        return (
            f"async-kernel v{async_kernel.__version__} name:{self.parent.name!r} backend:{str(self.parent.backend)!r}"
        )

    @property
    def execution_count(self) -> int:
        return self._execution_count

    @execution_count.setter
    def execution_count(self, value) -> None:
        return

    def _get_default_ns(self) -> dict[str, Any]:
        return {}

    @property
    def user_ns(self) -> dict[Any, Any]:
        ns = self._user_ns
        if "_ih" not in self._user_ns:
            ns.update(self._get_default_ns())
        return ns

    @user_ns.setter
    def user_ns(self, ns) -> None:
        ns = dict(ns)
        self.user_ns_hidden.clear()
        self._user_ns.clear()
        ns_ = self._get_default_ns()
        self.user_ns_hidden.update(ns_)
        self._user_ns.update(ns_)
        self._user_ns.update(ns)

    @override
    def __repr__(self) -> str:
        protected = " 🔐" if self.protected else ""
        if self.is_mainshell:
            return f"<{self.__class__.__name__}{protected}: Main Shell>"
        return f"<{self.__class__.__name__}{protected}: Subshell ({self.subshell_id})>"

    def __init__(self, *, protected: bool = False, **kwargs) -> None:

        if kwargs.pop("is_mainshell", False):
            self.set_trait("is_mainshell", True)
        super(LoggingConfigurable, self).__init__(**kwargs)
        self.set_trait("protected", protected)
        self.kernel._shell_created(self)  # pyright: ignore[reportPrivateUsage]
        if self.subshell_id:
            self.initialize()

    @contextlib.asynccontextmanager
    async def mainshell_running(self):
        # Used in the context of `Kernel._run`
        assert self.is_mainshell
        restore = self.apply_patches()
        try:
            self.initialize()
            self.log.info("Main shell started")
            yield
        finally:
            self.stop(force=True)
            self.log.info("Main shell stopped")
            restore()

    def apply_patches(self) -> Callable[[], None]:
        return lambda: None

    def initialize(self) -> None:
        self.initialize = lambda: None  # Replace with a dummy patch.
        self.init_builtins()

    def init_builtins(self) -> None:
        pass

    def stop(self, *, force: bool = False) -> None:
        """Stop the shell.

        Args:
            force: Force a protected shell to stop.
        """
        if self.protected and not force:
            return
        if not self.is_mainshell:
            self.kernel._subshell_stopped(self)  # pyright: ignore[reportPrivateUsage]
        self.reset(new_session=False)

    def reset(self, new_session=True, aggressive=False) -> None:
        """Reset the shell, cancelling all associated pending."""
        if not self._resetting:
            self._resetting = True
            try:
                for pen in self.pending_manager.pending:
                    pen.cancel()
                if new_session:
                    self._execution_count = 0
                    self._stop_on_error_info.clear()
            finally:
                self._resetting = False

    def get_ipython(self) -> BaseShell:
        """Return the shell for the current context."""
        return async_kernel.utils.get_ipython()

    @contextlib.contextmanager
    def context(self) -> Generator[None, Any, None]:
        """A context manager where the shell is active."""
        with self.pending_manager.context():
            yield

    def displayhook(self, result: Any) -> None:
        """Publish execution results.

        This is invoked every time the interpreter needs to print, and is
        activated by setting the variable sys.displayhook to it.
        """
        if result is not None and utils.show_result_enabled():
            content = {}
            content["execution_count"] = self.execution_count
            content["data"] = repr(result)
            content["metadata"] = {}
            self.parent.iopub_send("execute_result", content=content)

    async def do_execute(
        self,
        code: str = "",
        *,
        silent: bool = False,
        store_history: bool = False,
        user_expressions: dict[str, str] | None = None,
        allow_stdin: bool = False,
        stop_on_error: bool = False,
        cell_id: str | None = None,
        received_time: float = 0,
        tags: Iterable[str] = (),
        **_ignored,
    ) -> Content:
        """Execute code in the shell."""
        raise NotImplementedError

    async def do_complete(self, code: str, cursor_pos: int | None = None) -> Content:
        ""
        raise NotImplementedError

    async def is_complete(self, code: str) -> Content:
        ""
        raise NotImplementedError

    async def do_inspect(self, code: str, cursor_pos: int = 0, detail_level: Literal[0, 1] = 0) -> Content:
        ""
        raise NotImplementedError

    async def do_history(
        self,
        *,
        output: bool = False,
        raw: bool = True,
        hist_access_type: str,
        session: int = 0,
        start: int = 1,
        stop: int | None = None,
        n: int = 10,
        pattern: str = "*",
        unique: bool = False,
        **_ignored,
    ) -> Content:
        ""
        raise NotImplementedError
