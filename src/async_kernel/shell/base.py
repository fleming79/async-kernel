from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Generic, Literal, Self

from traitlets import traitlets
from traitlets.config import Configurable
from typing_extensions import override

import async_kernel
from async_kernel.common import Fixed
from async_kernel.interface import HasInterface
from async_kernel.pending import PendingManager
from async_kernel.typing import T_interface_co

if TYPE_CHECKING:
    from collections.abc import Generator

    from async_kernel import Kernel
    from async_kernel.typing import Content


__all__ = ["BaseShell"]


class ShellPendingManager(PendingManager):
    "A pending manager to track the active shell/subshell."


class BaseShell(HasInterface[T_interface_co], Configurable, Generic[T_interface_co]):
    """
    The base shell implementation.

    This should be the left most inherited class to be used.
    """

    kernel: Fixed[Self, Kernel[T_interface_co, Self]] = Fixed(lambda c: c["owner"].parent.kernel)  # pyright: ignore[reportAttributeAccessIssue]
    ""

    pending_manager = Fixed(ShellPendingManager)
    ""

    user_ns_hidden: Fixed[Self, dict] = Fixed(dict)
    ""

    timeout = traitlets.CFloat(0.0).tag(config=True)
    "A timeout in seconds to complete execute requests."

    stop_on_error_time_offset = traitlets.Float(0.0).tag(config=True)
    "An offset to add to the cancellation time to catch late arriving execute requests."

    protected = traitlets.Bool(read_only=True)
    ""
    is_mainshell = traitlets.Bool(False, read_only=True)
    ""

    subshell_id: Fixed[Self, str | None] = Fixed(
        lambda c: None if c["owner"].is_mainshell else c["owner"].pending_manager.id
    )

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

    def __init__(self, *, protected: bool = False, is_mainshell=False, **kwargs) -> None:
        self._baseshell_init(protected=protected, is_mainshell=is_mainshell)
        if is_mainshell:
            return
        super().__init__(**kwargs)

    def _baseshell_init(self, *, protected: bool, is_mainshell: bool):
        if is_mainshell:
            self.set_trait("is_mainshell", True)
            self.set_trait("protected", protected)
        elif not self.is_mainshell:
            self.set_trait("protected", protected)
            self.kernel._subshell_created(self)  # pyright: ignore[reportPrivateUsage]

    def stop(self, *, force: bool = False) -> None:
        """
        Stop the shell.

        Args:
            force: Force a protected shell to stop.
        """

        if self.protected and not force:
            return
        self.kernel._subshell_stopped(self)  # pyright: ignore[reportPrivateUsage]

    def reset(self, new_session=True, aggressive=False) -> None:
        "Reset the shell, cancelling all associated pending."
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

    def get_ipython(self) -> Self:
        """Return the shell for the current context."""
        return self.kernel.get_shell()

    @contextlib.contextmanager
    def context(self) -> Generator[None, Any, None]:
        "A context manager where the shell is active."
        with self.pending_manager.context():
            yield

    async def execute_request(
        self,
        code: str = "",
        *,
        silent: bool = False,
        store_history: bool = True,
        user_expressions: dict[str, str] | None = None,
        allow_stdin: bool = True,
        stop_on_error: bool = True,
        cell_id: str | None = None,
        received_time: float = 0,
        **_ignored,
    ) -> Content:
        """Handle a [execute request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)."""
        raise NotImplementedError

    async def do_complete_request(self, code: str, cursor_pos: int | None = None) -> Content:
        """Handle a [completion request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#completion)."""
        raise NotImplementedError

    async def is_complete_request(self, code: str) -> Content:
        """Handle an [is_complete request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness)."""
        raise NotImplementedError

    async def inspect_request(self, code: str, cursor_pos: int = 0, detail_level: Literal[0, 1] = 0) -> Content:
        """Handle a [inspect request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introspection)."""
        raise NotImplementedError

    async def history_request(
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
        """Handle a [history request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#history)."""
        raise NotImplementedError
