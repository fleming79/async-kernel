from __future__ import annotations

import contextlib
import os
import signal
import sys
import time
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, Self

from aiologic.lowlevel import enable_signal_safety
from traitlets import traitlets
from traitlets.config.configurable import LoggingConfigurable
from typing_extensions import override

import async_kernel
from async_kernel import Caller, utils
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.interface import HasInterface
from async_kernel.pending import PendingManager
from async_kernel.typing import Channel, ExecuteContent, Job, T_interface_co

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from types import FrameType

    from async_kernel import Kernel
    from async_kernel.typing import Content


__all__ = ["BaseShell"]


class ShellPendingManager(PendingManager):
    "A pending manager to track the active shell/subshell."


class BaseShell(HasInterface[T_interface_co], LoggingConfigurable, Generic[T_interface_co]):
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

    "A set for callbacks to register for calling when `interrupt` is called."

    _execution_count = 0
    _resetting = False
    _interrupt_requested: bool | Literal["FORCE"] = False
    _interrupts: ClassVar[set[Callable[[], object]]] = set()
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

        with contextlib.suppress(ValueError, AttributeError):
            import signal  # noqa: PLC0415

            sig = signal.signal(signal.SIGINT, self._signal_handler)

            def restore() -> None:
                signal.signal(signal.SIGINT, sig)

            return restore
        return lambda: None

    def initialize(self) -> None:
        self.initialize = lambda: None  # Replace with a dummy patch.
        self.init_builtins()

    def init_builtins(self) -> None:
        pass

    def stop(self, *, force: bool = False) -> None:
        """
        Stop the shell.

        Args:
            force: Force a protected shell to stop.
        """

        if self.protected and not force:
            return
        if not self.is_mainshell:
            self.kernel._subshell_stopped(self)  # pyright: ignore[reportPrivateUsage]

    @enable_signal_safety
    def _signal_handler(self, signum, frame: FrameType | None) -> None:
        "Handle interrupt signals."

        match self._interrupt_requested:
            case "FORCE":
                self._interrupt_requested = False
                raise KernelInterrupt
            case True:
                if frame and frame.f_locals is self.kernel.shell.user_ns:
                    self._interrupt_requested = False
                    raise KernelInterrupt
                self.last_interrupt_frame = frame

                def clearlast_interrupt_frame():
                    if self.last_interrupt_frame is frame:
                        self.last_interrupt_frame = None

                def re_raise():
                    if self.last_interrupt_frame is frame:
                        self._interrupt_now(force=True)

                # Race to check if the main thread should be interrupted.
                self.kernel.callers[Channel.shell].call_direct(clearlast_interrupt_frame)
                self.kernel.callers[Channel.control].call_later(1, re_raise)
            case False:
                signal.default_int_handler(signum, frame)

    def _interrupt_now(self, *, force=False) -> None:
        """
        Request an interrupt of the currently running shell thread.

        If called from the main thread, sets the interrupt request flag and sends a SIGINT signal
        to the current process. On Windows, uses `signal.raise_signal`; on other platforms, uses `os.kill`.
        If `force` is True, sets the interrupt request flag to "FORCE".

        Args:
            force: If True, requests a forced interrupt. Defaults to False.
        """
        # Restricted this to when the shell is running in the main thread.
        if self.parent.callers[Channel.shell].id == Caller.CALLER_MAIN_THREAD_ID:
            self._interrupt_requested = "FORCE" if force else True
            if sys.platform == "win32":
                signal.raise_signal(signal.SIGINT)
                time.sleep(0)
            else:
                os.kill(os.getpid(), signal.SIGINT)

    def interrupt(self) -> None:
        """
        Perform a keyboard interrupt.
        """
        debugger = self.kernel.debugger
        if (sys.platform != "emscripten") and (not debugger.enabled or not debugger.stopped_threads):
            self._interrupt_now()
        while self._interrupts:
            try:
                self._interrupts.pop()()
            except Exception:
                pass

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

    def get_ipython(self) -> BaseShell:
        """Return the shell for the current context."""
        return async_kernel.utils.get_ipython()

    @contextlib.contextmanager
    def context(self) -> Generator[None, Any, None]:
        "A context manager where the shell is active."
        with self.pending_manager.context():
            yield

    def displayhook(self, result: Any) -> None:
        """
        Publish execution results.

        This is invoked every time the interpreter needs to print, and is
        activated by setting the variable sys.displayhook to it.
        """
        if result is not None and utils.show_result_enabled():
            content = {}
            content["execution_count"] = self.execution_count
            content["data"] = repr(result)
            content["metadata"] = {}
            self.parent.iopub_send("execute_result", content=content)

    async def execute_request(self, job: Job[ExecuteContent]) -> Content:
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
