from __future__ import annotations

import threading
import weakref
from collections.abc import Callable
from sqlite3 import OperationalError
from typing import TYPE_CHECKING, Any, Self

from IPython.core.displayhook import DisplayHook
from IPython.core.displaypub import DisplayPublisher
from IPython.core.extensions import ExtensionManager
from IPython.core.formatters import DisplayFormatter
from IPython.core.history import HistoryManager, HistorySavingThread
from IPython.core.prefilter import PrefilterManager
from traitlets import traitlets
from typing_extensions import override

from async_kernel import utils
from async_kernel.common import Fixed
from async_kernel.interface.base import BaseInterface, HasInterface

if TYPE_CHECKING:
    from collections.abc import Callable

    from IPython.core.interactiveshell import ExecutionResult

    from async_kernel.shell import IPShell
    from async_kernel.typing import Message


class IPDisplayHook(HasInterface, DisplayHook):
    """Set as sys.displayhook and called whenever the interpreter needs to display output."""

    cache_size = traitlets.Int(1000, min=3).tag(config=True)
    do_full_cache = traitlets.Int(0).tag(config=True)

    _ = __ = ___ = ""

    def __init__(self, shell: IPShell) -> None:
        self._shell_ref = weakref.ref(shell)
        super(DisplayHook, self).__init__()

    @property
    def exec_result(self) -> ExecutionResult | None:  # pyright: ignore[reportImplicitOverride]
        return None  # pragma: no cover

    @exec_result.setter
    def exec_result(self, exec_result: ExecutionResult) -> None:
        pass  # pragma: no cover

    @property
    @override
    def shell(self) -> IPShell:
        return self._shell_ref()  # pyright: ignore[reportReturnType]

    @property
    @override
    def prompt_count(self) -> int:
        return self.shell.execution_count

    @override
    def start_displayhook(self) -> None:
        pass

    @override
    def write_output_prompt(self) -> None:
        pass  # pragma: no cover

    @override
    def write_format_data(self, format_dict, md_dict=None) -> None:
        pass  # pragma: no cover

    @override
    def finish_displayhook(self) -> None:
        pass  # pragma: no cover

    @override
    def quiet(self) -> bool:
        return not utils.show_result_enabled()

    @override
    def __call__(self, result=None) -> None:
        """
        Publish execution results.

        This is invoked every time the interpreter needs to print, and is
        activated by setting the variable sys.displayhook to it.
        """
        if result is not None and not self.quiet():
            format_dict, md_dict = self.shell.display_formatter.format(result)
            self.update_user_ns(result)
            if format_dict:
                content = {}
                content["execution_count"] = self.shell.execution_count
                content["data"] = format_dict
                content["metadata"] = md_dict
                self.log_output(format_dict)
                self.parent.iopub_send("execute_result", content=content)


class IPDisplayPublisher(HasInterface, DisplayPublisher):
    """A display publisher that publishes data using [iopub_send][async_kernel.interface.base.BaseInterface.iopub_send]."""

    _hooks: Fixed[Self, list[Callable[[Message[Any]], Any]]] = Fixed(list)

    @override
    def publish(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        data: dict[str, Any],
        metadata: dict | None = None,
        *,
        transient: dict | None = None,
        update: bool = False,
        **kwargs,
    ) -> None:
        """
        Publish a display-data message.

        Args:
            data: A mime-bundle dict, keyed by mime-type.
            metadata: Metadata associated with the data.
            transient: Transient data that may only be relevant during a live display, such as display_id.
                Transient data should not be persisted to documents.
            update: If True, send an update_display_data message instead of display_data.

        [Reference](https://jupyter-client.readthedocs.io/en/stable/messaging.html#update-display-data)
        """
        content = {"data": data, "metadata": metadata or {}, "transient": transient or {}} | kwargs
        msg_type = "update_display_data" if update else "display_data"
        msg = self.parent.msg(msg_type, content=content, parent=utils.get_parent_message())
        for hook in self._hooks:
            try:
                msg = hook(msg)
            except Exception:
                pass
            if msg is None:
                return
        self.parent.iopub_send(msg)

    @override
    def clear_output(self, wait: bool = False) -> None:
        """
        Clear output associated with the current execution (cell).

        Args:
            wait: If True, the output will not be cleared immediately,
                instead waiting for the next display before clearing.
                This reduces bounce during repeated clear & display loops.
        """
        self.parent.iopub_send(msg_or_type="clear_output", content={"wait": wait}, ident=b"display_data")

    def register_hook(self, hook: Callable[[Message[Any]], Any]) -> None:
        """Register a hook for when publish is called.

        The hook should return the message or None.
        Only return `None` when the message should *not* be sent.
        """
        self._hooks.append(hook)

    def unregister_hook(self, hook: Callable[[Message[Any]], Any]) -> None:
        while hook in self._hooks:
            self._hooks.remove(hook)


class IPHistoryManager(HasInterface[BaseInterface["IPShell"]], HistoryManager):
    @property
    @override
    def shell(self) -> IPShell:
        return self._shell_ref()  # pyright: ignore[reportReturnType]

    @override
    def __init__(self, *, shell: IPShell) -> None:
        """Create a new history manager associated with a shell instance."""
        self._shell_ref = weakref.ref(shell)
        super(HistoryManager, self).__init__(hist_file="" if shell.is_mainshell else ":memory:")

        self.db_input_cache_lock = threading.Lock()
        self.db_output_cache_lock = threading.Lock()

        try:
            self.new_session()
        except OperationalError as e:
            self.log.exception(
                "Failed to create history session in %s. History will not be saved.", self.hist_file, exc_info=e
            )
            self.hist_file = ":memory:"

        self.using_thread = False
        if self.enabled and self.hist_file != ":memory:":
            self.save_thread = HistorySavingThread(self)
            utils.mark_thread_pydev_do_not_trace(self.save_thread)
            try:
                self.save_thread.start()
            except RuntimeError as e:
                self.log.exception(
                    "Failed to start history saving thread. History will not be saved.",
                    exc_info=e,
                )
                self.hist_file = ":memory:"
            else:
                self.using_thread = True
        else:
            self.save_thread = None
            if shell is not shell.kernel.main_shell:
                self.output_hist.update(shell.kernel.main_shell.history_manager.output_hist)

        self._instances.add(self)

    def stop(self) -> None:
        self.end_session()
        if thread := self.save_thread:
            thread.stop()
            thread.join()
            self.save_thread = None
        self._instances.discard(self)

    # @override
    # def store_inputs(self, line_num: int, source: str, source_raw: str | None = None) -> None:
    #     if DisplayHook.semicolon_at_end_of_expression(source):
    #         utils._suppress_output.set(True)  # pyright: ignore[reportPrivateUsage]
    #     return super().store_inputs(line_num, source, source_raw)


class IPDisplayFormatter(HasInterface, DisplayFormatter):
    pass


class IPPrefilterManager(HasInterface, PrefilterManager):
    pass


class IPExtensionManager(HasInterface, ExtensionManager):
    shell: IPShell

    def __init__(self, *, shell: IPShell) -> None:
        super().__init__(shell=shell)
