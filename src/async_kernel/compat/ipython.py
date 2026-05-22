from __future__ import annotations

import os
import threading
from collections.abc import Callable
from sqlite3 import OperationalError
from typing import TYPE_CHECKING, Any, Literal, Self

from IPython.core.builtin_trap import BuiltinTrap
from IPython.core.display_trap import DisplayTrap
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
from async_kernel.interface import BaseInterface, HasInterface
from async_kernel.typing import Channel, Message, MsgType

if TYPE_CHECKING:
    from collections.abc import Callable

    from IPython.core.interactiveshell import ExecutionResult

    from async_kernel.shell import BaseShell, IPShell


class AsyncDisplayHook(HasInterface, DisplayHook):
    """
    A displayhook subclass that publishes data using [iopub_send][async_kernel.interface.base.BaseInterface.iopub_send].

    This lives on the interface rather than a shell.

    """

    cache_size = traitlets.Int(1000, min=3).tag(config=True)
    do_full_cache = traitlets.Int(0).tag(config=True)

    _content: Fixed[Self, dict[int, dict[str, Any]]] = Fixed[Self, dict[int, dict[str, Any]]](dict)
    _ = __ = ___ = ""

    def __init__(self, **kwargs) -> None:
        self._get_shell = self.parent.kernel.get_shell
        super(DisplayHook, self).__init__()

    @property
    def exec_result(self) -> ExecutionResult | None:  # pyright: ignore[reportImplicitOverride]
        return None  # pragma: no cover

    @exec_result.setter
    def exec_result(self, exec_result: ExecutionResult) -> None:
        pass  # pragma: no cover

    @property
    def shell(self):  # pyright: ignore[reportImplicitOverride]
        return self._get_shell()

    @property
    @override
    def prompt_count(self) -> int:
        return self.shell.execution_count

    @override
    def start_displayhook(self) -> None:
        pass  # pragma: no cover

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
    def quiet(self):
        pass  # pragma: no cover

    @override
    def __call__(self, result=None) -> None:
        """
        Publish execution results.

        This is invoked every time the interpreter needs to print, and is
        activated by setting the variable sys.displayhook to it.
        """
        msg_type, quiet = "display_data", False
        try:
            job = utils.get_job()
            if job["msg"]["header"]["msg_type"] == MsgType.execute_request:
                msg_type = "execute_result"
                if not (quiet := job["msg"]["content"].get("silent", True)):
                    quiet = self.semicolon_at_end_of_expression(job["msg"]["content"]["code"])
        except LookupError:
            quiet = True
        # self.check_for_underscore()
        if result is not None and not quiet:
            content = {}
            format_dict, md_dict = self.compute_format_data(result)
            self.update_user_ns(result)
            if format_dict:
                content["execution_count"] = self.prompt_count
                content["data"] = format_dict
                content["metadata"] = md_dict
                self.log_output(format_dict)
                self.parent.iopub_send(msg_type, content=content)


class AsyncDisplayPublisher(HasInterface, DisplayPublisher):
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
        if "application/vnd.jupyter.widget-view+json" in data and os.environ.get("VSCODE_CWD"):  # pragma: no cover
            # ref: https://github.com/microsoft/vscode-jupyter/wiki/Component:-IPyWidgets#two-widget-managers
            # On occasion we get `Error 'widget model not found'`
            # As a work-around inject a delay so the widget can be registered first.
            self.parent.callers[Channel.control].call_later(0.2, self.parent.iopub_send, msg)
        else:
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


class AsyncHistoryManager(HasInterface[BaseInterface["IPShell"]], HistoryManager):
    @override
    def __init__(self, *, shell: BaseShell) -> None:
        """Create a new history manager associated with a shell instance."""
        if shell.subshell_id:
            self.hist_file = ":memory:"
        super(HistoryManager, self).__init__(shell=shell)

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
            if shell is not self.parent.kernel.main_shell:
                self.output_hist.update(self.parent.kernel.main_shell.history_manager.output_hist)

        self._instances.add(self)

    def stop(self) -> None:
        self.end_session()
        if thread := self.save_thread:
            thread.stop()
            thread.join()
            self.save_thread = None
        self._instances.discard(self)


class AsyncBuiltinTrap(BuiltinTrap):
    def __init__(self, shell=None):  # pyright: ignore[reportMissingSuperCall]
        pass

    @override
    def __enter__(self):  # pyright: ignore[reportMissingSuperCall]
        return self

    @override
    def __exit__(self, type, value, traceback):  # pyright: ignore[reportMissingSuperCall]
        return False


class AsyncDisplayTrap(HasInterface[BaseInterface["IPShell"]], DisplayTrap):
    def __init__(self) -> None:
        self.display = self.parent.kernel.main_shell.displayhook
        super().__init__(hook=self.display)

    def __enter__(self) -> None:  # pyright: ignore[reportMissingSuperCall, reportIncompatibleMethodOverride, reportImplicitOverride]
        return

    def __exit__(self, type, value, traceback) -> Literal[False]:  # pyright: ignore[reportMissingSuperCall, reportImplicitOverride]
        return False


class AsyncDisplayFormatter(HasInterface, DisplayFormatter):
    pass


class AsyncPrefilterManager(HasInterface, PrefilterManager):
    pass


class AsyncExtensionManager(HasInterface, ExtensionManager):
    shell: IPShell

    def __init__(self, *, shell: IPShell) -> None:
        super().__init__(shell=shell)
