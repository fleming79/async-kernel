from __future__ import annotations

import builtins
import contextlib
import json
import pathlib
import sys
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, overload

import anyio
import IPython.core.release
from aiologic.lowlevel import async_checkpoint
from IPython.core.completer import provisionalcompleter, rectify_completions
from IPython.core.displayhook import DisplayHook
from IPython.core.displaypub import DisplayPublisher
from IPython.core.interactiveshell import ExecutionResult, InteractiveShell, InteractiveShellABC
from IPython.core.interactiveshell import _modified_open as _modified_open_  # pyright: ignore[reportPrivateUsage]
from IPython.core.magic import Magics, line_magic, magics_class
from IPython.utils.tokenutil import token_at_cursor
from jupyter_client.jsonutil import json_default
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import CFloat, Dict, Float, Instance, Type, default, observe, traitlets
from typing_extensions import override

from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed, LastUpdatedDict
from async_kernel.compiler import XCachingCompiler
from async_kernel.pending import PendingManager
from async_kernel.typing import Content, NoValue, Tags

if TYPE_CHECKING:
    from collections.abc import Callable

    from IPython.core.history import HistoryManager
    from traitlets.config import Configurable


__all__ = ["AsyncInteractiveShell"]


class KernelInterruptError(Exception):
    "Raised to interrupt the kernel."

    # We subclass from InterruptedError so if the backend is a SelectorEventLoop it can catch the exception.
    # Other event loops don't appear to have this issue.


class AsyncDisplayHook(DisplayHook):
    """
    A displayhook subclass that publishes data using [iopub_send][async_kernel.kernel.Kernel.iopub_send].

    This is intended to work with an InteractiveShell instance. It sends a dict of different
    representations of the object.
    """

    kernel = Fixed(lambda _: utils.get_kernel())
    content: Dict[str, Any] = Dict()

    @property
    @override
    def prompt_count(self) -> int:
        return self.kernel.shell.execution_count

    @override
    def start_displayhook(self) -> None:
        """Start the display hook."""
        self.content = {}

    @override
    def write_output_prompt(self) -> None:
        """Write the output prompt."""
        self.content["execution_count"] = self.prompt_count

    @override
    def write_format_data(self, format_dict, md_dict=None) -> None:
        """Write format data to the message."""
        self.content["data"] = format_dict
        self.content["metadata"] = md_dict

    @override
    def finish_displayhook(self) -> None:
        """Finish up all displayhook activities."""
        if self.content:
            self.kernel.iopub_send("display_data", content=self.content)
            self.content = {}


class AsyncDisplayPublisher(DisplayPublisher):
    """A display publisher that publishes data using [iopub_send][async_kernel.kernel.Kernel.iopub_send]."""

    topic: ClassVar = b"display_data"

    def __init__(self, shell=None, *args, **kwargs) -> None:
        super().__init__(shell, *args, **kwargs)
        self._hooks = []

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
        msg = utils.get_kernel().session.msg(msg_type, content, parent=utils.get_parent())  # pyright: ignore[reportArgumentType]
        for hook in self._hooks:
            try:
                msg = hook(msg)
            except Exception:
                pass
            if msg is None:
                return
        utils.get_kernel().iopub_send(msg)

    @override
    def clear_output(self, wait: bool = False) -> None:
        """
        Clear output associated with the current execution (cell).

        Args:
            wait: If True, the output will not be cleared immediately,
                instead waiting for the next display before clearing.
                This reduces bounce during repeated clear & display loops.
        """
        utils.get_kernel().iopub_send(msg_or_type="clear_output", content={"wait": wait}, ident=self.topic)

    def register_hook(self, hook: Callable[[dict], dict | None]) -> None:
        """Register a hook for when publish is called.

        The hook should return the message or None.
        Only return `None` when the message should *not* be sent.
        """
        self._hooks.append(hook)

    def unregister_hook(self, hook: Callable[[dict], dict | None]) -> None:
        while hook in self._hooks:
            self._hooks.remove(hook)


class AsyncInteractiveShell(InteractiveShell):
    """
    An IPython InteractiveShell adapted to work with [Async kernel][async_kernel.kernel.Kernel].

    Notable differences:
        - All [execute requests][async_kernel.asyncshell.AsyncInteractiveShell.execute_request] are run asynchronously.
        - Supports a soft timeout specified via tags `timeout=<value in seconds>`[^1].
        - Gui event loops(tk, qt, ...) [are not presently supported][async_kernel.asyncshell.AsyncInteractiveShell.enable_gui].
        - Not all features are support (see "not-supported" features listed below).
        - `user_ns` and `user_global_ns` are same dictionary which is a fixed `LastUpdatedDict`.

        [^1]: When the execution time exceeds the timeout value, the code execution will "move on".
    """

    _execution_count = 0
    _resetting = False
    displayhook_class = Type(AsyncDisplayHook)
    display_pub_class = Type(AsyncDisplayPublisher)
    displayhook: Instance[AsyncDisplayHook]
    display_pub: Instance[AsyncDisplayPublisher]
    compiler_class = Type(XCachingCompiler)
    compile: Instance[XCachingCompiler]
    kernel = Fixed(lambda _: utils.get_kernel())
    pending_manager = Fixed(PendingManager)
    subshell_id = Fixed(lambda _: None)
    user_ns_hidden: Fixed[Self, dict] = Fixed(lambda c: c["owner"]._get_default_ns())
    user_global_ns: Fixed[Self, dict] = Fixed(lambda c: c["owner"]._user_ns)  # pyright: ignore[reportIncompatibleMethodOverride]

    _user_ns: Fixed[Self, LastUpdatedDict] = Fixed(LastUpdatedDict)  # pyright: ignore[reportIncompatibleVariableOverride]
    _main_mod_cache = Fixed(dict)
    _stop_on_error_pool: Fixed[Self, set[Callable[[], object]]] = Fixed(set)
    _stop_on_error_info: Fixed[Self, dict[Literal["time", "execution_count"], Any]] = Fixed(dict)

    timeout = CFloat(0.0)
    "A timeout in seconds to complete [execute requests][async_kernel.asyncshell.AsyncInteractiveShell.execute_request]."

    stop_on_error_time_offset = Float(0.0)
    "An offset to add to the cancellation time to catch late arriving execute requests."

    run_cell: Callable[..., ExecutionResult] = None  # pyright: ignore[reportAssignmentType]
    "**Not supported** -  use [execute_request][async_kernel.asyncshell.AsyncInteractiveShell.execute_request] instead."
    should_run_async = None  # pyright: ignore[reportAssignmentType]
    loop_runner_map = None
    loop_runner = None
    autoindent = False
    debug = None
    "**Not supported - use the built in debugger instead.**"

    @override
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}  kernel_name: {self.kernel.kernel_name!r} subhsell_id: {self.subshell_id}>"

    @override
    def __init__(self, parent: None | Configurable = None) -> None:
        super().__init__(parent=parent)
        self.pending_manager.activate()
        with contextlib.suppress(AttributeError):
            utils.mark_thread_pydev_do_not_trace(self.history_manager.save_thread)  # pyright: ignore[reportOptionalMemberAccess]

    def _get_default_ns(self):
        # Copied from `InteractiveShell.init_user_ns`
        history = self.history_manager
        return {
            "_ih": getattr(history, "input_hist_parsed", False),
            "_oh": getattr(history, "output_hist", None),
            "_dh": getattr(history, "dir_hist", "."),
            "In": getattr(history, "input_hist_parsed", False),
            "Out": getattr(history, "output_hist", False),
            "get_ipython": self.get_ipython,
            "exit": self.exiter,
            "quit": self.exiter,
            "open": _modified_open_,
        }

    @default("banner1")
    def _default_banner1(self) -> str:
        return (
            f"Python {sys.version}\n"
            f"Async kernel ({self.kernel.kernel_name})\n"
            f"IPython shell {IPython.core.release.version}\n"
        )

    @observe("exit_now")
    def _update_exit_now(self, _) -> None:
        """Stop eventloop when `exit_now` fires."""
        if self.exit_now:
            self.kernel.stop()

    def ask_exit(self) -> None:
        if self.kernel.raw_input("Are you sure you want to stop the kernel?\ny/[n]\n") == "y":
            self.exit_now = True

    @override
    def init_create_namespaces(self, user_module=None, user_ns=None) -> None:
        return

    @override
    def save_sys_module_state(self) -> None:
        return

    @override
    def init_sys_modules(self) -> None:
        return

    @override
    def init_user_ns(self) -> None:
        return

    @property
    @override
    def execution_count(self) -> int:
        return self._execution_count

    @execution_count.setter
    def execution_count(self, value) -> None:
        return

    @property
    @override
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
        self.init_user_ns()
        ns_ = self._get_default_ns()
        self.user_ns_hidden.update(ns_)
        self._user_ns.update(ns_)
        self._user_ns.update(ns)

    @property
    @override
    def ns_table(self) -> dict[str, dict[Any, Any] | dict[str, Any]]:
        return {"user_global": self.user_global_ns, "user_local": self.user_ns, "builtin": builtins.__dict__}

    async def execute_request(
        self,
        code: str = "",
        silent: bool = False,
        store_history: bool = True,
        user_expressions: dict[str, str] | None = None,
        allow_stdin: bool = True,
        stop_on_error: bool = True,
        **_ignored,
    ) -> Content:
        """Handle a [execute request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)."""
        if (utils.get_job().get("received_time", 0.0) < self._stop_on_error_info.get("time", 0)) and not silent:
            return utils.error_to_content(RuntimeError("Aborting due to prior exception")) | {
                "execution_count": self._stop_on_error_info.get("execution_count", 0)
            }

        tags: list[str] = utils.get_tags()
        timeout: float = utils.get_timeout(tags=tags)
        suppress_error: bool = Tags.suppress_error in tags
        raises_exception: bool = Tags.raises_exception in tags
        stop_on_error_override: bool = Tags.stop_on_error in tags

        if stop_on_error_override:
            stop_on_error = utils.get_tag_value(Tags.stop_on_error, stop_on_error)
        elif suppress_error or raises_exception:
            stop_on_error = False

        if silent:
            execution_count: int = self.execution_count
        else:
            execution_count = self._execution_count = self._execution_count + 1
            self.kernel.iopub_send(
                msg_or_type="execute_input",
                content={"code": code, "execution_count": execution_count},
                ident=self.kernel.topic("execute_input"),
            )
        caller = Caller()
        err = None
        with anyio.CancelScope() as scope:

            def cancel():
                if not silent:
                    caller.call_direct(scope.cancel, "Interrupted")

            result = None
            try:
                self.kernel.interrupts.add(cancel)
                if stop_on_error:
                    self._stop_on_error_pool.add(cancel)
                with anyio.fail_after(delay=timeout or None):
                    result = await self.run_cell_async(
                        raw_cell=code,
                        store_history=store_history,
                        silent=silent,
                        transformed_cell=self.transform_cell(code),
                        shell_futures=True,
                    )
            except (Exception, anyio.get_cancelled_exc_class()) as e:
                # A safeguard to catch exceptions not caught by the shell.
                err = KernelInterruptError() if self.kernel._last_interrupt_frame else e  # pyright: ignore[reportPrivateUsage]
            else:
                err = result.error_before_exec or result.error_in_exec if result else KernelInterruptError()
                if not err and Tags.raises_exception in tags:
                    msg = "An expected exception was not raised!"
                    err = RuntimeError(msg)
            finally:
                self._stop_on_error_pool.discard(cancel)
                self.kernel.interrupts.discard(cancel)
                self.events.trigger("post_execute")
                if not silent:
                    self.events.trigger("post_run_cell", result)
        if (err) and (suppress_error or (isinstance(err, anyio.get_cancelled_exc_class()) and (timeout != 0))):
            # Suppress the error due to either:
            # 1. tag
            # 2. timeout
            err = None
        content = {
            "status": "error" if err else "ok",
            "execution_count": execution_count,
            "user_expressions": self.user_expressions(user_expressions if user_expressions is not None else {}),
        }
        if err:
            content |= utils.error_to_content(err)
            if (not silent) and stop_on_error:
                with anyio.CancelScope(shield=True):
                    await async_checkpoint(force=True)
                    self._stop_on_error_info["time"] = time.monotonic() + (self.stop_on_error_time_offset)
                    self._stop_on_error_info["execution_count"] = execution_count
                    self.log.info("An error occurred in a non-silent execution request")
                    if stop_on_error:
                        for c in frozenset(self._stop_on_error_pool):
                            c()
        return content

    async def do_complete_request(self, code: str, cursor_pos: int | None = None) -> Content:
        """Handle a [completion request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#completion)."""

        cursor_pos = cursor_pos or len(code)
        with provisionalcompleter():
            completions = self.Completer.completions(code, cursor_pos)
            completions = list(rectify_completions(code, completions))
        comps = [
            {
                "start": comp.start,
                "end": comp.end,
                "text": comp.text,
                "type": comp.type,
                "signature": comp.signature,
            }
            for comp in completions
        ]
        s, e = (completions[0].start, completions[0].end) if completions else (cursor_pos, cursor_pos)
        matches = [c.text for c in completions]
        return {
            "matches": matches,
            "cursor_end": e,
            "cursor_start": s,
            "metadata": {"_jupyter_types_experimental": comps},
            "status": "ok",
        }

    async def is_complete_request(self, code: str) -> Content:
        """Handle an [is_complete request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness)."""
        status, indent_spaces = self.input_transformer_manager.check_complete(code)
        content = {"status": status}
        if status == "incomplete":
            content["indent"] = " " * indent_spaces
        return content

    async def inspect_request(self, code: str, cursor_pos: int = 0, detail_level: Literal[0, 1] = 0) -> Content:
        """Handle a [inspect request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introspection)."""
        content = {"data": {}, "metadata": {}, "found": True}
        try:
            oname = token_at_cursor(code, cursor_pos)
            bundle = self.object_inspect_mime(oname, detail_level=detail_level)
            content["data"] = bundle
            if not self.enable_html_pager:
                content["data"].pop("text/html")
        except KeyError:
            content["found"] = False
        return content

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
        history_manager: HistoryManager = self.history_manager  # pyright: ignore[reportAssignmentType]
        assert history_manager
        match hist_access_type:
            case "tail":
                hist = history_manager.get_tail(n=n, raw=raw, output=output, include_latest=False)
            case "range":
                hist = history_manager.get_range(session, start, stop, raw, output)
            case "search":
                hist = history_manager.search(pattern=pattern, raw=raw, output=output, n=n, unique=unique)
            case _:
                hist = []
        return {"history": list(hist), "status": "ok"}

    @override
    def _showtraceback(self, etype, evalue, stb) -> None:
        if Tags.suppress_error in utils.get_tags():
            if msg := utils.get_tag_value(Tags.suppress_error, "⚠"):
                print(msg)
            return
        if utils.get_timeout() != 0.0 and etype is anyio.get_cancelled_exc_class():
            etype, evalue, stb = TimeoutError, "Cell execute timeout", []
        self.kernel.iopub_send(
            msg_or_type="error",
            content={"traceback": stb, "ename": str(etype.__name__), "evalue": str(evalue)},
        )

    @override
    def reset(self, new_session=True, aggressive=False):
        if not self._resetting:
            self._resetting = True
            try:
                super().reset(new_session, aggressive)
                for pen in self.pending_manager.pending:
                    pen.cancel()
                if new_session:
                    self._execution_count = 0
                    self._stop_on_error_info.clear()
            finally:
                self._resetting = False

    @override
    def init_magics(self) -> None:
        """Initialize magics."""
        super().init_magics()
        self.register_magics(KernelMagics)

    @override
    def enable_gui(self, gui=None) -> None:
        """
        Enable a given gui.

        Supported guis:
            - [x] inline
            - [x] ipympl
            - [ ] tk
            - [ ] qt
        """
        supported_no_eventloop = [None, "inline", "ipympl"]
        if gui not in supported_no_eventloop:
            msg = f"The backend {gui=} is not supported by async-kernel. The currently supported gui options are: {supported_no_eventloop}."
            raise NotImplementedError(msg)


class SubshellPendingManager(PendingManager):
    "A pending manager for subshells."


class AsyncInteractiveSubshell(AsyncInteractiveShell):
    """
    An asynchronous interactive subshell for managing isolated execution contexts within an async kernel.

    Each subshell has a unique `user_ns`, but shares its `user_global_ns` with the main shell
    (which is also the `user_ns` of the main shell).

    Call [`subshell.stop(force=True)`][async_kernel.asyncshell.AsyncInteractiveSubshell.stop] to stop a
    protected subshell when it is no longer required.

    Attributes:
        stopped: Indicates whether the subshell has been stopped.
        protected: If True, prevents the subshell from being stopped unless forced.
        pending_manager: Tracks pending started in the context of the subshell.
        subshell_id: Unique identifier for the subshell.

    Methods:
        stop: Stops the subshell, deactivating pending operations and removing it from the manager.

    See also:
        - [async_kernel.utils.get_subshell_id][]
        - [async_kernel.utils.subshell_context][]
    """

    stopped = traitlets.Bool(read_only=True)
    protected = traitlets.Bool(read_only=True)
    pending_manager = Fixed(SubshellPendingManager)
    subshell_id: Fixed[Self, str] = Fixed(lambda c: c["owner"].pending_manager.context_id)

    @override
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} kernel_name: {self.kernel.kernel_name!r}  subshell_id: {self.subshell_id}{'  stopped' if self.stopped else ''}>"

    @property
    @override
    def user_global_ns(self) -> dict:  # pyright: ignore[reportIncompatibleVariableOverride]
        return (
            self.kernel.main_shell.user_global_ns.copy() if self._resetting else self.kernel.main_shell.user_global_ns
        )

    @override
    def __init__(self, *, protected: bool = True) -> None:
        super().__init__(parent=self.kernel.main_shell)
        self.set_trait("protected", protected)
        self.stop_on_error_time_offset = self.kernel.main_shell.stop_on_error_time_offset
        self.kernel.subshell_manager.subshells[self.subshell_id] = self

    def stop(self, *, force=False) -> None:
        "Stop this subshell."
        if force or not self.protected:
            self.pending_manager.deactivate(cancel_pending=True)
            self.reset(new_session=False)
            self.kernel.subshell_manager.subshells.pop(self.subshell_id, None)
            self.set_trait("stopped", True)


class SubshellManager:
    """
    Manages all instances of [subshells][async_kernel.asyncshell.AsyncInteractiveSubshell].

    Warning:

        **Do NOT instantiate directly.** Instead access the instance via [async_kernel.kernel.Kernel.subshell_manager][].
    """

    __slots__ = ["__weakref__"]

    main_shell: Fixed[Self, AsyncInteractiveShell] = Fixed(lambda _: utils.get_kernel().main_shell)
    subshells: dict[str, AsyncInteractiveSubshell] = {}
    default_subshell_class = AsyncInteractiveSubshell

    def create_subshell(self, *, protected: bool = True) -> AsyncInteractiveSubshell:
        """
        Create a new instance of the default subshell class.

        Call [`subshell.stop(force=True)`][async_kernel.asyncshell.AsyncInteractiveSubshell.stop] to stop a
        protected subshell when it is no longer required.

        Args:
            protected: Protect the subshell from accidental deletion.
        """
        return self.default_subshell_class(protected=protected)

    def list_subshells(self) -> list[str]:
        return list(self.subshells)

    if TYPE_CHECKING:

        @overload
        def get_shell(self, subshell_id: str) -> AsyncInteractiveSubshell: ...
        @overload
        def get_shell(self, subshell_id: None = ...) -> AsyncInteractiveShell: ...

    def get_shell(
        self,
        subshell_id: str | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
    ) -> AsyncInteractiveShell | AsyncInteractiveSubshell:
        """
        Get a subshell or the main shell.

        Args:
            subshell_id: The id of an existing subshell.
        """
        if subshell_id is NoValue:
            subshell_id = utils.get_subshell_id()
        try:
            return self.subshells[subshell_id] if subshell_id else self.main_shell
        except KeyError:
            msg = f"Subshell with {subshell_id=} does not exist!"
            raise RuntimeError(msg) from None

    def delete_subshell(self, subshell_id: str) -> None:
        """
        Stop a subshell unless it is protected.

        Args:
            subshell_id: The id of an existing subshell to stop.
        """
        if subshell := self.subshells.get(subshell_id):
            subshell.stop()

    def stop_all_subshells(self, *, force: bool = False) -> None:
        """Stop all current subshells.

        Args:
            force: Passed to [async_kernel.asyncshell.AsyncInteractiveSubshell.stop][].
        """
        for subshell in set(self.subshells.values()):
            subshell.stop(force=force)


@magics_class
class KernelMagics(Magics):
    """Extra magics for async kernel."""

    @line_magic
    def connect_info(self, _) -> None:
        """Print information for connecting other clients to this kernel."""
        kernel = utils.get_kernel()
        connection_file = pathlib.Path(kernel.connection_file)
        # if it's in the default dir, truncate to basename
        if jupyter_runtime_dir() == str(connection_file.parent):
            connection_file = connection_file.name
        info = kernel.get_connection_info()
        print(
            json.dumps(info, indent=2, default=json_default),
            "Paste the above JSON into a file, and connect with:\n"
            + "    $> jupyter <app> --existing <file>\n"
            + "or, if you are local, you can connect with just:\n"
            + f"    $> jupyter <app> --existing {connection_file}\n"
            + "or even just:\n"
            + "    $> jupyter <app> --existing\n"
            + "if this is the most recent Jupyter kernel you have started.",
        )

    @line_magic
    def callers(self, _) -> None:
        "Print a table of [Callers][async_kernel.caller.Caller], indicating its status including:  -running - protected - on the current thread."
        callers = Caller.all_callers(running_only=False)
        n = max(len(c.name) for c in callers) + 6
        m = max(len(repr(c.thread)) for c in callers) + 6
        lines = ["".join(["Name".center(n), "Running ", "Protected", "Thread".center(m)]), "─" * (n + m + 22)]
        for caller in callers:
            running = ("✓" if caller.running else "✗").center(8)
            protected = "   🔐    " if caller.protected else "         "
            name = caller.name + " " * (n - len(caller.name))
            thread = repr(caller.thread)
            if caller.thread is threading.current_thread():
                thread += " ← current"
            lines.append("".join([name, running.center(8), protected, thread]))
        print(*lines, sep="\n")

    @line_magic
    def subshell(self, _) -> None:
        """Print subshell info [ref](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#list-subshells).

        See also:
            - [async_kernel.utils.get_kernel][]
        """
        kernel = utils.get_kernel()
        subshells = kernel.subshell_manager.list_subshells()
        subshell_list = (
            f"\t----- {len(subshells)} x subshell -----\n" + "\n".join(subshells) if subshells else "-- No subshells --"
        )
        print(f"Current shell:\t{kernel.shell}\n\n{subshell_list}")


InteractiveShellABC.register(AsyncInteractiveShell)
