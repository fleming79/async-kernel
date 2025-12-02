from __future__ import annotations

import builtins
import json
import pathlib
import sys
import threading
import time
import uuid
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
from traitlets import CFloat, Dict, Instance, Type, default, observe, traitlets
from typing_extensions import override

from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed
from async_kernel.compiler import XCachingCompiler
from async_kernel.typing import Content, Message, MetadataKeys, NoValue, Tags

if TYPE_CHECKING:
    from collections.abc import Callable

    from IPython.core.history import HistoryManager

    from async_kernel.kernel import Kernel


__all__ = ["AsyncDisplayHook", "AsyncDisplayPublisher", "AsyncInteractiveShell", "KernelInterruptError"]


class KernelInterruptError(InterruptedError):
    "Raised to interrupt the kernel."

    # We subclass from InterruptedError so if the backend is a SelectorEventLoop it can catch the exception.
    # Other event loops don't appear to have this issue.


class AsyncDisplayHook(DisplayHook):
    """
    A displayhook subclass that publishes data using [async_kernel.kernel.Kernel.iopub_send][].

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
    """A display publisher that publishes data using [async_kernel.kernel.Kernel.iopub_send][]."""

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
        - Supports a soft timeout specified via metadata `{"timeout":<value in seconds>}`[^1].
        - Gui event loops(tk, qt, ...) [are not presently supported][async_kernel.asyncshell.AsyncInteractiveShell.enable_gui].
        - Not all features are support (see "not-supported" features listed below).
        - `user_ns` and `user_global_ns` are same dictionary which is fixed.

        [^1]: When the execution time exceeds the timeout value, the code execution will "move on".
    """

    _execution_count = 0
    displayhook_class = Type(AsyncDisplayHook)
    display_pub_class = Type(AsyncDisplayPublisher)
    displayhook: Instance[AsyncDisplayHook]
    display_pub: Instance[AsyncDisplayPublisher]
    compiler_class = Type(XCachingCompiler)
    compile: Instance[XCachingCompiler]
    kernel = Fixed(lambda _: utils.get_kernel())
    user_ns_hidden: Fixed[Self, dict] = Fixed(lambda c: c["owner"]._get_default_ns())
    user_global_ns: Fixed[Self, dict] = Fixed(lambda c: c["owner"]._user_ns)  # pyright: ignore[reportIncompatibleMethodOverride]
    _user_ns: Fixed[Self, dict] = Fixed(dict)  # pyright: ignore[reportIncompatibleVariableOverride]
    _main_mod_cache = Fixed(dict)

    _stop_on_error_info: Fixed[Self, dict[Literal["time", "execution_count"], Any]] = Fixed(dict)
    "Details associated with the latest stop on error."

    execute_request_timeout = CFloat(default_value=None, allow_none=True)
    "A timeout in seconds to complete [execute requests][async_kernel.asyncshell.AsyncInteractiveShell.execute_request]."

    run_cell: Callable[..., ExecutionResult] = None  # pyright: ignore[reportAssignmentType]
    "**Not supported** -  use [execute_request][async_kernel.asyncshell.AsyncInteractiveShell.execute_request] instead."
    should_run_async = None  # pyright: ignore[reportAssignmentType]
    loop_runner_map = None
    loop_runner = None
    autoindent = False
    debug = None
    "**Not supported - use the built in debugger instead.**"

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
        parent: Message | dict[str, Any] | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        ident: bytes | list[bytes] | None = None,
        received_time: float = 0.0,
        tags: tuple[Tags, ...] = (),
        **_ignored,
    ) -> Content:
        """Handle a [execute request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)."""

        if (received_time < self._stop_on_error_info.get("time", 0)) and not silent:
            return utils.error_to_content(RuntimeError("Aborting due to prior exception")) | {
                "execution_count": self._stop_on_error_info.get("execution_count", 0)
            }

        execution_count = self.execution_count
        if not (silent):
            execution_count = self._execution_count = self._execution_count + 1
            self.kernel.iopub_send(
                msg_or_type="execute_input",
                content={"code": code, "execution_count": execution_count},
                parent=parent,
                ident=ident,
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
                with anyio.fail_after(delay=utils.get_execute_request_timeout()):
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
            finally:
                self.events.trigger("post_execute")
                if not silent:
                    self.events.trigger("post_run_cell", result)

            self.kernel.interrupts.discard(cancel)
        if (err) and (
            (Tags.suppress_error in tags)
            or (isinstance(err, anyio.get_cancelled_exc_class()) and (utils.get_execute_request_timeout() is not None))
        ):
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
                self._stop_on_error_info["time"] = time.monotonic()
                self._stop_on_error_info["execution_count"] = execution_count
                self.log.info("An error occurred in a non-silent execution request")
        return content

    async def do_complete_request(self, code: str, cursor_pos: int | None = None, **_ignored) -> Content:
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
        s, e = completions[0].start, completions[0].end if completions else (cursor_pos, cursor_pos)
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

    async def inspect_request(self, code: str, cursor_position: int, detail_level: Literal[0, 1]) -> Content:
        """Handle a [inspect request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introspection)."""
        content = {"data": {}, "metadata": {}, "found": True}
        try:
            oname = token_at_cursor(code, cursor_position)
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
            if msg := utils.get_metadata().get(MetadataKeys.suppress_error_message, "⚠"):
                print(msg)
            return
        if utils.get_execute_request_timeout() is not None and etype is anyio.get_cancelled_exc_class():
            etype, evalue, stb = TimeoutError, "Cell execute timeout", []
        self.kernel.iopub_send(
            msg_or_type="error",
            content={"traceback": stb, "ename": str(etype.__name__), "evalue": str(evalue)},
        )

    @override
    def reset(self, new_session=True, aggressive=False):
        super().reset(new_session, aggressive)
        if new_session:
            self._execution_count = 0

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


class AsyncInteractiveSubshell(AsyncInteractiveShell):
    protected = traitlets.Bool()
    subshell_id: Fixed[Self, str] = Fixed(lambda _: str(uuid.uuid4()))
    user_global_ns: Fixed[Self, dict[Any, Any]] = Fixed(lambda c: c["owner"].kernel.shell.user_global_ns)  # pyright: ignore[reportIncompatibleVariableOverride]

    def stop(self, *, force=False) -> None:
        "Stop this subshell."
        if force or not self.protected:
            self.kernel.subshell_manager.subshells.pop(self.subshell_id, None)
            if hm := self.history_manager:
                hm.end_session()
                self.history_manager = None


class SubshellManager:
    kernel: Fixed[Self, Kernel] = Fixed(lambda _: utils.get_kernel())
    subshells: Fixed[Self, dict[str, AsyncInteractiveSubshell]] = Fixed(dict)

    def create_subshell(self, *, protected=False) -> str:
        subshell = AsyncInteractiveSubshell(parent=self.kernel.shell)
        subshell.protected = protected
        self.subshells[subshell.subshell_id] = subshell
        return subshell.subshell_id

    def list_subshells(self) -> list[str]:
        return list(self.subshells)

    if TYPE_CHECKING:

        @overload
        def get_shell(self, subshell_id: str) -> AsyncInteractiveSubshell: ...
        @overload
        def get_shell(self, subshell_id: None = ...) -> AsyncInteractiveShell: ...

    def get_shell(self, subshell_id: str | None = None) -> AsyncInteractiveShell | AsyncInteractiveSubshell:
        "Get a subshell or the main shell"
        return (self.subshells[subshell_id] if subshell_id else None) or self.kernel.main_shell

    def delete_subshell(self, subshell_id: str) -> None:
        "Stop a subshell unless it is protected"
        if subshell := self.subshells.get(subshell_id):
            subshell.stop()

    def stop_all_subshells(self, *, force=False) -> None:
        """Stop all current subshells.

        Args:
            force: Passed to [async_kernel.asyncshell.AsyncInteractiveSubshell.stop][].
        """
        for subshell in frozenset(self.subshells.values()):
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
        """Print the number of subshells and the `subshell_id` [ref](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#list-subshells).

        See also:
            - [async_kernel.utils.get_kernel][]
        """
        subshells = utils.get_kernel().subshell_manager.list_subshells()
        print(f"{len(subshells)} subshells: {subshells}")


InteractiveShellABC.register(AsyncInteractiveShell)
