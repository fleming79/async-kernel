from __future__ import annotations

import builtins
import contextlib
import json
import os
import pathlib
import sys
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, Never, Self, TextIO, final

import anyio
import IPython.core.release
from aiologic.lowlevel import async_checkpoint
from IPython.core.completer import provisionalcompleter, rectify_completions
from IPython.core.displayhook import DisplayHook
from IPython.core.displaypub import DisplayPublisher
from IPython.core.history import HistoryManager
from IPython.core.interactiveshell import InteractiveShell
from IPython.core.interactiveshell import _modified_open as _modified_open_  # pyright: ignore[reportPrivateUsage]
from IPython.core.magic import Magics, line_cell_magic, line_magic, magics_class
from IPython.utils.tokenutil import token_at_cursor
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import CFloat, Float, Instance, Type, default, observe, traitlets
from typing_extensions import override

import async_kernel
from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed, KernelInterrupt, import_item
from async_kernel.compiler import XCachingCompiler
from async_kernel.debugger import Debugger
from async_kernel.event_loop.run import get_runtime_matplotlib_guis
from async_kernel.pending import PendingManager
from async_kernel.typing import Channel, Content, Message, NoValue, Tags

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from anyio.abc import ByteReceiveStream

    from async_kernel import Kernel
    from async_kernel.debugger import Debugger


__all__ = ["AsyncInteractiveShell"]


async def _forward_transport_stream(transport_stream: ByteReceiveStream, out: TextIO, /) -> None:
    from anyio.streams.text import TextReceiveStream  # noqa: PLC0415

    async for text in TextReceiveStream(transport_stream):
        out.write(text)


class AsyncDisplayHook(DisplayHook):
    """
    A displayhook subclass that publishes data using [iopub_send][async_kernel.kernel.Kernel.iopub_send].
    """

    shell: AsyncInteractiveShell
    _content: Fixed[Self, dict[int, dict[str, Any]]] = Fixed(dict)

    @property
    @override
    def prompt_count(self) -> int:
        return self.shell.execution_count

    @override
    def start_displayhook(self) -> None:
        """Start the display hook."""
        self._content[id(utils.get_job())] = {}

    @property
    def content(self) -> dict[str, Any]:
        return self._content[id(utils.get_job())]

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
        if content := self.content:
            self.shell.kernel.iopub_send("execute_result", content=content)
        self._content.pop(id(utils.get_job()))


class AsyncDisplayPublisher(DisplayPublisher):
    """A display publisher that publishes data using [iopub_send][async_kernel.kernel.Kernel.iopub_send]."""

    shell: AsyncInteractiveShell
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
        msg = self.shell.kernel.interface.msg(msg_type, content=content, parent=utils.get_parent())
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
            self.shell.kernel.callers[Channel.control].call_later(0.2, self.shell.kernel.iopub_send, msg)
        else:
            self.shell.kernel.iopub_send(msg)

    @override
    def clear_output(self, wait: bool = False) -> None:
        """
        Clear output associated with the current execution (cell).

        Args:
            wait: If True, the output will not be cleared immediately,
                instead waiting for the next display before clearing.
                This reduces bounce during repeated clear & display loops.
        """
        self.shell.kernel.iopub_send(msg_or_type="clear_output", content={"wait": wait}, ident=b"display_data")

    def register_hook(self, hook: Callable[[Message[Any]], Any]) -> None:
        """Register a hook for when publish is called.

        The hook should return the message or None.
        Only return `None` when the message should *not* be sent.
        """
        self._hooks.append(hook)

    def unregister_hook(self, hook: Callable[[Message[Any]], Any]) -> None:
        while hook in self._hooks:
            self._hooks.remove(hook)


class ShellPendingManager(PendingManager):
    "A pending manager to track the active shell/subshell."


class AsyncInteractiveShell(InteractiveShell):
    """
    An IPython InteractiveShell adapted to work with [async-kernel][async_kernel.kernel.Kernel].

    Notable differences:
        - Supports a soft timeout specified via tags `timeout=<value in seconds>`[^1].
        - `user_ns` and `user_global_ns` are same dictionary which is a fixed [dict][].
        - Supports async magic functions (See [KernelMagics.pip][]).

        [^1]: When the execution time exceeds the timeout value, the code execution will "move on".
    """

    DEFAULT_MATPLOTLIB_BACKENDS = ["inline", "ipympl"]

    _execution_count = 0
    _resetting = False
    displayhook_class = Type(AsyncDisplayHook)
    display_pub_class = Type(AsyncDisplayPublisher)
    displayhook: Instance[AsyncDisplayHook]
    display_pub: Instance[AsyncDisplayPublisher]
    history_manager: HistoryManager
    compiler_class = Type(XCachingCompiler)
    compile: Instance[XCachingCompiler]
    kernel: Instance[Kernel] = Instance("async_kernel.Kernel", (), read_only=True)

    pending_manager = Fixed(ShellPendingManager)
    subshell_id = Fixed(lambda _: None)

    debugger: Fixed[Self, Debugger | None] | None = None  # pyright: ignore[reportIncompatibleMethodOverride]
    "Handles [debug requests](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request)."

    user_ns_hidden: Fixed[Self, dict] = Fixed(lambda c: c["owner"]._get_default_ns())
    user_global_ns: Fixed[Self, dict] = Fixed(lambda c: c["owner"]._user_ns)  # pyright: ignore[reportIncompatibleMethodOverride]

    _user_ns: Fixed[Self, dict] = Fixed(dict)  # pyright: ignore[reportIncompatibleVariableOverride]
    _main_mod_cache = Fixed(dict)
    _stop_on_error_pool: Fixed[Self, set[Callable[[], object]]] = Fixed(set)
    _stop_on_error_info: Fixed[Self, dict[Literal["time", "execution_count"], Any]] = Fixed(dict)

    timeout = CFloat(0.0)
    "A timeout in seconds to complete execute requests."

    stop_on_error_time_offset = Float(0.0)
    "An offset to add to the cancellation time to catch late arriving execute requests."

    loop_runner_map = None
    loop_runner = None
    autoindent = False

    @override
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}  kernel_name: {self.kernel.kernel_name!r} subshell_id: {self.subshell_id}>"

    def __new__(cls) -> Self:
        if issubclass(cls, AsyncInteractiveSubshell):
            return super().__new__(cls)
        if not InteractiveShell._instance:
            if cls is AsyncInteractiveShell._cls:
                return super().__new__(cls)
            InteractiveShell._instance = AsyncInteractiveShell._cls()  # pyright: ignore[reportAttributeAccessIssue]
            InteractiveShell.instance = SubshellManager.get_shell
        return InteractiveShell._instance  # pyright: ignore[reportReturnType]

    @override
    def __init__(self, parent=None) -> None:  # pyright: ignore[reportInconsistentConstructor]
        if not hasattr(self, "configurables"):
            super().__init__(parent=parent)

    def _get_default_ns(self) -> dict[str, Any]:
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

    def __init_subclass__(cls) -> None:
        try:
            if not issubclass(cls, AsyncInteractiveSubshell):
                AsyncInteractiveShell._cls = cls
        except NameError:
            pass
        return super().__init_subclass__()

    @default("banner1")
    def _default_banner1(self) -> str:
        return (
            f"Python {sys.version}\n"
            f"async-kernel v{async_kernel.__version__}, {self.kernel.settings}) \n"
            f"IPython shell {IPython.core.release.version}\n"
        )

    @observe("exit_now")
    def _update_exit_now(self, _) -> None:
        """Stop eventloop when `exit_now` fires."""
        if self.exit_now:
            self.kernel.stop()

    def ask_exit(self) -> None:
        if self.kernel.interface.raw_input("Are you sure you want to stop the kernel?\ny/[n]\n") == "y":
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

    @override
    def init_hooks(self) -> None:
        """Initialize hooks."""
        super().init_hooks()

        def _show_in_pager(self, data: str | dict, start=0, screen_lines=0, pager_cmd=None) -> None:
            "Handle IPython page calls"
            if isinstance(data, dict):
                self.kernel.interface.iopub_send("display_data", content=data)
            else:
                self.kernel.interface.iopub_send("stream", content={"name": "stdout", "text": data})

        self.set_hook("show_in_pager", _show_in_pager, 99)

    @override
    def init_history(self) -> None:
        if self.subshell_id is None:
            self.history_manager = HistoryManager(shell=self, parent=self)
            self.configurables.append(self.history_manager)  # pyright: ignore[reportArgumentType]
            utils.mark_thread_pydev_do_not_trace(self.history_manager.save_thread)
        else:
            self.history_manager = HistoryManager(shell=self, parent=self, hist_file=":memory:")
            self.history_manager.output_hist.update(self.kernel.main_shell.history_manager.output_hist)

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

    @property
    def supported_features(self) -> list[str]:
        "Supported features included in the reply to a [async_kernel.kernel.Kernel.kernel_info_request][]."
        features = ["kernel subshells"]
        if self.debugger:
            features.append("debugger")
        return features

    async def run_line_magic_async(self, magic_name: str, line: str, _stack_depth=1) -> Any:
        "Call and awaits [run_line_magic][IPython.core.interactiveshell.InteractiveShell.run_line_magic]."
        result = self.run_line_magic(magic_name, line, _stack_depth)
        try:
            return await result  # pyright: ignore[reportGeneralTypeIssues]
        except TypeError:
            return result

    async def run_cell_magic_async(self, magic_name: str, line: str, cell: str) -> Any:
        "Call and awaits [run_cell_magic][IPython.core.interactiveshell.InteractiveShell.run_cell_magic]."
        result = self.run_cell_magic(magic_name, line, cell)
        try:
            return await result  # pyright: ignore[reportGeneralTypeIssues]
        except TypeError:
            return result

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
        if (received_time < self._stop_on_error_info.get("time", 0)) and not silent:
            return utils.error_to_content(RuntimeError("Aborting due to prior exception")) | {
                "execution_count": self._stop_on_error_info.get("execution_count", 0)
            }
        token = utils._cell_id_var.set(cell_id)  # pyright: ignore[reportPrivateUsage]
        try:
            tags: list[str] = utils.get_tags()
            timeout: float = utils.get_timeout(tags=tags)
            suppress_error: bool = Tags.suppress_error in tags
            raises_exception: bool = Tags.raises_exception in tags
            stop_on_error_override: bool = Tags.stop_on_error in tags
            transformed_cell = (
                self.transform_cell(code)
                .replace("get_ipython().run_line_magic(", "await get_ipython().run_line_magic_async(")
                .replace("get_ipython().run_cell_magic(", "await get_ipython().run_cell_magic_async(")
            )
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
                    self.kernel.interface.interrupts.add(cancel)
                    if stop_on_error:
                        self._stop_on_error_pool.add(cancel)
                    with anyio.fail_after(delay=timeout or None):
                        result = await self.run_cell_async(
                            raw_cell=code,
                            store_history=store_history,
                            silent=silent,
                            transformed_cell=transformed_cell,
                            shell_futures=True,
                            cell_id=cell_id,
                        )
                except (Exception, anyio.get_cancelled_exc_class()) as e:
                    # A safeguard to catch exceptions not caught by the shell.
                    err = KernelInterrupt() if self.kernel.interface.last_interrupt_frame else e
                else:
                    err = result.error_before_exec or result.error_in_exec if result else KernelInterrupt()
                    if not err and Tags.raises_exception in tags:
                        msg = "An expected exception was not raised!"
                        err = RuntimeError(msg)
                finally:
                    self._stop_on_error_pool.discard(cancel)
                    self.kernel.interface.interrupts.discard(cancel)
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
                        self._stop_on_error_info["time"] = time.monotonic() + float(self.stop_on_error_time_offset)
                        self._stop_on_error_info["execution_count"] = execution_count
                        self.log.info("An error occurred in a non-silent execution request")
                        if stop_on_error:
                            for c in frozenset(self._stop_on_error_pool):
                                c()
            return content
        finally:
            utils._cell_id_var.reset(token)  # pyright: ignore[reportPrivateUsage]

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
        history_manager = self.history_manager
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
    def reset(self, new_session=True, aggressive=False) -> None:
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
        if (gui is not None) and (gui not in (supported := self._list_matplotlib_backends_and_gui_loops())):
            msg = f"The gui {gui!r} is not one of the supported gui options for this thread! {supported}="
            raise RuntimeError(msg)

    @override
    def enable_matplotlib(self, gui: str | None = None) -> tuple[str | Any | None, Any | str]:  # pragma: no cover
        """
        Enable interactive matplotlib and inline figure support.

        This takes the following steps:

        1. select the appropriate matplotlib backend
        2. set up matplotlib for interactive use with that backend
        3. configure formatters for inline figure display

        Args:
            gui:
                If given, dictates the choice of matplotlib GUI backend to use
                (should be one of IPython's supported backends, 'qt', 'osx', 'tk',
                'gtk', 'wx' or 'inline', 'ipympl'), otherwise we use the default chosen by
                matplotlib (as dictated by the matplotlib build-time options plus the
                user's matplotlibrc configuration file).  Note that not all backends
                make sense in all contexts, for example a terminal ipython can't
                display figures inline.
        """
        import matplotlib_inline.backend_inline  # noqa: PLC0415
        from IPython.core import pylabtools as pt  # noqa: PLC0415

        backends = self._list_matplotlib_backends_and_gui_loops()
        gui = gui or backends[0]
        gui, backend = pt.find_gui_and_backend(gui, self.pylab_gui_select)
        self.enable_gui(gui)
        try:
            pt.activate_matplotlib(backend)
        except RuntimeError as e:
            e.add_note(f"This thread supports the gui {gui!s} but pyplot only supports one interactive backend.")

        matplotlib_inline.backend_inline.configure_inline_support(self, backend)

        # Now we must activate the gui pylab wants to use, and fix %run to take
        # plot updates into account
        self.magics_manager.registry["ExecutionMagics"].default_runner = pt.mpl_runner(self.safe_execfile)

        return gui, backend

    def _list_matplotlib_backends_and_gui_loops(self) -> list[str | None]:
        return [*get_runtime_matplotlib_guis(), *self.DEFAULT_MATPLOTLIB_BACKENDS]

    @contextlib.contextmanager
    def context(self) -> Generator[None, Any, None]:
        "A context manager where the shell is active."
        with self.pending_manager.context():
            yield

    def stop(self, *, force=False) -> None:
        "Stop the shell - do not call directly."
        if force:
            self.reset(new_session=False)
            try:
                self.history_manager.end_session()
                self.history_manager.save_thread.stop()  # pyright: ignore[reportOptionalMemberAccess]
                self.history_manager.save_thread.join()  # pyright: ignore[reportOptionalMemberAccess]
            except AttributeError:
                pass
            SubshellManager.stop_all_subshells(force=True)
            InteractiveShell._instance = None


class AsyncInteractiveSubshell(AsyncInteractiveShell):
    """
    An asynchronous interactive subshell for managing isolated execution contexts within an async-kernel.

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
    subshell_id: Fixed[Self, str] = Fixed(lambda c: c["owner"].pending_manager.id)

    def __init_subclass__(cls) -> None:
        AsyncInteractiveSubshell._cls = cls
        return super().__init_subclass__()

    @override
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} kernel_name: {self.kernel.kernel_name!r}  subshell_id: {self.subshell_id}{'  stopped' if self.stopped else ''}>"

    @property
    @override
    def user_global_ns(self) -> dict:  # pyright: ignore[reportIncompatibleVariableOverride]
        return (
            self.kernel.main_shell.user_global_ns.copy() if self._resetting else self.kernel.main_shell.user_global_ns
        )

    @property
    def debugger(self) -> Debugger | None:  # pyright: ignore[reportIncompatibleVariableOverride, reportImplicitOverride]
        return AsyncInteractiveShell().debugger

    def __new__(cls, *, protected: bool = True) -> Self:  # noqa: ARG004
        if cls is AsyncInteractiveSubshell:
            return cls._cls()
        return super().__new__(cls)

    @override
    def __init__(self, *, protected: bool = True) -> None:

        super().__init__(parent=AsyncInteractiveShell())
        self.set_trait("protected", protected)
        self.stop_on_error_time_offset = self.kernel.main_shell.stop_on_error_time_offset
        SubshellManager.subshells[self.subshell_id] = self

    @override
    def stop(self, *, force=False) -> None:
        "Stop this subshell."
        if force or not self.protected:
            for pen in self.pending_manager.pending:
                pen.cancel(f"Subshell {self.subshell_id} is stopping.")
            self.reset(new_session=False)
            self.kernel._subshell_stopped(self.subshell_id)  # pyright: ignore[reportPrivateUsage]
            SubshellManager.subshells.pop(self.subshell_id, None)
            self.set_trait("stopped", True)


class IPythonAsyncInteractiveShell(AsyncInteractiveShell):
    "The default AsyncInteractiveShell"

    debugger = Fixed(
        lambda _: (
            import_item("async_kernel.debugger.Debugger")()
            if (not utils.LAUNCHED_BY_DEBUGPY) & (sys.platform != "emscripten")
            else None
        )
    )


class IPythonInteractiveSubshell(AsyncInteractiveSubshell):
    "The default AsyncInteractiveSubshell"


@final
class SubshellManager:
    """
    Manages all instances of [subshells][async_kernel.asyncshell.IPythonInteractiveSubshell].

    Note:

        - All methods are [classmethod][].
    """

    subshells: dict[str, AsyncInteractiveSubshell] = {}

    def __new__(cls) -> Never:
        msg = "Instantiation is not required, use classmethods directly."
        raise RuntimeError(msg)

    @classmethod
    def create_subshell(cls, *, protected: bool = True) -> AsyncInteractiveSubshell:
        """
        Create a new instance of the default subshell class.

        Call [`subshell.stop(force=True)`][async_kernel.asyncshell.IPythonInteractiveSubshell.stop] to stop a
        protected subshell when it is no longer required.

        Args:
            protected: Protect the subshell from accidental deletion.
        """
        return AsyncInteractiveSubshell(protected=protected)

    @classmethod
    def list_subshells(cls) -> list[str]:
        return list(cls.subshells)

    @classmethod
    def get_shell(
        cls,
        subshell_id: str | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
    ) -> AsyncInteractiveShell | AsyncInteractiveSubshell:
        """
        Get a subshell or the main shell.

        Args:
            subshell_id: The id of an existing subshell.
        """
        mainshell = AsyncInteractiveShell()
        if subshell_id is NoValue:
            subshell_id = ShellPendingManager.active_id()
        if subshell_id is None or subshell_id == mainshell.pending_manager.id:
            return mainshell
        return cls.subshells[subshell_id]

    @classmethod
    def delete_subshell(cls, subshell_id: str) -> None:
        """
        Stop a subshell unless it is protected.

        Args:
            subshell_id: The id of an existing subshell to stop.
        """
        if subshell := cls.subshells.get(subshell_id):
            subshell.stop()

    @classmethod
    def stop_all_subshells(cls, *, force: bool = False) -> None:
        """Stop all current subshells.

        Args:
            force: Passed to [async_kernel.asyncshell.IPythonInteractiveSubshell.stop][].
        """
        for subshell in set(cls.subshells.values()):
            subshell.stop(force=force)


@magics_class
class KernelMagics(Magics):
    """Extra magics for async-kernel."""

    shell: AsyncInteractiveShell  # pyright: ignore[reportIncompatibleVariableOverride]

    @line_magic
    def connect_info(self, _) -> None:
        """Print information for connecting other clients to this kernel."""
        connection_file = pathlib.Path(self.shell.kernel.connection_file)
        # if it's in the default dir, truncate to basename
        if jupyter_runtime_dir() == str(connection_file.parent):
            connection_file = connection_file.name
        info = self.shell.kernel.get_connection_info()
        print(
            json.dumps(info, indent=2),
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
        "Print a table of [Callers][async_kernel.caller.Caller] indicating it's status."
        callers = Caller.all_callers(running_only=False)
        n = max(len(c.name) for c in callers) + 6
        m = max(len(repr(c.id)) for c in callers) + 6
        t = max(len(str(c.thread.name)) for c in callers) + 6
        lines = [
            "".join(["Name".center(n), "Running ", "Protected", "Thread".center(t), "Caller".center(m)]),
            "─" * (n + m + t + 22),
        ]
        for caller in callers:
            running = ("✓" if caller.running else "✗").center(8)
            protected = "   🔐    " if caller.protected else "         "
            name = caller.name + " " * (n - len(caller.name))
            thread = str(caller.thread.name).center(t)
            caller_id = str(caller.id)
            if caller.id == Caller.id_current():
                caller_id += " ← current"
            lines.append("".join([name, running.center(8), protected, thread, caller_id]))
        print(*lines, sep="\n")

    @line_magic
    def subshell(self, _) -> None:
        """
        Print subshell info [ref](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#list-subshells).
        """
        subshells = SubshellManager.list_subshells()
        subshell_list = (
            f"\t----- {len(subshells)} x subshell -----\n" + "\n".join(subshells) if subshells else "-- No subshells --"
        )
        print(f"Current shell:\t{self.shell}\n\n{subshell_list}")

    @line_magic
    async def pip(self, line: str) -> Any | None:
        """Run the pip package manager for the current environment.

        Usage:
          %pip install [pkgs]
        """
        if sys.platform == "emscripten":
            import micropip  # noqa: PLC0415

            match line.split(maxsplit=1)[0]:
                case "install":
                    requirements = [
                        f"emfs:{n}" if n.startswith(".") else n for n in line.removeprefix("install").split()
                    ]
                    return await micropip.install(requirements, verbose=True)
                case "uninstall":
                    return micropip.uninstall(line.removeprefix("uninstall").split(), verbose=True)
                case "freeze":
                    return micropip.freeze()
                case "list":
                    return micropip.list()
                case _ as name:
                    print("Unsupported command:", name)
        else:
            cmd = [sys.executable, "-m", "pip", *line.split()]
            async with await anyio.open_process(cmd) as process, anyio.create_task_group() as tg:
                if process.stdout:
                    tg.start_soon(_forward_transport_stream, process.stdout, sys.stdout)
                if process.stderr:
                    tg.start_soon(_forward_transport_stream, process.stderr, sys.stderr)

        return None

    @line_magic
    async def uv(self, line) -> None:
        """Run the uv package manager for the current environment.

        Usage:
          %uv pip install [pkgs]
        """
        cmd = ["uv", *line.split()]
        async with await anyio.open_process(cmd) as process, anyio.create_task_group() as tg:
            if process.stdout:
                tg.start_soon(_forward_transport_stream, process.stdout, sys.stdout)
            if process.stderr:
                tg.start_soon(_forward_transport_stream, process.stderr, sys.stdout)

    @line_cell_magic
    async def python(self, line: str, cell: str) -> None:
        """
        Run python code.

        Useful only when the primary language is not Python.
        """
        shell = SubshellManager.get_shell()
        await shell.run_cell_async(raw_cell=line or cell, store_history=False, silent=True, cell_id=utils.get_cell_id())
