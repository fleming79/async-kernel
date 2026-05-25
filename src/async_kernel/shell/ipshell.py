from __future__ import annotations

import builtins
import json
import pathlib
import shlex
import sys
import threading
import time
import weakref
from collections.abc import Callable
from contextlib import contextmanager
from sqlite3 import OperationalError
from typing import TYPE_CHECKING, Any, Literal, Never, Self, TextIO, cast

import anyio
import IPython.core.release
from aiologic.lowlevel import async_checkpoint
from anyio.streams.text import TextReceiveStream
from IPython.core.completer import provisionalcompleter, rectify_completions
from IPython.core.displayhook import DisplayHook
from IPython.core.displaypub import DisplayPublisher
from IPython.core.extensions import ExtensionManager
from IPython.core.formatters import DisplayFormatter
from IPython.core.history import HistoryManager, HistorySavingThread
from IPython.core.interactiveshell import ExecutionResult, InteractiveShell
from IPython.core.interactiveshell import _modified_open as _modified_open_  # pyright: ignore[reportPrivateUsage]
from IPython.core.magic import Magics, line_cell_magic, line_magic, magics_class, no_var_expand
from IPython.core.prefilter import PrefilterManager
from IPython.display import display
from IPython.utils.ipstruct import Struct
from IPython.utils.tokenutil import token_at_cursor
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import traitlets
from typing_extensions import override

import async_kernel
from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.compiler import XCachingCompiler
from async_kernel.event_loop.run import get_runtime_matplotlib_guis
from async_kernel.interface.base import BaseInterface, HasInterface
from async_kernel.shell.base import BaseShell
from async_kernel.typing import Content, ExecuteContent, Job, RunMode, Tags

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CodeType

    from anyio.abc import ByteReceiveStream
    from IPython.core.interactiveshell import ExecutionResult

    from async_kernel.pending import Pending
    from async_kernel.shell import IPShell
    from async_kernel.typing import Message


__all__ = ["IPShell"]


class MethodNotSupported(Exception):
    """
    This exception is used inside overridden methods to indicate that it
    should not be used.
    """


class NullContext:
    __slots__ = ["weakref"]

    def __enter__(self) -> None:
        return

    def __exit__(self, type, value, traceback) -> Literal[False]:
        return False


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
        hist_file = ":memory:" if not shell.is_mainshell or sys.platform == "emscripten" else ""

        super(HistoryManager, self).__init__(hist_file=hist_file)

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


class IPDisplayFormatter(HasInterface, DisplayFormatter):
    pass


class IPPrefilterManager(HasInterface, PrefilterManager):
    pass


class IPExtensionManager(HasInterface, ExtensionManager):
    shell: IPShell

    def __init__(self, *, shell: IPShell) -> None:
        super().__init__(shell=shell)


class IPShell(BaseShell, InteractiveShell):  # pyright: ignore[reportUnsafeMultipleInheritance, reportIncompatibleVariableOverride, reportIncompatibleMethodOverride]
    """
    An IPython InteractiveShell implementation.
    """

    timeout = traitlets.CFloat(0.0).tag(config=True)
    "A timeout in seconds to complete execute requests."

    stop_on_error_time_offset = traitlets.Float(0.0).tag(config=True)
    "An offset to add to the cancellation time to catch late arriving execute requests."

    default_matplotlib_backends = traitlets.List(["inline", "ipympl"]).tag(config=True)
    ""
    compiler_class = traitlets.Type(XCachingCompiler).tag(config=True)
    ""
    prefilter_manager_class = traitlets.Type(IPPrefilterManager).tag(config=True)
    ""
    displayhook_class = traitlets.Type(IPDisplayHook).tag(config=True)
    ""
    display_pub_class = traitlets.Type(IPDisplayPublisher).tag(config=True)
    ""
    display_formatter_class = traitlets.Type(IPDisplayFormatter).tag(config=True)
    ""

    configurables = Fixed(list)
    "Not used. Provided for compatibility."

    compile: Fixed[Self, XCachingCompiler] = Fixed(lambda c: c["owner"].compiler_class())
    "The compiler: provides a filename for a selection of code (cell)."

    prefilter_manager: Fixed[Self, IPPrefilterManager] = Fixed(
        lambda c: c["owner"].prefilter_manager_class(shell=c["owner"])
    )
    ""
    extension_manager: Fixed[Self, IPExtensionManager] = Fixed(lambda c: IPExtensionManager(shell=c["owner"]))
    "A manager for loading extensions."

    builtin_trap = Fixed(NullContext)
    "A nullcontext. We leave the builtins constant once set."

    displayhook: Fixed[Self, IPDisplayHook] = Fixed(lambda c: c["owner"].displayhook_class(c["owner"]))  # pyright: ignore[reportIncompatibleMethodOverride]
    """An implementation of [sys.displayhook][]. 
    
    The interface provides the displayhook.
    """

    display_pub: Fixed[Self, IPDisplayPublisher] = Fixed(lambda c: c["owner"].display_pub_class())
    "Used in [IPython.display.publish_display_data][]."

    display_formatter: Fixed[Self, IPDisplayFormatter] = Fixed(lambda c: c["owner"].display_formatter_class())
    """
    An object capable of transforming python objects to MIME content.
    
    Notes:
        - Primarily used in [IPython.core.interactiveshell.InteractiveShell.user_expressions][].
        - [Ipython docs](https://ipython.readthedocs.io/en/stable/config/shell_mimerenderer.html).
    """

    history_manager: Fixed[Self, IPHistoryManager] = Fixed(lambda c: IPHistoryManager(shell=c["owner"]), mode="ignore")
    ""

    meta = Fixed(Struct)
    tempfiles = Fixed(list, mode="ignore")
    tempdirs = Fixed(list, mode="ignore")

    _main_mod_cache = Fixed(dict)
    _stop_on_error_pool: Fixed[Self, set[Callable[[], object]]] = Fixed(set)

    # Disabled attributes
    loop_runner_map = None
    loop_runner = None
    autoindent = False
    call_pdb = Fixed(lambda _: None, mode="ignore")
    trio_runner = None

    # Disabled methods
    @override
    def init_prefilter(self) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def init_create_namespaces(self, user_module=None, user_ns=None) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def save_sys_module_state(self) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def init_sys_modules(self) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def init_history(self) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def init_encoding(self) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def init_user_ns(self) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def init_instance_attrs(self) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def init_payload(self) -> Never:
        raise MethodNotSupported  # pragma: no cover

    @override
    def initialize(self):
        # Expects the kernel to be started.
        super().initialize()

        self.init_ipython_dir(None)
        self.init_profile_dir(None)

        self.init_syntax_highlighting()
        self.init_hooks()
        self.init_events()
        self.init_pushd_popd_magic()
        self.init_logger()

        self.init_completer()

        self.init_traceback_handlers(((), None))
        self.init_prompts()

        self.init_magics()
        self.init_alias()
        self.init_logstart()

        self.user_ns_hidden.update(self.user_ns)
        self.events.trigger("shell_initialized", self)

    @override
    def apply_patches(self) -> Callable[[], None]:
        """Apply patches returning a callable to reverse the patches."""
        original = InteractiveShell.initialized, InteractiveShell.instance
        InteractiveShell.initialized = lambda: True
        InteractiveShell.instance = utils.get_ipython  # pyright: ignore[reportAttributeAccessIssue]

        restore_ = super().apply_patches()

        def restore():
            InteractiveShell.initialized, InteractiveShell.instance = original
            restore_()

        return restore

    @override
    def _get_default_ns(self) -> dict[str, Any]:
        # Copied from `InteractiveShell.init_user_ns`
        history = self.history_manager
        return {
            "_ih": getattr(history, "input_hist_parsed", False),
            "_oh": getattr(history, "output_hist", None),
            "_dh": getattr(history, "dir_hist", "."),
            "In": getattr(history, "input_hist_parsed", False),
            "Out": getattr(history, "output_hist", False),
            "exit": self.exiter,
            "quit": self.exiter,
            "open": _modified_open_,
            "_": "",
            "__": "",
            "___": "",
        }

    @property
    @override
    def banner(self) -> str:
        return super(BaseShell, self).banner

    @traitlets.default("banner1")
    def _default_banner1(self) -> str:
        kernel_info = (
            f"async-kernel v{async_kernel.__version__} name:{self.parent.name!r} backend:{str(self.parent.backend)!r}"
        )
        return f"Python {sys.version}\n{kernel_info}\nIPython shell {IPython.core.release.version}\n"

    @traitlets.observe("exit_now")
    def _update_exit_now(self, _) -> None:
        """Stop eventloop when `exit_now` fires."""
        if self.exit_now:
            self.parent.stop()

    def ask_exit(self) -> None:
        if self.kernel.raw_input("Are you sure you want to stop the kernel?\ny/[n]\n") == "y":
            self.exit_now = True

    @override
    def init_builtins(self) -> None:
        super().init_builtins()
        if self.is_mainshell:
            builtins.__dict__["__IPYTHON__"] = True
            builtins.__dict__["display"] = display
            builtins.__dict__["get_ipython"] = utils.get_ipython
            builtins.__dict__["Caller"] = Caller

    @override
    def init_hooks(self) -> None:
        """Initialize hooks."""
        super().init_hooks()

        def _show_in_pager(self, data: str | dict, start=0, screen_lines=0, pager_cmd=None) -> None:
            "Handle IPython page calls"
            if isinstance(data, dict):
                self.parent.iopub_send("display_data", content=data)
            else:
                self.parent.iopub_send("stream", content={"name": "stdout", "text": data})

        self.set_hook("show_in_pager", _show_in_pager, 99)

    @contextmanager
    @override
    def _tee(self, channel: Literal["stdout", "stderr"]):
        yield

    @property
    @override
    def display_trap(self):
        show = True
        if msg := utils.get_parent_message():
            if "silent" in msg["content"]:
                show = not msg["content"]["silent"]
            if show and (code := msg["content"].get("code")) and str(code).strip().endswith(";"):
                show = False
        return utils.show_result(show)

    @property
    @override
    def ns_table(self) -> dict[str, dict[Any, Any] | dict[str, Any]]:
        return {"user_global": self.user_global_ns, "user_local": self.user_ns, "builtin": builtins.__dict__}

    async def run_line_magic_async(self, magic_name: str, line: str, _stack_depth=1) -> Any:
        "Call and awaits [run_line_magic][IPython.core.interactiveshell.InteractiveShell.run_line_magic]."
        async with Caller().create_pending_group(mode=1):
            result = self.run_line_magic(magic_name, line, _stack_depth)
            try:
                return await result  # pyright: ignore[reportGeneralTypeIssues]
            except TypeError:
                return result

    async def run_cell_magic_async(self, magic_name: str, line: str, cell: str) -> Any:
        "Call and awaits [run_cell_magic][IPython.core.interactiveshell.InteractiveShell.run_cell_magic]."
        async with Caller().create_pending_group(mode=1):
            result = self.run_cell_magic(magic_name, line, cell)
            try:
                return await result  # pyright: ignore[reportGeneralTypeIssues]
            except TypeError:
                return result

    @override
    def system(self, cmd: list[str] | str, *, stderr_to_stdout: bool = False, **kwargs: Any) -> Pending[None]:
        """
        Make a system call in a separate thread.

        Args:
            cmd: Passed as the first argument 'command' when calling [anyio.open_process][].
            stderr_to_stdout: Send stderr output to stdout.
            **kwargs: Keyword arguments are passed to [anyio.open_process][].

        Tip:
            - The output can be redicted by making the call in the context of
                [async_kernel.utils.redirect_stdout][] and/or [async_kernel.utils.redirect_stderr][].
        """

        async def forward_output(transport_stream: ByteReceiveStream | None, out: TextIO, /) -> None:
            if transport_stream:
                async for text in TextReceiveStream(transport_stream):
                    out.write(text)

        async def open_process() -> None:
            async with await anyio.open_process(cmd, **kwargs) as process, anyio.create_task_group() as tg:
                tg.start_soon(forward_output, process.stdout, sys.stdout)
                tg.start_soon(forward_output, process.stderr, sys.stdout if stderr_to_stdout else sys.stderr)

        return Caller().to_thread(open_process)

    @override
    async def run_code(
        self, code_obj: CodeType, result: ExecutionResult | None = None, *, async_: bool = False
    ) -> bool:
        """Execute a code object.

        When an exception occurs, self.showtraceback() is called to display a traceback.

        Args:
            code_obj: A compiled code object, to be executed.
            result: An object to store exceptions that occur during execution.
            async_:  (Experimental) Attempt to run top-level asynchronous code in a default loop.

        Returns:
            False: successful execution.
            True: an error occurred.
        """
        try:
            if async_:
                await eval(code_obj, self.user_global_ns, self.user_ns)
            else:
                exec(code_obj, self.user_global_ns, self.user_ns)
        except self.custom_exceptions:
            etype, value, tb = sys.exc_info()
            if result is not None:
                result.error_in_exec = value
            self.CustomTB(etype, value, tb)
        except Exception as e:
            if result is not None:
                result.error_in_exec = e
            self.showtraceback(running_compiled_code=True)
        else:
            return False
        return True

    def transform_cell_async(self, raw_cell: str) -> str:
        "Transform the cell and substitute magic calls with an awaitable wrapper."

        return (
            self.transform_cell(raw_cell)
            .replace("get_ipython().run_line_magic(", "await get_ipython().run_line_magic_async(")
            .replace("get_ipython().run_cell_magic(", "await get_ipython().run_cell_magic_async(")
            .replace("get_ipython().system(", "await get_ipython().system(")
        )

    @override
    async def execute_request(self, job: Job[ExecuteContent]) -> Content:
        """Handle a [execute request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)."""

        content = cast("ExecuteContent", job["msg"]["content"])
        code = content["code"]
        silent = content.get("silent", False)
        cell_id = job["msg"]["metadata"].get("cellId")
        stop_on_error = content.get("stop_on_error", False)

        if (job["received_time"] < self._stop_on_error_info.get("time", 0)) and not silent:
            return utils.error_to_content(RuntimeError("Aborting due to prior exception")) | {
                "execution_count": self._stop_on_error_info.get("execution_count", 0)
            }
        token = utils._cell_id_var.set(cell_id)  # pyright: ignore[reportPrivateUsage]
        try:
            tags: list[str] = utils.get_tags()
            timeout: float = utils.get_timeout(tags=tags)
            raises_exception: bool = Tags.raises_exception in tags
            stop_on_error_override: bool = Tags.stop_on_error in tags
            if stop_on_error_override:
                stop_on_error = utils.get_tag_value(Tags.stop_on_error, stop_on_error)
            elif raises_exception:
                stop_on_error = False

            if silent:
                execution_count: int = self.execution_count
            else:
                execution_count = self._execution_count = self._execution_count + 1
                self.parent.iopub_send(
                    msg_or_type="execute_input",
                    content={"code": code, "execution_count": execution_count},
                    ident=b"kernel.execute_input",
                )
            caller = Caller()
            err = None
            with anyio.CancelScope() as scope:

                def cancel():
                    if not silent:
                        caller.call_direct(scope.cancel, "Interrupted")

                result = None
                try:
                    self._interrupts.add(cancel)
                    if stop_on_error:
                        self._stop_on_error_pool.add(cancel)
                    with anyio.fail_after(delay=timeout or None):
                        result = await self.run_cell_async(
                            raw_cell=code,
                            store_history=content.get("store_history", False),
                            silent=silent,
                            transformed_cell=self.transform_cell_async(code),
                            shell_futures=True,
                            cell_id=cell_id,
                        )
                except (Exception, anyio.get_cancelled_exc_class()) as e:
                    # A safeguard to catch exceptions not caught by the shell.
                    if utils.LAUNCHED_BY_DEBUGPY_PYTEST:
                        raise
                    err = KernelInterrupt() if self.parent.last_interrupt_frame else e
                else:
                    err = result.error_before_exec or result.error_in_exec if result else KernelInterrupt()
                    if not err and Tags.raises_exception in tags:
                        msg = "An expected exception was not raised!"
                        err = RuntimeError(msg)
                finally:
                    self._stop_on_error_pool.discard(cancel)
                    self._interrupts.discard(cancel)
                    self.events.trigger("post_execute")
                    if not silent:
                        self.events.trigger("post_run_cell", result)
            if (err) and (isinstance(err, anyio.get_cancelled_exc_class()) and (timeout != 0)):
                # Suppress the error due to either:
                # 1. tag
                # 2. timeout
                err = None
            content = {
                "status": "error" if err else "ok",
                "execution_count": execution_count,
                "user_expressions": self.user_expressions(content.get("user_expressions", {})),
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
                                await async_checkpoint(force=True)
            return content
        finally:
            utils._cell_id_var.reset(token)  # pyright: ignore[reportPrivateUsage]

    @override
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

    @override
    async def is_complete_request(self, code: str) -> Content:
        """Handle an [is_complete request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness)."""
        status, indent_spaces = self.input_transformer_manager.check_complete(code)
        content = {"status": status}
        if status == "incomplete":
            content["indent"] = " " * indent_spaces
        return content

    @override
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

    @override
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
        if utils.get_timeout() != 0.0 and etype is anyio.get_cancelled_exc_class():
            etype, evalue, stb = TimeoutError, "Cell execute timeout", []
        if isinstance(evalue, KernelInterrupt):
            stb = []
        self.parent.iopub_send(
            msg_or_type="error",
            content={"traceback": stb, "ename": str(etype.__name__), "evalue": str(evalue)},
        )

    @override
    def init_magics(self) -> None:
        """Initialize magics."""
        super().init_magics()
        self.register_magics(KernelMagics)
        # Line magics
        self.magics_manager.register_alias("!", "system")

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
        return [*get_runtime_matplotlib_guis(), *self.default_matplotlib_backends]

    @override
    def stop(self, *, force=False) -> None:
        "Stop the shell - do not call directly."

        if self.protected and not force:
            return
        self.parent.kernel._subshell_stopped(self)  # pyright: ignore[reportPrivateUsage]
        self.configurables.clear()
        self.user_ns.clear()
        self.history_manager.stop()
        try:
            self.atexit_operations()
        except AttributeError:
            pass


@magics_class
class KernelMagics(HasInterface[BaseInterface[IPShell]], Magics):
    """Extra magics for async-kernel."""

    @line_magic
    def connect_info(self, _) -> None:
        """Print information for connecting other clients to this kernel."""
        if isinstance(f := getattr(self.parent, "connection_file", None), pathlib.Path) and f.exists():
            connection_file = f
            # if it's in the default dir, truncate to basename
            if jupyter_runtime_dir() == str(connection_file.parent):
                connection_file = connection_file.name
            info = json.loads(f.read_bytes()) if f.exists() else ""
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
        else:
            print("No connection info")  # pragma: no cover

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
        subshells = list(self.parent.kernel.subshells)
        subshell_list = (
            f"\t----- {len(subshells)} x subshell -----\n" + "\n".join(subshells) if subshells else "-- No subshells --"
        )
        print(f"Current shell:\t{self.shell}\n\n{subshell_list}")

    @no_var_expand
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
            await self.parent.kernel.get_shell().system([sys.executable, "-m", "pip", *line.split()])
        return None

    @no_var_expand
    @line_magic
    def uv(self, line) -> Pending[None]:
        """Run the uv package manager for the current environment.

        Usage:
          %uv pip install [pkgs]
        """
        cmd = ["uv", *shlex.split(line or "-h")]
        if "--color" not in line:
            cmd.extend(["--color", "always"])
        return self.parent.kernel.get_shell().system(cmd, stderr_to_stdout=True)

    @no_var_expand
    @line_cell_magic
    async def thread(self, line: str, cell: str | None = None) -> None:
        """
        Run the python code in a caller managed child thread.

        Both line and cell magic are supported.

        For cell_magic, [CallerCreateOptions][async_kernel.typing.CallerCreateOptions] can be passed as literals.

        Example:
            %%thread name="Trio executor" backend=trio
        """
        shell = self.parent.kernel.get_shell()
        if cell is None:
            cell = line
            options: Any = None
        else:
            options = RunMode.line_to_options(line)
        caller = shell.kernel.caller
        await (caller.get(**options).call_soon if options else caller.to_thread)(
            shell.run_cell_async,
            raw_cell=cell,
            store_history=False,
            silent=False,
            cell_id=None,
            transformed_cell=shell.transform_cell_async(cell),
        )

    async def _call_using_backend(self, backend: Literal["asyncio", "trio"], code: str) -> None:
        shell = self.parent.kernel.get_shell()
        await Caller().call_using_backend(
            backend,
            shell.run_cell_async,
            raw_cell=code,
            store_history=False,
            silent=False,
            cell_id=None,
            transformed_cell=shell.transform_cell_async(code),
        )

    @no_var_expand
    @line_cell_magic
    async def asyncio(self, line: str, cell: str | None = None) -> None:
        ""
        await self._call_using_backend("asyncio", cell or line)

    @no_var_expand
    @line_cell_magic
    async def trio(self, line: str, cell: str | None = None) -> None:
        ""
        await self._call_using_backend("trio", cell or line)
