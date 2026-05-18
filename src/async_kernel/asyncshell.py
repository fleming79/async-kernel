from __future__ import annotations

import builtins
import contextlib
import json
import pathlib
import shlex
import sys
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, TextIO

import anyio
import IPython.core.release
from aiologic.lowlevel import async_checkpoint
from anyio.streams.text import TextReceiveStream
from IPython.core.builtin_trap import builtin_mod  # pyright: ignore[reportPrivateImportUsage]
from IPython.core.completer import provisionalcompleter, rectify_completions
from IPython.core.getipython import get_ipython
from IPython.core.interactiveshell import InteractiveShell
from IPython.core.interactiveshell import _modified_open as _modified_open_  # pyright: ignore[reportPrivateUsage]
from IPython.core.magic import Magics, line_cell_magic, line_magic, magics_class, no_var_expand
from IPython.display import display
from IPython.utils.tokenutil import token_at_cursor
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import traitlets
from typing_extensions import override

import async_kernel
from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.compat.ipython import (
    AsyncDisplayFormatter,
    AsyncDisplayHook,
    AsyncDisplayPublisher,
    AsyncDisplayTrap,
    AsyncHistoryManager,
    NoopBuiltinTrap,
)
from async_kernel.compiler import XCachingCompiler
from async_kernel.event_loop.run import get_runtime_matplotlib_guis
from async_kernel.interface import HasParentInterface
from async_kernel.interface.base import ShellPendingManager
from async_kernel.typing import Content, RunMode, Tags

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from anyio.abc import ByteReceiveStream

    from async_kernel import Kernel
    from async_kernel.pending import Pending


__all__ = ["AsyncInteractiveShell"]


# _____________________________ IMPORTANT _____________________________________
# Patch Ipython's InteractiveShell, which may get called by external modules.
InteractiveShell.initialized = lambda: True
InteractiveShell.instance = lambda: utils.get_kernel().shell


def _noop(*args, **kwargs) -> None:
    return None


class AsyncInteractiveShell(HasParentInterface, InteractiveShell):
    """
    An IPython InteractiveShell adapted to work with [async-kernel][async_kernel.kernel.Kernel].

    Info:
        - There is only one interactive shell instance.
        - When subclassing:
            - The last defined subclass is used when creating the shell.
            - A matching subclass of [AsyncInteractiveSubshell][] should also be defined.

    Notable differences:
        - Supports a soft timeout specified via tags `timeout=<value in seconds>`[^1].
        - `user_ns` and `user_global_ns` are same dictionary which is a fixed [dict][].
        - Supports async magic functions (See [KernelMagics.pip][]).

        [^1]: When the execution time exceeds the timeout value, the code execution will "move on".
    """

    DEFAULT_MATPLOTLIB_BACKENDS: ClassVar = ["inline", "ipympl"]

    kernel: Fixed[Self, Kernel] = Fixed(lambda c: c["owner"].parent.kernel)

    display_trap = Fixed(AsyncDisplayTrap)
    ""
    display_pub = Fixed(AsyncDisplayPublisher)
    ""
    display_formatter = Fixed(AsyncDisplayFormatter)
    ""

    history_manager: Fixed[Self, AsyncHistoryManager] = Fixed(
        lambda c: AsyncHistoryManager(shell=c["owner"]), set_mode="ignore"
    )
    ""

    compiler_class = traitlets.Type(XCachingCompiler).tag(config=True)
    ""

    pending_manager = Fixed(ShellPendingManager)
    ""

    user_ns_hidden: Fixed[Self, dict] = Fixed(dict)
    ""

    timeout = traitlets.CFloat(0.0).tag(config=True)
    "A timeout in seconds to complete execute requests."

    stop_on_error_time_offset = traitlets.Float(0.0)
    "An offset to add to the cancellation time to catch late arriving execute requests."

    protected = traitlets.Bool(read_only=True)
    ""
    is_mainshell = traitlets.Bool(False, read_only=True)
    ""

    subshell_id: Fixed[Self, str | None] = Fixed(
        lambda c: None if c["owner"].is_mainshell else c["owner"].pending_manager.id
    )
    configurables = Fixed(list)
    compile: traitlets.Instance[XCachingCompiler]

    _execution_count = 0
    _resetting = False
    _user_ns: Fixed[Self, dict] = Fixed(dict)  # pyright: ignore[reportIncompatibleVariableOverride]
    _main_mod_cache = Fixed(dict)
    _stop_on_error_pool: Fixed[Self, set[Callable[[], object]]] = Fixed(set)
    _stop_on_error_info: Fixed[Self, dict[Literal["time", "execution_count"], Any]] = Fixed(dict)
    _subshells: Fixed[Self, dict[str, Self]] = Fixed(dict)

    # Overridden attributes
    displayhook_class = None
    display_pub_class = None
    loop_runner_map = None
    loop_runner = None
    autoindent = False
    call_pdb = Fixed(lambda _: None, set_mode="ignore")
    trio_runner = None

    # Overridden methods
    init_create_namespaces: Callable[..., None] = _noop
    save_sys_module_state = _noop
    init_sys_modules = _noop
    init_displayhook = _noop
    init_display_pub = _noop
    init_history = _noop
    init_encoding = _noop
    init_user_ns = _noop
    init_data_pub = _noop
    init_environment = _noop

    @property
    @override
    def user_global_ns(self) -> dict:
        if self.is_mainshell:
            return self.user_ns
        return self.parent.shell.user_global_ns.copy() if self._resetting else self.parent.shell.user_global_ns

    @property
    @override
    def displayhook(self) -> AsyncDisplayHook:
        return self.parent.displayhook

    @override
    def __repr__(self) -> str:
        protected = " 🔐" if self.protected else ""
        if self.is_mainshell:
            return f"<{self.__class__.__name__}{protected}: Main Shell>"
        return f"<{self.__class__.__name__}{protected}: Subshell ({self.subshell_id})>"

    def __init__(self, *, protected: bool = False, **kwargs: Any) -> None:
        if "__mainshell__" in kwargs:
            self.set_trait("is_mainshell", True)
            return  # __init__ gets called again by the interface ('self.parent').
        if protected:
            self.set_trait("protected", True)
        if self.subshell_id:
            self.parent._subshell_created(self)  # pyright: ignore[reportPrivateUsage]

        # This is where traits with a config_key argument are updated
        # from the values on config.
        super(InteractiveShell, self).__init__(**kwargs)

        # These are relatively independent and stateless
        self.init_ipython_dir(kwargs.get("ipython_dir"))
        self.init_profile_dir(kwargs.get("profile_dir"))
        self.init_instance_attrs()
        self.init_prefilter()

        self.init_syntax_highlighting()
        self.init_hooks()
        self.init_events()
        self.init_pushd_popd_magic()
        self.init_logger()

        self.init_builtins()

        # The following was in post_config_initialization
        self.init_completer()

        self.init_traceback_handlers(kwargs.get("custom_exceptions", ((), None)))
        self.init_prompts()
        # self.init_display_formatter()

        self.init_magics()
        self.init_alias()
        self.init_logstart()

        self.init_extension_manager()
        self.init_payload()
        self.user_ns_hidden.update(self.user_ns)
        self.events.trigger("shell_initialized", self)

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
            "_": "",
            "__": "",
            "___": "",
        }

    @traitlets.default("banner1")
    def _default_banner1(self) -> str:
        kernel_info = (
            f"async-kernel v{async_kernel.__version__} name:{self.parent.name!r} backend:{str(self.parent.backend)!r}"
        )

        return f"Python {sys.version}\n{kernel_info}) \nIPython shell {IPython.core.release.version}\n"

    @traitlets.observe("exit_now")
    def _update_exit_now(self, _) -> None:
        """Stop eventloop when `exit_now` fires."""
        if self.exit_now:
            self.parent.stop()

    def ask_exit(self) -> None:
        if self.parent.raw_input("Are you sure you want to stop the kernel?\ny/[n]\n") == "y":
            self.exit_now = True

    @override
    def init_builtins(self):
        # A single, static flag that we set to True.  Its presence indicates
        # that an IPython shell has been created, and we make no attempts at
        # removing on exit or representing the existence of more than one
        # IPython at a time.
        builtin_mod.__dict__["__IPYTHON__"] = True
        builtin_mod.__dict__["display"] = display
        builtin_mod.__dict__["display"] = get_ipython

        self.builtin_trap = NoopBuiltinTrap()

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

    @property
    @override
    def execution_count(self) -> int:
        return self._execution_count

    @execution_count.setter
    def execution_count(self, value) -> None:
        return

    @contextmanager
    @override
    def _tee(self, channel: Literal["stdout", "stderr"]):
        yield

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

    def transform_cell_async(self, raw_cell: str) -> str:
        "Transform the cell and substitute magic calls with an awaitable wrapper."

        return (
            self.transform_cell(raw_cell)
            .replace("get_ipython().run_line_magic(", "await get_ipython().run_line_magic_async(")
            .replace("get_ipython().run_cell_magic(", "await get_ipython().run_cell_magic_async(")
            .replace("get_ipython().system(", "await get_ipython().system(")
        )

    @override
    def get_ipython(self) -> AsyncInteractiveShell:
        """Return the shell for the current context."""
        return self.kernel.shell

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
                    self.parent.interrupts.add(cancel)
                    if stop_on_error:
                        self._stop_on_error_pool.add(cancel)
                    with anyio.fail_after(delay=timeout or None):
                        result = await self.run_cell_async(
                            raw_cell=code,
                            store_history=store_history,
                            silent=silent,
                            transformed_cell=self.transform_cell_async(code),
                            shell_futures=True,
                            cell_id=cell_id,
                        )
                except (Exception, anyio.get_cancelled_exc_class()) as e:
                    # A safeguard to catch exceptions not caught by the shell.
                    err = KernelInterrupt() if self.parent.last_interrupt_frame else e
                else:
                    err = result.error_before_exec or result.error_in_exec if result else KernelInterrupt()
                    if not err and Tags.raises_exception in tags:
                        msg = "An expected exception was not raised!"
                        err = RuntimeError(msg)
                finally:
                    self._stop_on_error_pool.discard(cancel)
                    self.parent.interrupts.discard(cancel)
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
        if utils.get_timeout() != 0.0 and etype is anyio.get_cancelled_exc_class():
            etype, evalue, stb = TimeoutError, "Cell execute timeout", []
        self.parent.iopub_send(
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
        return [*get_runtime_matplotlib_guis(), *self.DEFAULT_MATPLOTLIB_BACKENDS]

    @contextlib.contextmanager
    def context(self) -> Generator[None, Any, None]:
        "A context manager where the shell is active."
        with self.pending_manager.context():
            yield

    def stop(self, *, force=False) -> None:
        "Stop the shell - do not call directly."

        if self.protected and not force:
            return
        self.parent._subshell_stopped(self)  # pyright: ignore[reportPrivateUsage]

        self.configurables.clear()

        self.user_ns.clear()
        self.history_manager.stop()
        try:
            self.atexit_operations()
        except AttributeError:
            pass


@magics_class
class KernelMagics(HasParentInterface, Magics):
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
        subshells = list(self.parent.subshells)
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
            await self.parent.get_shell().system([sys.executable, "-m", "pip", *line.split()])
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
        return self.parent.get_shell().system(cmd, stderr_to_stdout=True)

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
        shell = self.parent.get_shell()
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
        shell = self.parent.get_shell()
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
