from __future__ import annotations

import builtins
import contextlib
import functools
import getpass
import os
import signal
import sys
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, Self

import anyio
import traitlets
from aiologic.lowlevel import enable_signal_safety
from traitlets.config import LoggingConfigurable

import async_kernel
from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.comm import CommManager
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.debugger import Debugger
from async_kernel.interface import HasInterface
from async_kernel.outstream import OutStream
from async_kernel.pending import Pending
from async_kernel.shell.base import ShellPendingManager
from async_kernel.typing import (
    CallerCreateOptions,
    Channel,
    ExecuteContent,
    HandlerType,
    Job,
    MsgType,
    NoValue,
    RunMode,
    T_interface_co,
    T_shell_co,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from types import CoroutineType, FrameType

    from async_kernel.typing import Content, Message

__all__ = ["Kernel", "KernelInterrupt"]


class Kernel(
    HasInterface[T_interface_co],
    LoggingConfigurable,
    anyio.AsyncContextManagerMixin,
    Generic[T_interface_co, T_shell_co],
):
    """
    The class containing the handler methods to implement a Jupyter Kernel.
    """

    help_links = traitlets.List(trait=traitlets.Dict()).tag(config=True)
    "A list of links provided kernel info request."

    supported_features = traitlets.List(traitlets.Unicode()).tag(config=True)
    "A list of features supported by the kernel."

    handle_in_shell_thread = traitlets.List(
        traitlets.UseEnum(MsgType),
        [MsgType.comm_msg, MsgType.comm_open, MsgType.comm_close],
    ).tag(config=True)
    """
    A list of `MsgType` that are always handled in the shell's thread (typically the _MainThread_).
    """

    handle_in_thread = traitlets.Dict(key_trait=traitlets.UseEnum(MsgType), value_trait=traitlets.Unicode())
    """
    A mapping of `MsgType` to the name of a separate caller (thread) in which to run the handler.
    """

    callers: Fixed[Self, dict[Literal[Channel.shell, Channel.control], Caller]] = Fixed(
        lambda c: c["owner"].parent.callers
    )
    "A shortcut to the callers dict on the parent."

    caller: Fixed[Self, Caller] = Fixed(lambda c: c["owner"].callers[Channel.shell])
    "The caller for the shell thread."

    debugger = Fixed(Debugger)
    "The debugger for handling debug requests."

    comm_manager = Fixed(CommManager)
    "Creates [async_kernel.comm.Comm][] instances and maintains a mapping to `comm_id` to `Comm` instances."

    active_execute_requests: Fixed[Self, set[Pending[Any]]] = Fixed(set)
    "A set of active execute requests that gets updated by the shell."

    interrupt_check_delay = traitlets.CFloat(1.0).tag(config=True)
    "The delay for a check before raising an interrupt in seconds."

    _interrupt_message = "Kernel interrupted"

    _restart = False
    _handler_cache: ClassVar[dict[tuple[str | None, MsgType, Callable], HandlerType]] = {}
    _subshells: dict[str, T_shell_co]
    _interrupt_requested: None | Pending = None

    @traitlets.default("help_links")
    def _default_help_links(self) -> tuple[dict[str, str], ...]:
        return (
            {
                "text": "Async Kernel Reference ",
                "url": "https://fleming79.github.io/async-kernel/",
            },
            {
                "text": "IPython Reference",
                "url": "https://ipython.readthedocs.io/en/stable/",
            },
            {
                "text": "IPython magic Reference",
                "url": "https://ipython.readthedocs.io/en/stable/interactive/magics.html",
            },
            {
                "text": "Matplotlib ipympl Reference",
                "url": "https://matplotlib.org/ipympl/",
            },
            {
                "text": "Matplotlib Reference",
                "url": "https://matplotlib.org/contents.html",
            },
        )

    @traitlets.default("handle_in_thread")
    def _default_handle_in_thread(self) -> dict[MsgType, str]:
        return {
            MsgType.inspect_request: "language_server",
            MsgType.complete_request: "language_server",
            MsgType.is_complete_request: "language_server",
        }

    @traitlets.default("supported_features")
    def _default_supported_features(self) -> list[str]:
        features = ["kernel subshells"]
        if self.debugger.enabled:
            features.append("debugger")
        return features

    @property
    def kernel_info(self) -> dict[str, Any]:
        "Info provided to a kernel info request."
        return {
            "protocol_version": async_kernel.kernel_protocol_version,
            "implementation": async_kernel.distribution_name,
            "implementation_version": async_kernel.__version__,
            "language_info": async_kernel.kernel_protocol_version_info,
            "banner": self.shell.banner,
            "help_links": self.help_links,
            "debugger": self.debugger.enabled,
            "supported_features": self.supported_features,
        }

    @property
    def main_shell(self) -> T_shell_co:
        try:
            return self._main_shell
        except AttributeError:
            msg = "The main_shell is only available once the kernel is started."
            raise RuntimeError(msg) from None

    @property
    def shell(self) -> T_shell_co:
        """
        The shell given the current context.

        Notes:
            - The `subshell_id` of the main shell is `None`.
        """
        return self.get_shell()

    @property
    def subshells(self) -> dict[str, T_shell_co]:
        return self._subshells.copy()

    def __init__(self, parent: T_interface_co, shell_class: type[T_shell_co]) -> None:
        self._subshells = {}
        assert self.parent is parent
        super().__init__()
        self._shell_class = shell_class

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        """
        The kernel runs in this context.

        Notes:
            - Entered by the interface (parent).
            - Overload as required.
        """
        remove_patch = self.apply_patches()
        try:
            self._main_shell = self._shell_class(protected=True, is_mainshell=True)
            async with self._main_shell.mainshell_running():
                self.log.info("Kernel started")
                yield self
        finally:
            self.log.info("Kernel stopped")
            for subshell in self._subshells.copy().values():
                subshell.stop(force=True)
            for comm in self.comm_manager.comms.copy().values():
                comm.close(deleting=True)
            remove_patch()
            self._handler_cache.clear()

    def _signal_handler(self, signum, frame: FrameType | None) -> None:
        "Handle interrupt signals."

        if pen := self._interrupt_requested:
            self._interrupt_requested = None
            if not pen.done():
                with enable_signal_safety():
                    pen.set_result(None)
                raise KernelInterrupt
        elif signum == signal.SIGINT:
            self.log.info("Keyboard interrupt")
            self.parent.stop()

    async def do_interrupt(self) -> None:
        """
        Interrupt/cancel non-silent active execute requests.
        """
        assert Caller() is self.callers[Channel.control], "Must be called from the control thread."
        for pen in self.active_execute_requests.copy():
            if not pen.metadata.get("kwargs", {}).get("silent", False):
                pen.cancel(self._interrupt_message)
        if (
            (sys.platform != "emscripten")
            and (not self.debugger.enabled or not self.debugger.stopped_threads)
            and (self.caller.id == self.caller.CALLER_MAIN_THREAD_ID)
        ):
            # Can only signal when the shell's thread is the  MainThread.
            self._interrupt_requested = pen = Pending()
            self.caller.call_direct(lambda: pen.set_result(None))
            try:
                await pen.wait(result=False, timeout=1, protect=True)
            except TimeoutError:
                if sys.platform == "win32":
                    signal.raise_signal(signal.SIGINT)
                else:
                    os.kill(os.getpid(), signal.SIGINT)
                await pen.wait(result=False, timeout=10, protect=True)

    def _patch_signal(self) -> Callable[[], None]:

        with contextlib.suppress(ValueError, AttributeError):
            import signal  # noqa: PLC0415

            sig = signal.signal(signal.SIGINT, self._signal_handler)

            def restore() -> None:
                signal.signal(signal.SIGINT, sig)

            return restore
        return lambda: None

    def apply_patches(self) -> Callable[[], None]:
        """Apply patches returning a callable to reverse the patches."""

        original = sys.displayhook, builtins.input, getpass.getpass
        builtins.input, sys.displayhook, getpass.getpass = self.raw_input, self.displayhook, self.getpass
        restore_comm = self.comm_manager.patch_comm()
        restore_stdout = OutStream("stdout").patch()
        restore_stderr = OutStream("stderr").patch()
        restore_signal = self._patch_signal()

        def restore() -> None:
            sys.displayhook, builtins.input, getpass.getpass = original
            restore_stdout()
            restore_stderr()
            restore_comm()
            restore_signal()

        return restore

    def displayhook(self, result: Any):
        "The global patch for [sys.displayhook][] responsible for python display callbacks."
        self.get_shell().displayhook(result)

    def _shell_created(self, shell: T_shell_co) -> None:  # pyright: ignore[reportGeneralTypeIssues]
        "Called by `BaseShell.__init__`"
        if shell.subshell_id:
            self._subshells[shell.subshell_id] = shell

    def _subshell_stopped(self, shell: T_shell_co) -> None:  # pyright: ignore[reportGeneralTypeIssues]
        "Called by `BaseShell.stop`"
        if subshell_id := shell.subshell_id:
            self._subshells.pop(subshell_id, None)
        for key in list(self._handler_cache):
            if key[0] == subshell_id:
                self._handler_cache.pop(key, None)

    def _get_handler(
        self,
        job: Job,
        send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]],
        iopub_send: Callable,
    ) -> HandlerType:
        try:
            subshell_id = job["msg"]["content"]["subshell_id"]
        except KeyError:
            try:
                subshell_id = job["msg"]["header"]["subshell_id"]  # pyright: ignore[reportTypedDictNotRequiredAccess]
            except KeyError:
                subshell_id = None
        msg_type = MsgType(job["msg"]["header"]["msg_type"])

        if msg_type is MsgType.execute_request:
            key = (subshell_id, msg_type, send_reply)
        else:
            key = (None, msg_type, send_reply)
        try:
            return self._handler_cache[key]
        except KeyError:
            handler: HandlerType = getattr(self, msg_type)

            @functools.wraps(handler)
            async def run_handler(job: Job) -> None:
                job_token = utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
                subshell_token = ShellPendingManager._id_contextvar.set(subshell_id)  # pyright: ignore[reportPrivateUsage]

                try:
                    iopub_send(
                        msg_or_type="status",
                        parent=job["msg"],
                        content={"execution_state": "busy"},
                        ident=b"kernel.status",
                    )
                    if (content := await handler(job)) is not None:
                        await send_reply(job, content)
                except Exception as e:
                    await send_reply(job, utils.error_to_content(e))
                    self.log.exception("Exception in message handler:", exc_info=e)
                finally:
                    utils._job_var.reset(job_token)  # pyright: ignore[reportPrivateUsage]
                    ShellPendingManager._id_contextvar.reset(subshell_token)  # pyright: ignore[reportPrivateUsage]
                    iopub_send(
                        msg_or_type="status",
                        parent=job["msg"],
                        content={"execution_state": "idle"},
                        ident=b"kernel.status",
                    )
                    del job

            self._handler_cache[key] = run_handler
            return run_handler

    def message_handler(
        self,
        job: Job,
        send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]],
        iopub_send: Callable,
        /,
    ) -> None:
        """
        Schedule handling of the job (msg) with a handler running in a Task managed by a Caller.

        Each `msg_type` runs in a separate task, possibly in a separate thread and event loop.
        Typically, jobs are queued for execution by either the 'shell' or 'control' caller using
        [queue_call][async_kernel.caller.Caller.queue_call].

        'execute_request' messages can also specify alternate run modes:
            - task: Run the execute request as a task.
            - thread: Run the execute request in a worker thread.

            The alternate run mode can be specified in a few ways:
            - as a comment on the first line of the code block `# task` or `# thread`.
            - As a tag `thread` or `task`

        Args:
            job: A dict with the msg and supporting details.
            send_reply: The function for the handler to use to send the reply to the message.
        """
        handler = self._get_handler(job, send_reply, iopub_send)

        run_mode: RunMode | CallerCreateOptions | None = None
        msg_type = MsgType(job["msg"]["header"]["msg_type"])

        if msg_type is MsgType.execute_request:
            caller = self.callers[job["msg"]["channel"]]  # pyright: ignore[reportArgumentType]
            try:
                run_mode = next(mode for tag in utils.get_tags(job) if (mode := RunMode.to_runmode(tag)))
            except StopIteration:
                if content := job["msg"].get("content", {}):
                    if (code := content.get("code")) and (
                        mode := RunMode.to_runmode(code.strip().split("\n", maxsplit=1)[0])
                    ):
                        run_mode = mode
                    if content.get("silent"):
                        run_mode = RunMode.task

        elif msg_type in self.handle_in_shell_thread:
            caller = self.callers[Channel.shell]
        else:
            caller = self.callers[Channel.control]
            if thread_name := self.handle_in_thread.get(msg_type):
                caller = caller.get(name=thread_name, no_debug=True)

        match run_mode:
            case RunMode.queue | None:
                caller.queue_call(handler, job)
            case RunMode.task:
                caller.call_soon(handler, job)
            case RunMode.thread:
                caller.to_thread(handler, job)
            case _ as options:
                caller.get(**options).call_soon(handler, job)

        self.log.debug("***handle message %s*** %s %s %s", msg_type, run_mode, handler, job)

    def get_shell(self, subshell_id: str | None | NoValue = NoValue) -> T_shell_co:  # pyright: ignore[reportInvalidTypeForm]
        """
        Get a shell by `subshell_id`.

        Args:
            subshell_id: The id of an existing subshell.
        """
        if (subshell_id := subshell_id if subshell_id is not NoValue else ShellPendingManager.active_id()) is None:
            return self.main_shell
        return self._subshells.get(subshell_id) or self.main_shell

    def create_subshell(self, *, protected: bool = False) -> T_shell_co:
        """
        Create a subshell.

        Use [`shell.stop(force=True)`][async_kernel.shell.base.BaseShell.stop] to stop a
        protected subshell when it is no longer required.

        Args:
            protected: Protect the subshell from accidental deletion.
        Tip:
            - `await shell.ready` to ensure the shell is 'ready'.
        """

        return self._shell_class(protected=protected)

    async def kernel_info_request(self, job: Job[Content], /) -> Content:
        """Handle an [kernel info request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-info)."""
        return self.kernel_info

    async def comm_info_request(self, job: Job[Content], /) -> Content:
        """Handle an [comm info request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-info)."""
        c = job["msg"]["content"]
        target_name = c.get("target_name", None)
        comms = {
            k: {"target_name": v.target_name}
            for (k, v) in self.comm_manager.comms.copy().items()
            if v.target_name == target_name or target_name is None
        }
        return {"comms": comms}

    async def execute_request(self, job: Job[ExecuteContent], /) -> Content:
        """Handle an [execute request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)."""
        return await self.shell.do_execute(
            cell_id=job["msg"]["metadata"].get("cellId"),
            received_time=job["received_time"],
            tags=job["msg"]["metadata"].get("tags", ()),
            **job["msg"]["content"],  # pyright: ignore[reportArgumentType]
        )

    async def complete_request(self, job: Job[Content], /) -> Content:
        """Handle an [completion request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#completion)."""
        return await self.shell.do_complete(
            code=job["msg"]["content"].get("code", ""), cursor_pos=job["msg"]["content"].get("cursor_pos", 0)
        )

    async def is_complete_request(self, job: Job[Content], /) -> Content:
        """Handle an [is_complete request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness)."""
        return await self.shell.is_complete(job["msg"]["content"].get("code", ""))

    async def inspect_request(self, job: Job[Content], /) -> Content:
        """Handle an [inspect request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introspection)."""
        c = job["msg"]["content"]
        return await self.shell.do_inspect(
            code=c.get("code", ""),
            cursor_pos=c.get("cursor_pos", 0),
            detail_level=c.get("detail_level", 0),
        )

    async def history_request(self, job: Job[Content], /) -> Content:
        """Handle an [history request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#history)."""
        return await self.shell.do_history(**job["msg"]["content"])

    async def comm_open(self, job: Job[Content], /) -> None:
        """Handle an [comm open request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#opening-a-comm)."""
        self.comm_manager.comm_open(stream=None, ident=None, msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_msg(self, job: Job[Content], /) -> None:
        """Handle an [comm msg request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-messages)."""
        self.comm_manager.comm_msg(stream=None, ident=None, msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_close(self, job: Job[Content], /) -> None:
        """Handle an [comm close request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#tearing-down-comms)."""
        self.comm_manager.comm_close(stream=None, ident=None, msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def interrupt_request(self, job: Job[Content], /) -> Content:
        """Handle an [interrupt request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-interrupt)."""
        await self.do_interrupt()
        return {}

    async def shutdown_request(self, job: Job[Content], /) -> Content:
        """Handle an [shutdown request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-shutdown)."""
        self._restart = job["msg"]["content"].get("restart", False)
        self.parent.stop()
        return {"restart": self._restart}

    async def debug_request(self, job: Job[Content], /) -> Content:
        """Handle an [debug request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request)."""
        return await self.debugger.process_request(job["msg"]["content"])

    async def create_subshell_request(self: Kernel, job: Job[Content], /) -> Content:
        """Handle an [create subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#create-subshell)."""
        async with self.caller.create_pending_group():
            shell = self.create_subshell(protected=False)
            return {"subshell_id": shell.subshell_id}

    async def delete_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle an [delete subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#delete-subshell)."""
        if (subshell_id := job["msg"]["content"]["subshell_id"]) and (subshell := self._subshells.get(subshell_id)):
            subshell.stop()
        return {}

    async def list_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle an [list subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#list-subshells)."""
        return {"subshell_id": list(self._subshells)}

    def get_parent(self) -> Message[dict[str, Any]] | None:
        """
        A convenience method to access the 'message' in the current context if there is one.

        'parent' is the parameter name used by [Session.send][jupyter_client.session.Session.send] to provide context when sending a reply.

        See also:
            - [ipywidgets.Output][ipywidgets.widgets.widget_output.Output]:
                Uses `get_ipython().kernel.get_parent()` to obtain the `msg_id` which
                is used to 'capture' output when its context has been acquired.
        """
        return utils.get_parent_message()

    async def do_complete(self, code: str, cursor_pos: int | None) -> Content:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_complete][]."
        return await self.shell.do_complete(code=code, cursor_pos=cursor_pos)

    async def do_inspect(
        self, code: str, cursor_pos: int = 0, detail_level: Literal[0, 1] = 0, omit_sections=()
    ) -> Content:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_inspect][]."
        return await self.shell.do_inspect(code=code, cursor_pos=cursor_pos, detail_level=detail_level)

    async def do_history(
        self,
        hist_access_type,
        output,
        raw,
        session=None,
        start=None,
        stop=None,
        n=None,
        pattern=None,
        unique=False,
    ) -> Content:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_history][]."
        return await self.shell.do_history(
            output=output,
            raw=raw,
            hist_access_type=hist_access_type,
            session=session,  # pyright: ignore[reportArgumentType]
            start=start,  # pyright: ignore[reportArgumentType]
            stop=stop,
        )

    async def do_execute(
        self,
        code: str,
        silent: bool = True,
        store_history: bool = True,
        user_expressions: dict[str, str] | None = None,
        allow_stdin: bool = False,
        *,
        cell_meta: dict[str, Any] | None = None,
        cell_id: str | None = None,
        **_ignored,
    ) -> Content:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_execute][]."
        return await self.shell.do_execute(
            code=code,
            silent=silent,
            store_history=store_history,
            user_expressions=user_expressions,
            allow_stdin=allow_stdin,
            cell_id=cell_id,
        )

    def getpass(self, prompt="", stream=None) -> str:
        "Matches signature of [ipykernel.kernelbase.Kernel.getpass][]."
        return self.parent.input_request(str(prompt), password=True)

    def raw_input(self, prompt="") -> str:
        "Matches signature of [ipykernel.kernelbase.Kernel.raw_input][]."
        return self.parent.input_request(str(prompt), password=False)
