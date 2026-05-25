from __future__ import annotations

import builtins
import functools
import getpass
import sys
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from io import TextIOBase
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, Self

import traitlets
from traitlets.config import LoggingConfigurable
from typing_extensions import override

import async_kernel
from async_kernel import utils
from async_kernel.comm import CommManager
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.debugger import Debugger
from async_kernel.interface import HasInterface
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
    from contextvars import ContextVar
    from types import CoroutineType

    from async_kernel.caller import Caller
    from async_kernel.typing import Content, Message

__all__ = ["Kernel", "KernelInterrupt"]


class OutStream(HasInterface, TextIOBase):
    """
    A file like object that sends or redirects text as it is written.

    Only intended for internal use.
    """

    def __init__(self, name: Literal["stdout", "stderr"]) -> None:
        """
        Args:
            send: A callback to send text as it is written.
            context: A context variable to an potential alternate target for the text.
        """
        super().__init__()
        self.name = name
        self._context: ContextVar = getattr(async_kernel.utils, f"_{name}_context")
        self.ident = f"stream.{self.name}".encode()
        self._origin = None

    def patch(self) -> Callable[[], None]:
        self._origin = origin = getattr(sys, self.name)
        setattr(sys, self.name, self)

        def restore() -> None:
            setattr(sys, self.name, origin)

        return restore

    @override
    def isatty(self) -> Literal[True]:
        return True

    @override
    def readable(self) -> Literal[False]:
        return False

    @override
    def seekable(self) -> Literal[False]:
        return False

    @override
    def writable(self) -> Literal[True]:
        return True

    @override
    def flush(self) -> None:
        if c_out := self._context.get():
            c_out.flush()

    @override
    def write(self, string: str) -> int:
        if not isinstance(string, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            msg = f"Not a string: {string!r}"  # pyright: ignore[reportUnreachable]
            raise TypeError(msg)
        if out := self._context.get():
            out.write(string)
        else:
            interface = self.parent
            interface.iopub_send(msg_or_type="stream", content={"name": self.name, "text": string}, ident=self.ident)
            if self._origin and not self.parent.quiet:
                self._origin.write(string)  # pragma: no cover
                self._origin.flush()  # pragma: no cover

        return len(string)

    @override
    def writelines(self, sequence) -> None:
        self.write("".join(sequence))
        self.flush()


class Kernel(HasInterface[T_interface_co], LoggingConfigurable, Generic[T_interface_co, T_shell_co]):
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

    callers: Fixed[Self, dict] = Fixed(lambda c: c["owner"].parent.callers)
    "A shortcut to the callers dict on the parent."

    caller: Fixed[Self, Caller] = Fixed(lambda c: c["owner"].callers[Channel.shell])
    "The caller for the shell thread."

    debugger = Fixed(Debugger)
    "The debugger for handling debug requests."

    comm_manager = Fixed(CommManager)
    "Creates [async_kernel.comm.Comm][] instances and maintains a mapping to `comm_id` to `Comm` instances."

    _restart = False
    _handler_cache: ClassVar[dict[tuple[str | None, MsgType, Callable], HandlerType]] = {}
    _subshells: dict[str, T_shell_co]

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
    async def running(self) -> AsyncGenerator[None]:
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
                yield
        finally:
            self.log.info("Kernel stopped")
            for subshell in tuple(self._subshells.values()):
                subshell.stop(force=True)
            for comm in tuple(self.comm_manager.comms.values()):
                comm.close(deleting=True)
            remove_patch()
            del self._main_shell
            self._handler_cache.clear()

    def apply_patches(self) -> Callable[[], None]:
        """Apply patches returning a callable to reverse the patches."""

        original = sys.displayhook, builtins.input, getpass.getpass
        builtins.input, sys.displayhook, getpass.getpass = self.raw_input, self.displayhook, self.getpass
        restore_comm = self.comm_manager.patch_comm()
        restore_stdout = OutStream("stdout").patch()
        restore_stderr = OutStream("stderr").patch()

        def restore() -> None:
            sys.displayhook, builtins.input, getpass.getpass = original
            restore_stdout()
            restore_stderr()
            restore_comm()

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
            caller = self.callers[job["msg"]["channel"]]
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

        self.log.debug("%s %s %s %s", msg_type, run_mode, handler, job)

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

        Use [`shell.stop(force=True)`][async_kernel.shell.BaseShell.stop] to stop a
        protected subshell when it is no longer required.

        Args:
            protected: Protect the subshell from accidental deletion.
        Tip:
            - `await shell.ready` to ensure the shell is 'ready'.
        """

        return self._shell_class(protected=protected)

    async def kernel_info_request(self, job: Job[Content], /) -> Content:
        """Handle a [kernel info request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-info)."""
        return self.kernel_info

    async def comm_info_request(self, job: Job[Content], /) -> Content:
        """Handle a [comm info request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-info)."""
        c = job["msg"]["content"]
        target_name = c.get("target_name", None)
        comms = {
            k: {"target_name": v.target_name}
            for (k, v) in tuple(self.comm_manager.comms.items())
            if v.target_name == target_name or target_name is None
        }
        return {"comms": comms}

    async def execute_request(self, job: Job[ExecuteContent], /) -> Content:
        """Handle a [execute request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)."""
        return await self.shell.execute_request(job)

    async def complete_request(self, job: Job[Content], /) -> Content:
        """Handle a [completion request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#completion)."""
        return await self.shell.do_complete_request(
            code=job["msg"]["content"].get("code", ""), cursor_pos=job["msg"]["content"].get("cursor_pos", 0)
        )

    async def is_complete_request(self, job: Job[Content], /) -> Content:
        """Handle a [is_complete request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness)."""
        return await self.shell.is_complete_request(job["msg"]["content"].get("code", ""))

    async def inspect_request(self, job: Job[Content], /) -> Content:
        """Handle a [inspect request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introspection)."""
        c = job["msg"]["content"]
        return await self.shell.inspect_request(
            code=c.get("code", ""),
            cursor_pos=c.get("cursor_pos", 0),
            detail_level=c.get("detail_level", 0),
        )

    async def history_request(self, job: Job[Content], /) -> Content:
        """Handle a [history request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#history)."""
        return await self.shell.history_request(**job["msg"]["content"])

    async def comm_open(self, job: Job[Content], /) -> None:
        """Handle a [comm open request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#opening-a-comm)."""
        self.comm_manager.comm_open(stream=None, ident=None, msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_msg(self, job: Job[Content], /) -> None:
        """Handle a [comm msg request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-messages)."""
        self.comm_manager.comm_msg(stream=None, ident=None, msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_close(self, job: Job[Content], /) -> None:
        """Handle a [comm close request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#tearing-down-comms)."""
        self.comm_manager.comm_close(stream=None, ident=None, msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def interrupt_request(self, job: Job[Content], /) -> Content:
        """Handle an [interrupt request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-interrupt)."""
        self.shell.interrupt()
        return {}

    async def shutdown_request(self, job: Job[Content], /) -> Content:
        """Handle a [shutdown request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-shutdown)."""
        self._restart = job["msg"]["content"].get("restart", False)
        self.parent.stop()
        return {"restart": self._restart}

    async def debug_request(self, job: Job[Content], /) -> Content:
        """Handle a [debug request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request)."""
        return await self.debugger.process_request(job["msg"]["content"])

    async def create_subshell_request(self: Kernel, job: Job[Content], /) -> Content:
        """Handle a [create subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#create-subshell)."""
        async with self.caller.create_pending_group():
            shell = self.create_subshell(protected=False)
            return {"subshell_id": shell.subshell_id}

    async def delete_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle a [delete subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#delete-subshell)."""
        if (subshell_id := job["msg"]["content"]["subshell_id"]) and (subshell := self._subshells.get(subshell_id)):
            subshell.stop()
        return {}

    async def list_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle a [list subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#list-subshells)."""
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
        return await self.shell.do_complete_request(code=code, cursor_pos=cursor_pos)

    async def do_inspect(self, code: str, cursor_pos: int | None, detail_level=0, omit_sections=()) -> Content:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_inspect][]."
        return await self.shell.inspect_request(code=code, cursor_pos=cursor_pos)  # pyright: ignore[reportArgumentType]

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
        return await self.shell.history_request(
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
        silent: bool,
        store_history: bool = True,
        user_expressions: dict[str, str] | None = None,
        allow_stdin: bool = False,
        *,
        cell_meta: dict[str, Any] | None = None,
        cell_id: str | None = None,
    ) -> Content:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_execute][]."
        if cell_meta is None:
            cell_meta = {}
        if cell_id is not None:
            cell_meta["cellId"] = cell_id
        msg = self.parent.msg(
            "execute_request",
            content=ExecuteContent(
                code=code,
                silent=silent,
                store_history=store_history,
                user_expressions=user_expressions or {},
                allow_stdin=allow_stdin,
                stop_on_error=False,
            ),
            metadata=cell_meta,
        )
        job: Job[ExecuteContent] = Job(msg=msg, ident=[], received_time=time.monotonic())
        token = utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        try:
            return await self.shell.execute_request(job)
        finally:
            utils._job_var.reset(token)  # pyright: ignore[reportPrivateUsage]

    def getpass(self, prompt="", stream=None) -> str:
        "Matches signature of [ipykernel.kernelbase.Kernel.getpass][]."
        return self.parent.input_request(str(prompt), password=True)

    def raw_input(self, prompt="") -> str:
        "Matches signature of [ipykernel.kernelbase.Kernel.raw_input][]."
        return self.parent.input_request(str(prompt), password=False)
