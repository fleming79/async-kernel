"""The base class definition to interface with the kernel."""

from __future__ import annotations

import ast
import builtins
import contextlib
import functools
import gc
import getpass
import signal
import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, override
from uuid import uuid4

import anyio
from aiologic.lowlevel import current_async_library, enable_signal_safety
from traitlets import traitlets
from traitlets.config.configurable import SingletonConfigurable

import async_kernel
from async_kernel import utils
from async_kernel.asyncshell import AsyncInteractiveShell, ShellPendingManager
from async_kernel.caller import Caller
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.iostream import OutStream
from async_kernel.kernel import Kernel
from async_kernel.typing import (
    Backend,
    CallerCreateOptions,
    Channel,
    Content,
    HandlerType,
    Job,
    Message,
    MsgHeader,
    MsgType,
    NoValue,
    RunMode,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Mapping
    from types import CoroutineType, FrameType


__all__ = ["BaseKernelInterface"]


def extract_header(msg_or_header: dict[str, Any]) -> MsgHeader | dict:
    """Given a message or header, return the header."""
    if not msg_or_header:
        return {}
    try:
        # See if msg_or_header is the entire message.
        h = msg_or_header["header"]
    except KeyError:
        try:
            # See if msg_or_header is just the header
            h = msg_or_header["msg_id"]
        except KeyError:  # noqa: TRY203
            raise
        else:
            h = msg_or_header
    return h


class DictValueLiteralEval(traitlets.Dict):
    "An instance of a Python dict which converts string values to Python literals."

    @override
    def item_from_string(self, s: str) -> dict:
        d = super().item_from_string(s)
        for k, v in d.items():
            try:
                d[k] = ast.literal_eval(v)
            except ValueError:
                pass
        return d


class BaseKernelInterface(SingletonConfigurable, anyio.AsyncContextManagerMixin):
    """
    The base class for interfacing with the kernel.

    Must be overloaded to be useful.
    """

    name = traitlets.Unicode("async-kernel").tag(config=True)
    ""

    start_interface = traitlets.Unicode("").tag(config=True)
    "The value used to import the interface using [async_kernel.kernelspec.import_start_interface][]."

    quiet = traitlets.Bool(True).tag(config=True)
    "Only send stdout/stderr to output stream."

    callers: Fixed[Self, dict[Literal[Channel.shell, Channel.control], Caller]] = Fixed(dict)
    "The caller associated with the kernel once it has started."

    interrupts: Fixed[Self, set[Callable[[], object]]] = Fixed(set)
    "A set for callbacks to register for calling when `interrupt` is called."

    last_interrupt_frame = None
    "This frame is set when an interrupt is intercepted and cleared once the interrupt has been handled."

    backend: traitlets.TraitType[Backend, Backend] = traitlets.UseEnum(Backend).tag(config=True)
    "The type of asynchronous backend used. Options are 'asyncio' or 'trio'."

    host = None

    banner = traitlets.Unicode().tag(config=True)
    "Info to insert into shell banner."


    handle_in_shell_thread = traitlets.List(
        traitlets.UseEnum(MsgType), [MsgType.comm_msg, MsgType.comm_open, MsgType.comm_close]
    )
    """
    A list of `MsgType` that are always handled in the shell's thread (typically the _MainThread_).
    """

    handle_in_thread = traitlets.Dict(key_trait=traitlets.UseEnum(MsgType), value_trait=traitlets.Unicode())
    """
    A mapping of `MsgType` to the name of a separate caller (thread) in which to run the handler.
    """

    _instance: Self | None = None
    _zmq_context = None
    _handler_cache: ClassVar[dict[tuple[str | None, MsgType, Callable], HandlerType]] = {}

    # the kernel class, as an importstring
    kernel_class = traitlets.Type(default_value=Kernel, klass=Kernel).tag(config=True)
    "The Kernel subclass to be used."

    pre_start = traitlets.List(["init_path", "init_extensions", "init_code"]).tag(config=True)
    """
    A list of method names to call when the kernel is ready but prior to handling messages. 

    Notes:
        - The builtin interact
        - A pending group is created to wait for pending created in the context.
        - To bypass the pending group, use the `call_direct` method. 
    """

    kernel = traitlets.Instance(Kernel, (), read_only=True)
    "The kernel."

    @classmethod
    @override
    def instance(cls, **kwargs) -> Self:
        return cls(**kwargs)

    @staticmethod
    @override
    def clear_instance() -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        raise NotImplementedError

    def __new__(cls, **kwargs) -> Self:
        if (interface := BaseKernelInterface._instance) is None:
            BaseKernelInterface._instance = interface = super().__new__(cls, **kwargs)
        if not isinstance(interface, cls):
            msg = f"An unrelated instance already exists! {interface=}"
            raise TypeError(msg)
        return interface

    @traitlets.default("handle_in_thread")
    def _default_handle_in_thread(self) -> dict[MsgType, str]:
        return {
            MsgType.inspect_request: "language_server",
            MsgType.complete_request: "language_server",
            MsgType.is_complete_request: "language_server",
        }

    @traitlets.default("banner")
    def _default_banner(self) -> str:
        return f"async-kernel v{async_kernel.__version__} name:{self.name!r} backend:{str(self.backend)!r}"

    @property
    def shell(self) -> AsyncInteractiveShell:
        "A link to the main shell."
        return self.kernel.main_shell

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Kernel]:
        self.backend = Backend(current_async_library())
        kernel = self.kernel
        kernel.comm_manager.patch_comm()
        sig = restore_io = None
        caller = Caller(
            "manual",
            name="Shell",
            protected=True,
            log=self.log,
            zmq_context=self._zmq_context,
            host=self.host,
        )
        self.callers[Channel.shell] = caller
        self.callers[Channel.control] = caller.get(name="Control", log=kernel.log, protected=True)

        async with caller:
            assert BaseKernelInterface._instance is self
            restore_io = None
            try:
                restore_io = self._patch_io()
                with contextlib.suppress(ValueError, AttributeError):
                    sig = signal.signal(signal.SIGINT, self._signal_handler)
                with anyio.CancelScope() as scope:
                    self._scope = scope
                    # Pre-start callbacks
                    async with self.callers[Channel.shell].create_pending_group():
                        for n in self.pre_start:
                            try:
                                getattr(self, n)()
                            except Exception as e:
                                self.log.exception("Pre-start callback %r error", n, exc_info=e)
                    kernel.event_started.set()
                    self.log.info("Kernel started: %s", self.banner)
                    yield kernel
            finally:
                self.kernel.event_stopped.set()
                with anyio.CancelScope(shield=True):
                    await kernel.do_shutdown(kernel._restart)  # pyright: ignore[reportPrivateUsage]
                if sig:
                    signal.signal(signal.SIGINT, sig)
                if restore_io:
                    restore_io()
                self._handler_cache.clear()
                BaseKernelInterface._instance = None
                Kernel._instance = None  # pyright: ignore[reportPrivateUsage]
                gc.collect()

    @enable_signal_safety
    def _signal_handler(self, signum, frame: FrameType | None) -> None:
        self.last_interrupt_frame = frame
        self.interrupt()
        self.last_interrupt_frame = None
        raise KernelInterrupt

    def _subshell_stopped(self, subshell_id: str) -> None:
        for key in list(self._handler_cache):
            if key[0] == subshell_id:
                self._handler_cache.pop(key, None)

    def _patch_io(self) -> Callable[[], None]:
        original_io = sys.stdout, sys.stderr, sys.displayhook, builtins.input, self.getpass

        def restore():
            sys.stdout, sys.stderr, sys.displayhook, builtins.input, getpass.getpass = original_io

        builtins.input = self.raw_input
        getpass.getpass = self.getpass
        for name in ["stdout", "stderr"]:

            def flusher(string: str, name=name) -> None:
                "Publish stdio or stderr when flush is called"
                self.iopub_send(
                    msg_or_type="stream",
                    content={"name": name, "text": string},
                    ident=f"stream.{name}".encode(),
                )
                if not self.quiet and (echo := (sys.__stdout__ if name == "stdout" else sys.__stderr__)):
                    echo.write(string)  # pragma: no cover
                    echo.flush()  # pragma: no cover

            context = utils._stdout_context if name == "stdout" else utils._stderr_context  # pyright: ignore[reportPrivateUsage]
            wrapper = OutStream(send=flusher, context=context)
            setattr(sys, name, wrapper)

        return restore

    def _get_handler(self, job: Job, send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]]) -> HandlerType:
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
            handler: HandlerType = getattr(self.kernel, msg_type)

            @functools.wraps(handler)
            async def run_handler(job: Job) -> None:
                job_token = utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
                subshell_token = ShellPendingManager._id_contextvar.set(subshell_id)  # pyright: ignore[reportPrivateUsage]

                try:
                    self.iopub_send(
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
                    self.iopub_send(
                        msg_or_type="status",
                        parent=job["msg"],
                        content={"execution_state": "idle"},
                        ident=b"kernel.status",
                    )
                    del job

            self._handler_cache[key] = run_handler
            return run_handler

    def message_handler(self, job: Job, send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]], /) -> None:
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

        handler = self._get_handler(job, send_reply)

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

        self.log.debug("%s %s %s %s", msg_type, run_mode, handler, job)

    async def run(self, *, stopped: Callable[[], Any] | None = None) -> None:
        """
        Run the kernel.

        Args:
            stopped: An optional callback that is called when the kernel has stopped.

        This method requires that a [Caller][async_kernel.caller.Caller] instance does not already exist in the current thread.
        """
        try:
            async with self as kernel:
                await kernel.event_stopped
        finally:
            if stopped:
                stopped()

    def stop(self) -> None:
        """
        Stop the kernel.
        """
        if scope := getattr(self, "_scope", None):
            del self._scope
            self.log.info("Stopping kernel")
            self.callers[Channel.shell].call_direct(scope.cancel, "Stopping kernel")
            self.kernel.event_stopped.set()

    def load_settings(self, settings: Mapping[str, Any]) -> dict[str, Any]:
        """
        Load settings via dotted path relative to the interface.

        Args:
            settings: A mapping of dotted name to value. If the literal value cannot be set,
                it will be evaluated and set, where possible. Unset values are quietly ignored.

        Returns:
            dict: The values that were successfully set.
        """
        return async_kernel.utils.apply_settings(self, settings)

    def input_request(self, prompt: str, *, password: bool = False) -> str:
        """
        Forward an input request to the frontend.

        Args:
            prompt: The user prompt.
            password: If the prompt should be considered as a password.

        Raises:
           IPython.core.error.StdinNotImplementedError: if active frontend doesn't support stdi
        """
        raise NotImplementedError

    def raw_input(self, prompt: str = "") -> str:
        """
        Forward a raw_input request to the client.

        Args:
            prompt: The user prompt.

        Raises:
           IPython.core.error.StdinNotImplementedError: if active frontend doesn't support stdin.
        """
        return self.input_request(str(prompt), password=False)

    def getpass(self, prompt: str = "") -> str:
        """
        Forward getpass to the client.

        Args:
            prompt: The user prompt.

        Raises:
           IPython.core.error.StdinNotImplementedError: if active frontend doesn't support stdin.
        """
        return self.input_request(prompt, password=True)

    def interrupt(self) -> None:
        """
        Interrupt execution, possible raising a [async_kernel.asyncshell.KernelInterrupt][].
        """
        while self.interrupts:
            try:
                self.interrupts.pop()()
            except Exception:
                pass

    def msg(
        self,
        msg_type: str,
        *,
        content: dict | None = None,
        parent: Message | dict[str, Any] | None = None,
        header: MsgHeader | dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        channel: Channel = Channel.shell,
    ) -> Message[dict[str, Any]]:
        """
        Create a new message.

        This format is different from what is sent over the wire. The
        serialize/deserialize methods converts this nested message dict to the wire
        format, which is a list of message parts.
        """
        parent = parent or utils.get_parent()
        if header is None:
            session = ""
            if parent and (header := parent.get("header")):
                session = header.get("session", "")
            header = MsgHeader(
                date=datetime.now(UTC),
                msg_id=str(uuid4()),
                msg_type=msg_type,
                session=session,
                username="",
                version=async_kernel.kernel_protocol_version,
            )
        return Message(  # pyright: ignore[reportCallIssue]
            channel=channel,
            header=header,
            parent_header=extract_header(parent),  # pyright: ignore[reportArgumentType]
            content={} if content is None else content,
            metadata=metadata if metadata is not None else {},
        )

    def iopub_send(
        self,
        msg_or_type: Message[dict[str, Any]] | dict[str, Any] | str,
        *,
        content: Content | None = None,
        metadata: dict[str, Any] | None = None,
        parent: dict[str, Any] | MsgHeader | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        ident: bytes | list[bytes] | None = None,
        buffers: list[bytes] | None = None,
    ) -> None:
        """Send an iopub message."""
        raise NotImplementedError
