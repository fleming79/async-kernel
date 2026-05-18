"""The base class definition to interface with the kernel."""

from __future__ import annotations

import ast
import builtins
import contextlib
import functools
import gc
import getpass
import os
import signal
import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, final
from uuid import uuid4

import anyio
from aiologic import Event
from aiologic.lowlevel import current_async_library, enable_signal_safety
from traitlets import traitlets
from traitlets.config import Config, Configurable
from traitlets.config.application import Application, ClassesType
from typing_extensions import override

import async_kernel
from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.iostream import OutStream
from async_kernel.pending import PendingManager
from async_kernel.typing import Backend, Channel, Message, MsgHeader, MsgType, NoValue, RunMode

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Mapping
    from types import CoroutineType, FrameType

    from async_kernel.asyncshell import AsyncInteractiveShell
    from async_kernel.compat.ipython import AsyncDisplayHook
    from async_kernel.kernel import Kernel
    from async_kernel.typing import CallerCreateOptions, Content, HandlerType, Job

__all__ = ["BaseInterface", "HasInterface"]


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


class ShellPendingManager(PendingManager):
    "A pending manager to track the active shell/subshell."


class BaseInterface(Application, anyio.AsyncContextManagerMixin):
    """
    The base class for a singleton kernel interface.

    This singleton provides configuration and convenient references to other objects.
    This is where kerenels should be launched f

    Usage:
        launch:
            ```python
            Interface.launch_instance()
            ```
        async context:
            ```python
            async with Interface() as interface:
                ...
            ```
    """

    name = traitlets.Unicode("async").tag(config=True)
    ""

    classes: ClassesType = final([])
    "The classes registered with the interface."

    aliases = Application.aliases | {
        "name": "BaseInterface.name",
        "start_interface": "BaseInterface.start_interface",
        "kernel_class": "BaseInterface.kernel_class",
        "shell_class": "BaseInterface.shell_class",
    }
    flags = Application.flags | {
        "quiet": ({"BaseInterface": {"quiet": True}}, "Only send stdout/stderr to output stream."),
        "automagic": (
            {"InteractiveShell": {"automagic": True}},
            "Turn on the auto calling of magic commands. Type %%magic at the IPython  prompt  for  more information.",
        ),
        "no-automagic": ({"InteractiveShell": {"automagic": False}}, "Turn off the auto calling of magic commands."),
    }

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

    handle_in_shell_thread = traitlets.List(
        traitlets.UseEnum(MsgType), [MsgType.comm_msg, MsgType.comm_open, MsgType.comm_close]
    ).tag(config=True)
    """
    A list of `MsgType` that are always handled in the shell's thread (typically the _MainThread_).
    """

    handle_in_thread = traitlets.Dict(key_trait=traitlets.UseEnum(MsgType), value_trait=traitlets.Unicode())
    """
    A mapping of `MsgType` to the name of a separate caller (thread) in which to run the handler.
    """

    host = None

    _initialized = False
    _instance: Self | None = None
    _zmq_context = None
    _handler_cache: ClassVar[dict[tuple[str | None, MsgType, Callable], HandlerType]] = {}

    # the kernel class, as an importstring
    kernel_class: traitlets.Type[type[Kernel], type[Kernel] | str] = traitlets.Type("async_kernel.Kernel").tag(  # pyright: ignore[reportAssignmentType]
        config=True
    )
    "The Kernel subclass to be used."

    kernel: Fixed[Self, Kernel] = Fixed(lambda c: c["owner"].kernel_class())
    "The kernel."

    shell_class: traitlets.Type[type[AsyncInteractiveShell], type[AsyncInteractiveShell] | str] = traitlets.Type(
        "async_kernel.asyncshell.AsyncInteractiveShell"
    ).tag(  # pyright: ignore[reportAssignmentType]
        config=True
    )
    "The class to use for shells and subshells."

    shell: Fixed[Self, AsyncInteractiveShell] = Fixed(
        # A work-around to avoid infinite recursion during initialization of the main shell.
        lambda c: c["owner"].shell_class(protected=True, __mainshell__=True),
        created=lambda c: c["obj"].__init__(protected=True),
    )
    "The mainshell."

    subshells: Fixed[Self, dict[str, AsyncInteractiveShell]] = Fixed(dict)
    "A dict of subshells"

    displayhook: Fixed[Self, AsyncDisplayHook] = Fixed("async_kernel.compat.ipython.AsyncDisplayHook")
    ""

    event_started = Fixed(Event)
    "An event that occurs when the kernel is started."

    event_stopped = Fixed(Event)
    "An event that occurs when the kernel is stopped."

    supported_features = traitlets.List(traitlets.Unicode()).tag(config=True)
    "A list of features supported by the kernel."

    help_links = traitlets.List(trait=traitlets.Dict()).tag(config=True)
    "A list of links provided kernel info request."

    _restart = False

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

    @traitlets.default("supported_features")
    def _default_supported_features(self) -> list[str]:
        features = ["kernel subshells"]
        if self.kernel.debugger:
            features.append("debugger")
        return features

    @traitlets.default("handle_in_thread")
    def _default_handle_in_thread(self) -> dict[MsgType, str]:
        return {
            MsgType.inspect_request: "language_server",
            MsgType.complete_request: "language_server",
            MsgType.is_complete_request: "language_server",
        }

    @classmethod
    @override
    def initialized(cls) -> bool:
        """Has an instance been created and initialized?"""
        return getattr(cls._instance, "_initialized", False)

    @classmethod
    @override
    def instance(cls) -> Self:
        "Get the singleton instance that was created using `launch_instance`."
        if not cls._instance:
            msg = "An instance does not exist!"
            raise RuntimeError(msg)
        if not isinstance(cls._instance, cls):
            msg = f"An instance exists but it is not an instance of {cls}!"
            raise TypeError(msg)
        return cls._instance

    @classmethod
    @override
    def clear_instance(cls) -> None:
        raise NotImplementedError

    @classmethod
    @override
    def launch_instance(
        cls,
        argv: list[str] | None | NoValue = None,  # pyright: ignore[reportInvalidTypeForm]
        settings: dict | None = None,
        **kwargs: Any,
    ) -> None:
        app = None
        if BaseInterface._instance:
            msg = "An interface already exists!"
            raise RuntimeError(msg)
        try:
            app = cls(argv, settings, **kwargs)
            app.start()
            app.exit()
        except BaseException:
            del app
            BaseInterface._instance = None
            gc.collect()
            raise

    def __new__(cls, argv: list | None | NoValue = NoValue, settings=None, /, **kwargs) -> Self:  # noqa: ARG004  # pyright: ignore[reportInvalidTypeForm]
        if BaseInterface._instance:
            msg = "An interface already exists!"
            raise RuntimeError(msg)
        BaseInterface._instance = super().__new__(cls, **kwargs)
        return BaseInterface._instance

    def __init__(self, argv: list | None | NoValue = NoValue, settings=None, /, **kwargs) -> None:  # pyright: ignore[reportInvalidTypeForm]
        super().__init__(**kwargs)
        self.initialize(argv)
        if settings:
            self.load_settings(settings)

    @override
    def initialize(self, argv: None | list | NoValue = NoValue) -> None:  # pyright: ignore[reportInvalidTypeForm]
        """
        Initialize the interface.

        This may be called more than once, however configuration is only done on the first call.
        """
        assert self._instance is self
        initialize = not self._initialized
        self._initialized = True

        # Environment variables
        if not os.environ.get("MPLBACKEND"):
            os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"
        if not os.environ.get("UV_PROJECT_ENVIRONMENT"):
            os.environ["UV_PROJECT_ENVIRONMENT"] = sys.prefix

        if initialize:
            super().initialize([] if argv is NoValue else argv)

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
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
        self.callers[Channel.control] = caller.get(name="Control", log=self.log, protected=True)

        async with caller:
            assert BaseInterface._instance is self
            restore_io = None
            try:
                with contextlib.suppress(ValueError, AttributeError):
                    sig = signal.signal(signal.SIGINT, self._signal_handler)
                with anyio.CancelScope() as scope:
                    restore_io = self._patch_io()
                    self._scope = scope
                    self.event_started.set()
                    self.log.info("Kernel started")
                    yield self
            finally:
                self.event_stopped.set()
                with anyio.CancelScope(shield=True):
                    await self._do_shutdown()
                if sig:
                    signal.signal(signal.SIGINT, sig)
                if restore_io:
                    restore_io()
                BaseInterface._instance = None

    async def _do_shutdown(self):

        assert self.event_stopped
        self.log.info("Kernel shutdown started")

        for subshell in set(self.subshells.values()):
            subshell.stop(force=True)
        self.shell.stop(force=True)
        self._handler_cache.clear()

        for comm in tuple(self.kernel.comm_manager.comms.values()):
            comm.close(deleting=True)
        self.kernel.comm_manager.comms.clear()

        await anyio.sleep(0.1)
        self.log.info("Kernel shutdown complete")

    @enable_signal_safety
    def _signal_handler(self, signum, frame: FrameType | None) -> None:
        self.last_interrupt_frame = frame
        self.interrupt()
        self.last_interrupt_frame = None
        raise KernelInterrupt

    def _patch_io(self) -> Callable[[], None]:
        original_io = sys.stdout, sys.stderr, sys.displayhook, builtins.input, getpass.getpass

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
        sys.displayhook = self.displayhook
        return restore

    async def run(self, *, stopped: Callable[[], Any] | None = None) -> None:
        """
        Run the kernel.

        Args:
            stopped: An optional callback that is called when the kernel has stopped.

        This method requires that a [Caller][async_kernel.caller.Caller] instance does not already exist in the current thread.
        """
        try:
            async with self:
                await self.event_stopped
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
            self.event_stopped.set()

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

    def get_shell(self, subshell_id: str | None | NoValue = NoValue) -> AsyncInteractiveShell:  # pyright: ignore[reportInvalidTypeForm]
        """
        Get a shell by `subshell_id`.

        Args:
            subshell_id: The id of an existing subshell.
        """
        if (subshell_id := subshell_id if subshell_id is not NoValue else ShellPendingManager.active_id()) is None:
            return self.shell
        return self.subshells.get(subshell_id) or self.shell

    async def create_subshell(self, *, protected: bool = True) -> AsyncInteractiveShell:
        """
        Create a subshell.

        Use [`subshell.stop(force=True)`][async_kernel.asyncshell.AsyncInteractiveSubshell.stop] to stop a
        protected subshell when it is no longer required.

        Args:
            protected: Protect the subshell from accidental deletion.
        Tip:
            - `await shell.ready` to ensure the shell is 'ready'.
        """

        return self.shell_class(protected=protected)

    def _subshell_created(self, shell: AsyncInteractiveShell) -> None:
        "Called by `AsyncInteractiveShell.__init__`"
        if shell.subshell_id:
            self.subshells[shell.subshell_id] = shell

    def _subshell_stopped(self, shell: AsyncInteractiveShell) -> None:
        "Called by `AsyncInteractiveShell.stop`"
        if subshell_id := shell.subshell_id:
            self.subshells.pop(subshell_id, None)
        for key in list(self._handler_cache):
            if key[0] == subshell_id:
                self._handler_cache.pop(key, None)

    def _get_handler(
        self, job: Job, send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]], iopub_send: Callable
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
            handler: HandlerType = getattr(self.kernel, msg_type)

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

        self.log.debug("%s %s %s %s", msg_type, run_mode, handler, job)

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
        parent = parent or utils.get_parent_message()
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


class HasInterface:
    """
    A mixin class providing a reference to the global [kernel interface][async_kernel.interface.base.BaseInterface].

    This class is designed to be compatible with [Configurable][] objects enabling the sharing
    of configuration and log. The global _kernel interface_ must exist before creating subclass
    instances using this mixin.
    """

    parent: Fixed[Any, BaseInterface] = final(
        Fixed(
            lambda _: BaseInterface.instance(),
            use_weakref=True,
            set_mode="ignore",
        )
    )
    """
    The global instance of the [interface][async_kernel.interface.base.BaseInterface].

    Setting `parent` is ignored.
    """

    @property
    def config(self) -> Config:
        """
        A reference to the `parent.config`.

        Setting the config will update `parent.config`instead of replacing it.
        """
        return self.parent.config

    @config.setter
    def config(self, value: Config) -> None:
        pass

    def __init_subclass__(cls, **kwargs) -> None:

        if cls.parent is not HasInterface.parent or cls.config is not HasInterface.config:
            replaced = [k for k in ["parent", "config"] if getattr(cls, k) is not getattr(HasInterface, k)]
            msg = f"Parameter override detected for class `{cls.__name__}`!"
            if len(replaced) == 2:
                msg = f"{msg}\nTip: Make `HasInterface` the first inherited class (left-most)."
            else:
                msg = f"{msg}\nThe parameter named {replaced[0]!r} must not be overloaded."
            raise TypeError(msg)
        super().__init_subclass__(**kwargs)
        if issubclass(cls, Configurable):
            BaseInterface.classes.insert(0, cls)

    def __new__(cls, *args, **kwargs) -> Self:

        if not BaseInterface.initialized():
            msg = "A global BaseInterface has not been created yet!"
            raise RuntimeError(msg)
        if (new := super().__new__) is object.__new__:
            inst = new(cls)
        try:
            inst = new(cls, *args, **kwargs)
        except TypeError:
            if args or kwargs:
                inst = new(cls)
            else:
                raise
        inst.parent  # noqa: B018 # touch
        return inst
