"""The base class definition to interface with the kernel."""

from __future__ import annotations

import ast
import gc
import importlib.util
import logging
import os
import sys
import weakref
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, Self, final
from uuid import uuid4

import anyio
from aiologic import Event
from aiologic.lowlevel import AsyncLibraryNotFoundError, create_async_waiter, current_async_library
from traitlets import traitlets
from traitlets.config import Config, Configurable
from traitlets.config.application import Application, ClassesType
from typing_extensions import override

import async_kernel
import async_kernel.event_loop
from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed
from async_kernel.pending import Pending, ProtectedPending
from async_kernel.typing import (
    Backend,
    BuffersType,
    Channel,
    Content,
    Hosts,
    Job,
    Message,
    MsgHeader,
    MsgType,
    MsgTypeNoReply,
    NoValue,
    RunSettings,
    T,
    T_interface_co,
    T_shell_co,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from async_kernel.client.base import BaseKernelClient
    from async_kernel.kernel import Kernel

__all__ = ["BaseInterface", "BaseMessageApplication", "HasInterface", "PendingMessage"]


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
    """An instance of a Python dict which converts string values to Python literals."""

    @override
    def item_from_string(self, s: str) -> dict:
        d = super().item_from_string(s)
        for k, v in d.items():
            try:
                d[k] = ast.literal_eval(v)
            except ValueError:
                pass
        return d


class PendingMessage(Pending[Message[T]], Generic[T]):
    @property
    def msg_id(self) -> str:
        return self.metadata["parent"]["header"]["msg_id"]


class BaseMessageApplication(Application, anyio.AsyncContextManagerMixin):
    """The base application for kernel interfaces and clients."""

    kernel_name = traitlets.Unicode("async").tag(config=True)
    "The kernels name."

    started = Fixed(ProtectedPending)
    "A Pending that is set when the application has started."

    stopping = Fixed(ProtectedPending)
    """
    A Pending that is set when stop is called.
    """
    stopped: Fixed[Self, ProtectedPending] = Fixed(
        ProtectedPending, created=lambda c: c["obj"].add_done_callback(c["owner"]._on_stopped)
    )
    """
    A Pending that is set once the application is stopped.
    """
    callers: Fixed[Self, dict[Literal[Channel.shell, Channel.control], Caller]] = Fixed(dict)
    "The callers used by the messaging application."

    aliases: dict[str | tuple[str, ...], str] = Application.aliases | {  # pyright: ignore[reportIncompatibleVariableOverride]
        ("name", "n"): "BaseMessageApplication.kernel_name"
    }

    session_id = Fixed(lambda _: str(uuid4()))
    "Used to identify this object in messages."

    ""
    log: logging.Logger

    _pending_messages: Fixed[Self, dict[str, PendingMessage[Any]]] = Fixed(dict)

    @property
    def summary(self) -> str:
        return f"name={self.kernel_name!r}"

    @asynccontextmanager
    async def __asynccontextmanager__(self, *, set_started=True) -> AsyncGenerator[Self]:
        # Thread: shell
        if async_kernel.utils.PYTEST_LOG_CLI_DEBUG:
            # We apply some patches when pytest logging / debugging pytest so that log messages
            # aren't sent to stdout, but do get sent to to the cli.
            self.log_level = 10
            self.log.setLevel(logging.DEBUG)
            for handler in self.log.handlers:
                handler.setLevel(logging.WARNING if handler.name == "console" else logging.DEBUG)
            for handler in logging.getLogger().handlers:
                if handler.__class__ is logging.StreamHandler:
                    self.log.addHandler(handler)

        if self.stopped.done():
            msg = "Stopped early"
            raise RuntimeError(msg)

        channels_started, stop_channels = create_async_waiter(), Event()
        async with Caller() as caller, caller.get(name="Control") as caller_ctrl:
            self.callers[Channel.shell] = caller
            self.callers[Channel.control] = caller_ctrl
            pen_channels = caller_ctrl.call_soon(self._open_channels, channels_started.wake, stop_channels)
            await channels_started
            try:
                with anyio.CancelScope() as scope:
                    self._force_stop = lambda: caller.call_direct(scope.cancel, "Force stop")
                    if set_started:
                        self._started()  # pragma: no cover
                    yield self
            finally:
                del self._force_stop
                self.stop()
                self.stopped.set_result(None)
                stop_channels.set()
                await pen_channels.wait(shield=True)
                del pen_channels

    async def _open_channels(self, ready: Callable[[], Any], stop: Awaitable, /) -> None:
        ready()
        await stop

    def _started(self) -> None:
        self.log.info("Interface started: %s", self.summary)
        self.started.set_result(None)

    def _on_stopped(self, _) -> None:
        self.log.info("%s, stopped", self)

    def _force_stop(self) -> None:
        pass

    def _handle_reply(self, msg: Message) -> None:
        """A handler for incoming messages."""
        # Thread: undefined
        if (parent := msg.get("parent_header")) and (f := self._pending_messages.pop(parent["msg_id"], None)):
            self.log.debug("Received %s %s", msg["header"]["msg_type"], msg)
            f.set_result(msg)

    def _handle_request(self, job: Job) -> None:
        raise NotImplementedError

    def stop(self, force=False) -> None:
        """Stop the kernel and this interface."""
        if not self.stopped.done():
            self.stopping.set_result(None)
            if not self.callers:
                self.stopped.set_result(None)
            if force:
                self._force_stop()
            self.log.info("%s, stopping", self)

    def msg(
        self,
        msg_type: str | MsgType,
        content: T | None = None,
        *,
        channel: Channel,
        parent: Message | dict[str, Any] | None = None,
        header: MsgHeader | dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Message[T]:
        """Create a new message."""
        parent = parent or utils.get_parent_message()
        if header is None:
            header = MsgHeader(
                date=datetime.now(UTC),
                msg_id=str(uuid4()),
                msg_type=msg_type,
                session=self.session_id,
                username="",
                version=async_kernel.kernel_protocol_version,
            )
        buffers = content.pop("buffers", None) if content else None  # pyright: ignore[reportAttributeAccessIssue]
        return Message(
            channel=channel,
            header=header,  # pyright: ignore[reportArgumentType]
            parent_header=extract_header(parent),  # pyright: ignore[reportArgumentType]
            content={} if content is None else content,
            metadata=metadata if metadata is not None else {},
            buffers=buffers,
        )

    @override
    def print_help(self, classes: bool = False) -> None:
        from async_kernel.compat.attr_docs import get_attr_docs  # noqa: PLC0415

        # Copy trailing docstrings into trait.help.
        for cls in self.classes:
            try:
                for name, value in get_attr_docs(cls).items():
                    if value and isinstance(trait := getattr(cls, name), traitlets.TraitType) and not trait.help:
                        trait.help = value
            except OSError:
                continue  # Coverage can cause issues with some files.
        super().print_help(classes)

    def send_message(
        self,
        msg: Message,
        buffers: BuffersType = None,
        ident: bytes | list[bytes] | None = None,
    ) -> PendingMessage[Content]:
        """Sends the message to the other side (client for kernel and vice versa) and returns a PendingMessage."""
        if MsgType(msg["header"]["msg_type"]) in MsgTypeNoReply:
            msg_ = f"{msg['header']['msg_type']} does not send a reply! Use `send_message_no_reply` instead."
            raise TypeError(msg_)
        self.log.debug("Send mssage %s %s", msg["header"]["msg_type"], msg)
        self._pending_messages[msg["header"]["msg_id"]] = pen = PendingMessage()
        pen.metadata.update(parent=self._send_msg(msg, buffers, ident))
        return pen

    def send_message_no_reply(
        self,
        msg: Message,
        buffers: BuffersType = None,
        ident: bytes | list[bytes] | None = None,
    ) -> Message:
        """Sends a message without expecting a reply.

        This could be of two categories:
            1. The reply to a request.
            2. A message of a type that does not have a reply such as comm_open, comm_close, comm_msg.
        """
        return self._send_msg(msg, buffers, ident)

    def _send_msg(
        self,
        msg: Message,
        buffers: BuffersType = None,
        ident: bytes | list[bytes] | None = None,
    ) -> Message:
        raise NotImplementedError


class BaseInterface(BaseMessageApplication, Generic[T_shell_co]):
    """The base class for kernel interface (singleton).

    The interface creates the kernel and provides external communication. It is also
    the parent object for all objects that subclass from `HasInterface`. Configurable
    objects that subclass from `HasInterface` inherit their configuration from the
    interface (Application).

    Usage:
        launch:
            ```python
            Interface.launch_instance()
            ```
        async context:
            ```python
            async with Interface() as interface:
                interface.kernel
                ...
            ```
    """

    classes: ClassesType = final([])
    "The classes registered with the interface."

    aliases = {
        "launcher": "BaseInterface.launcher",
        "timeout": "BaseShell.timeout",
        "kernel_class": "BaseInterface.kernel_class",
        "shell_class": "BaseInterface.shell_class",
        "help_links": "Kernel.help_links",
        "supported_features": "Kernel.supported_features",
        "interface_class": "BaseInterface.interface_class",
    } | BaseMessageApplication.aliases
    ""
    flags = {
        "quiet": ({"BaseInterface": {"quiet": True}}, "Only send stdout/stderr to output stream."),
        "no-quiet": ({"BaseInterface": {"quiet": False}}, "Only send stdout/stderr to output stream."),
    } | BaseMessageApplication.flags
    ""

    host: traitlets.TraitType[Hosts | None, Hosts | None] = traitlets.UseEnum(
        Hosts, default_value=None, allow_none=True
    ).tag(config=True)
    "The name of a (gui) event loop (if one is used)."

    host_options = DictValueLiteralEval(allow_none=True).tag(config=True)
    "Options for starting the loop."

    backend: traitlets.TraitType[Backend, Backend] = traitlets.UseEnum(Backend).tag(config=True)
    "The type of asynchronous backend used. Options are 'asyncio' or 'trio'."

    backend_options = DictValueLiteralEval(allow_none=True).tag(config=True)
    "Options for starting the backend."

    interface_class: traitlets.Type[type[Self], type[Self] | str] = traitlets.Type(
        "async_kernel.interface.base.BaseInterface"
    ).tag(  # pyright: ignore[reportAssignmentType]
        config=True
    )
    "The interface class to use when launching."

    kernel_class: traitlets.Type[type[Kernel[Self, T_shell_co]], type[Kernel[Self, T_shell_co]] | str] = traitlets.Type(
        "async_kernel.Kernel"
    ).tag(  # pyright: ignore[reportAssignmentType]
        config=True
    )
    "The Kernel class to use when creating the kernel."

    shell_class: traitlets.Type[type[T_shell_co], type[T_shell_co] | str] = traitlets.Type(
        "async_kernel.shell.ipshell.IPShell", "async_kernel.shell.BaseShell"
    ).tag(  # pyright: ignore[reportAssignmentType]
        config=True
    )
    "The class to use for shells and subshells."

    client_class: traitlets.Type[type[BaseKernelClient[Self]], type[BaseKernelClient[Self]] | str] = traitlets.Type(
        klass="async_kernel.client.base.BaseKernelClient"
    ).tag(  # pyright: ignore[reportAssignmentType]
        config=True
    )

    quiet = traitlets.Bool(True).tag(config=True)
    "Only send stdout/stderr to output stream."

    launcher = traitlets.Unicode("").tag(config=True)
    "The value used to import the interface using [async_kernel.kernelspec.import_launcher][]."

    kernel: Fixed[Self, Kernel[Self, T_shell_co]] = Fixed(
        lambda c: c["owner"].kernel_class(c["owner"], c["owner"].shell_class)
    )
    """The kernel."""

    client: Fixed[Self, BaseKernelClient[Self]] = Fixed(
        lambda c: c["owner"].client_class(),
        created=lambda c: c["obj"].set_interface(c["owner"]),  # Touch interface to lock it in.
    )
    """A client that is started with this interface."""

    shell: Fixed[Self, T_shell_co] = Fixed(lambda c: c["owner"].kernel.main_shell)
    "The main shell."

    _instance: Self | None = None

    @property
    @override
    def summary(self) -> str:
        return f"name={self.kernel_name!r} backend={str(self.backend)!r}"

    @traitlets.default("backend")
    def _default_backend(self) -> Backend:
        try:
            return Backend(current_async_library())
        except AsyncLibraryNotFoundError:
            if (
                not self.host
                and not self.trait_has_value("backend_options")
                and (importlib.util.find_spec("winloop") or importlib.util.find_spec("uvloop"))
            ):
                self.backend_options["use_uvloop"] = True
            return Backend.asyncio

    @traitlets.default("shell_class")
    def _default_shell_class(self):
        # We use a method to delay IPython import until it is needed
        from async_kernel.shell.ipshell import IPShell  # noqa: PLC0415

        return IPShell

    @classmethod
    @override
    def initialized(cls) -> bool:
        """Has an instance been created?"""
        return cls._instance is not None

    @classmethod
    @override
    def instance(cls) -> T_interface_co:
        """Get the singleton instance that was created using `launch_instance`."""
        if not cls._instance:
            msg = "An instance does not exist!"
            raise RuntimeError(msg)
        if not isinstance(cls._instance, cls):
            msg = f"An instance exists but it is not an instance of {cls}!"
            raise TypeError(msg)
        return cls._instance  # pyright: ignore[reportReturnType]

    @classmethod
    @override
    def clear_instance(cls) -> None:
        raise NotImplementedError

    @classmethod
    @override
    def launch_instance(
        cls,
        argv: list[str] | None = None,
        kernel_class: type[Kernel[Self, T_shell_co]] | None = None,
        shell_class: type[T_shell_co] | None = None,
        **kwargs: Any,
    ) -> None:
        app = e = None
        if BaseInterface._instance:
            msg = "An interface already exists!"
            raise RuntimeError(msg)
        try:
            app = cls(argv, kernel_class=kernel_class, shell_class=shell_class, **kwargs)
            app.start()
            app.exit()
        except BaseException as e_:
            e = e_
        finally:
            if app:
                app.stopped.set_result(None)
                app.stop()
            del app
            gc.collect()
            if e:
                raise e

    def __new__(cls, argv: list | NoValue | None = NoValue, /, **kwargs) -> Self:  # noqa: ARG004  # pyright: ignore[reportInvalidTypeForm]
        if BaseInterface._instance:
            msg = "An interface already exists!"
            raise RuntimeError(msg)
        BaseInterface._instance = inst = super().__new__(cls, **kwargs)
        return inst

    def __init__(
        self,
        argv: list | NoValue | None = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        /,
        *,
        kernel_class: type[Kernel[Self, T_shell_co]] | str | None = None,
        shell_class: type[T_shell_co] | str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        for name, value in [("kernel_class", kernel_class), ("shell_class", shell_class)]:
            if value:
                self.set_trait(name, value)
        self.initialize(argv)

    @override
    def _on_stopped(self, _) -> None:
        if BaseInterface._instance is self:
            BaseInterface._instance = None
        super()._on_stopped(_)

    @override
    def initialize(self, argv: list | NoValue | None = NoValue) -> None:  # pyright: ignore[reportInvalidTypeForm]
        """Initialize the interface **DO NOT CALL DIRECTLY**."""
        assert self._instance is self

        def initialized(argv: Any = NoValue) -> None:
            msg = "Already initialized!"
            raise RuntimeError(msg)

        self.initialize = initialized

        # Environment variables
        if not os.environ.get("MPLBACKEND"):
            os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"
        if not os.environ.get("UV_PROJECT_ENVIRONMENT"):
            os.environ["UV_PROJECT_ENVIRONMENT"] = sys.prefix
        self.parse_command_line([] if argv is NoValue else argv)
        self.interface_class = self.__class__

    @override
    def start(self) -> None:
        """Start the interface blocking until it stops.

        Warning:
            - Running in a thread other than the 'MainThread' is permitted, but discouraged.
            - Blocking calls can only be interrupted in the 'MainThread' because
                [*'threads cannot be destroyed, stopped, suspended, resumed, or interrupted'*](https://docs.python.org/3/library/threading.html#module-threading).
            - Some libraries may assume the call is occurring in the 'MainThread'.
            - If there is an `asyncio` or `trio` event loop already running in the desired thread;
                start asynchronously instead (`async with interface: ...`).
        """
        if BaseInterface._instance is not self:
            msg = "This interface is not the global instance!"
            raise RuntimeError(msg)

        settings = RunSettings(
            backend=self.backend,
            backend_options=self.backend_options,
            host=self.host,
            host_options=self.host_options,
        )
        try:
            async_kernel.event_loop.run(self.run, (), settings)
        finally:
            if BaseInterface._instance is self:
                BaseInterface._instance = None

    @override
    def exit(self, exit_status: int | str | None = 0) -> None:
        self.stopped.set_result(None)
        return super().exit(exit_status)

    @asynccontextmanager
    async def __asynccontextmanager__(self, *, set_started=True) -> AsyncGenerator[Self]:

        def cache_iopub_send(*args, __send__=self.iopub_send, **kwargs) -> None:  # pragma: no cover
            # Cache iopub messages, send when started or discard if stopped early.
            self.started.add_done_callback(lambda _: not self.stopping.done() and __send__(*args, **kwargs))

        self.backend = Backend(current_async_library())
        self.log.info("Starting kernel interface")
        self.iopub_send = cache_iopub_send
        self.started.add_done_callback(lambda _: delattr(self, "iopub_send"))
        try:
            async with super().__asynccontextmanager__(set_started=False), self.kernel, self.client:
                if set_started:
                    self._started()
                yield self
        finally:
            if BaseInterface._instance is self:
                BaseInterface._instance = None

    async def run(self, *, stopped: Callable[[], Any] | None = None) -> None:
        """Run the kernel.

        Args:
            stopped: An optional callback that is called when the kernel has stopped.

        This method requires that a [Caller][async_kernel.caller.Caller] instance does not already exist in the current thread.
        """
        try:
            async with self:
                await self.stopping
        finally:
            if stopped:
                stopped()

    def input_request(self, prompt: str, *, password=False) -> PendingMessage[Content]:
        job = utils.get_job()
        if not job["msg"].get("content", {}).get("allow_stdin", False):
            msg = "Stdin is not allowed in this context!"
            raise RuntimeError(msg)
        pen_reply = self.send_message(
            self.msg(
                MsgType.input_request,
                content=Content(prompt=prompt, password=password),
                parent=job["msg"],
                channel=Channel.stdin,
                # The client is assumed to have set the 'identity' of the stdin socket to 'session.bsession'.
            ),
            ident=job["msg"]["header"]["session"].encode(),
        )
        if current_pen := self.callers[Channel.shell].current_pending():
            current_pen.add_done_callback(lambda _: pen_reply.cancel(""))
        return pen_reply

    def iopub_send(
        self,
        msg_or_type: MsgType | Message[dict[str, Any]] | dict[str, Any] | str,
        *,
        content: Content | None = None,
        metadata: dict[str, Any] | None = None,
        parent: dict[str, Any] | MsgHeader | NoValue | None = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        ident: bytes | list[bytes] | None = None,
        buffers: BuffersType = None,
    ) -> None:
        if parent is NoValue:
            parent = async_kernel.utils.get_parent_message()
        if isinstance(msg_or_type, dict):
            assert MsgType(msg_or_type["header"]["msg_type"])
        else:
            msg_or_type = self.msg(
                msg_type=MsgType(msg_or_type),
                content=content,
                parent=parent,  # pyright: ignore[reportArgumentType]
                metadata=metadata,
                channel=Channel.iopub,
            )
        self.send_message_no_reply(msg_or_type, buffers=buffers, ident=ident)  # pyright: ignore[reportArgumentType]


class HasInterface(Generic[T_interface_co]):
    """A mixin class providing a reference to the global [interface][async_kernel.interface.base.BaseInterface].

    This class is designed to be compatible with [Configurable][] objects enabling the sharing
    of configuration and log objects. The global _interface_ must exist before creating subclass
    instances using this mixin.
    """

    _interface: weakref.ref

    @property
    def parent(self) -> T_interface_co:
        """The interface at the time of creation."""
        return self._interface()  # pyright: ignore[reportReturnType]

    @parent.setter
    def parent(self, value: Any):
        pass

    @property
    def config(self) -> Config:
        """A reference to the `parent.config`.

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

        # Register class for configuration
        if issubclass(cls, Configurable):
            BaseInterface.classes.insert(0, cls)

    def __new__(cls, *args, **kwargs) -> Self:

        if not (interface := BaseInterface._instance):  # pyright: ignore[reportPrivateUsage]
            msg = "A global BaseInterface has not been created yet!"
            raise RuntimeError(msg)
        inst = new_(cls) if (new_ := super().__new__) is object.__new__ else new_(cls, *args, **kwargs)
        inst._interface = weakref.ref(interface)
        return inst
