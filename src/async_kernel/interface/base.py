"""The base class definition to interface with the kernel."""

from __future__ import annotations

import ast
import gc
import importlib.util
import os
import sys
import weakref
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, Self, final
from uuid import uuid4

import anyio
from aiologic.lowlevel import AsyncLibraryNotFoundError, async_sleep_forever, current_async_library
from traitlets import traitlets
from traitlets.config import Config, Configurable
from traitlets.config.application import Application, ClassesType
from typing_extensions import override

import async_kernel
import async_kernel.event_loop
from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed
from async_kernel.pending import Pending
from async_kernel.typing import (
    Backend,
    Channel,
    Hosts,
    Message,
    MsgHeader,
    MsgType,
    NoValue,
    RunSettings,
    T,
    T_interface_co,
    T_shell_co,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from async_kernel.kernel import Kernel
    from async_kernel.typing import Content

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


class BaseInterface(Application, anyio.AsyncContextManagerMixin, Generic[T_shell_co]):
    """
    The base class for kernel interface (singleton).

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

    aliases: dict[str | tuple[str, ...], str] = {  # pyright: ignore[reportIncompatibleVariableOverride]
        ("name", "n"): "BaseInterface.name",
        "launcher": "BaseInterface.launcher",
        "timeout": "BaseShell.timeout",
        "kernel_class": "BaseInterface.kernel_class",
        "shell_class": "BaseInterface.shell_class",
        "help_links": "Kernel.help_links",
        "supported_features": "Kernel.supported_features",
        "interface_class": "BaseInterface.interface_class",
    } | Application.aliases
    ""
    flags = {
        "quiet": ({"BaseInterface": {"quiet": True}}, "Only send stdout/stderr to output stream."),
        "no-quiet": ({"BaseInterface": {"quiet": False}}, "Only send stdout/stderr to output stream."),
    } | Application.flags
    ""

    name = traitlets.Unicode("async").tag(config=True)
    "The name of the kernel used in the kernelspec."

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

    quiet = traitlets.Bool(True).tag(config=True)
    "Only send stdout/stderr to output stream."

    launcher = traitlets.Unicode("").tag(config=True)
    "The value used to import the interface using [async_kernel.kernelspec.import_launcher][]."

    kernel: Fixed[Self, Kernel[Self, T_shell_co]] = Fixed(
        lambda c: c["owner"].kernel_class(c["owner"], c["owner"].shell_class)
    )
    "The kernel."

    callers: Fixed[Self, dict[Literal[Channel.shell, Channel.control], Caller]] = Fixed(dict)
    "The caller associated with the kernel once it has started."

    started = Fixed(Pending)
    "A Pending that is set when the interface has started."

    stopping = Fixed(Pending)
    """
    A Pending that is set when stop is called.
    """

    _instance: Self | None = None
    _zmq_context = None
    last_interrupt_frame = None

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

    @property
    def summary(self) -> str:
        return f"name={self.name!r} backend={str(self.backend)!r}"

    @classmethod
    @override
    def initialized(cls) -> bool:
        """Has an instance been created?"""
        return cls._instance is not None

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
        argv: list[str] | None = None,
        kernel_class: type[Kernel[Self, T_shell_co]] | None = None,
        shell_class: type[T_shell_co] | None = None,
        **kwargs: Any,
    ) -> None:
        app = None
        if BaseInterface._instance:
            msg = "An interface already exists!"
            raise RuntimeError(msg)
        try:
            app = cls(argv, kernel_class=kernel_class, shell_class=shell_class, **kwargs)
            app.start()
            app.exit()
        except BaseException:
            del app
            BaseInterface._instance = None
            gc.collect()
            raise

    def __new__(cls, argv: list | None | NoValue = NoValue, /, **kwargs) -> Self:  # noqa: ARG004  # pyright: ignore[reportInvalidTypeForm]
        if BaseInterface._instance:
            msg = "An interface already exists!"
            raise RuntimeError(msg)
        BaseInterface._instance = super().__new__(cls, **kwargs)
        return BaseInterface._instance

    def __init__(
        self,
        argv: list | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
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
    def initialize(self, argv: None | list | NoValue = NoValue) -> None:  # pyright: ignore[reportInvalidTypeForm]
        """
        Initialize the interface **DO NOT CALL DIRECTLY**.
        """
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
        """
        Start the interface blocking until it stops.

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

    @asynccontextmanager
    async def __asynccontextmanager__(self, *, set_started=True) -> AsyncGenerator[Self]:
        def cache_iopub_send(*args, **kwargs) -> None:  # pragma: no cover
            # Cache iopub messages, send when started or discard if stopped early.
            self.started.add_done_callback(lambda _: not self.stopping.done() and send(*args, **kwargs))

        if self.stopping.done() or self.started.done():
            msg = "Stopped early"
            raise RuntimeError(msg)

        send = self.iopub_send
        self.iopub_send = cache_iopub_send
        self.started.add_done_callback(lambda _: delattr(self, "iopub_send"))

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
        self.backend = Backend(current_async_library())
        try:
            async with caller:
                with anyio.CancelScope() as scope:

                    def stop(_):
                        self.log.info("Stopping kernel")
                        caller.call_later(0.5, scope.cancel, "Stopping kernel")

                    self.stopping.add_done_callback(stop)
                    try:
                        async with self.kernel.running():
                            if set_started:
                                self._started()
                            yield self
                    finally:
                        self.stop()
        finally:
            if BaseInterface._instance is self:
                BaseInterface._instance = None
            self.log.info("Interface stopped")

    def _started(self):
        self.log.info("Interface started: %s", self.summary)
        self.started.set_result(None)

    async def run(self, *, stopped: Callable[[], Any] | None = None) -> None:
        """
        Run the kernel.

        Args:
            stopped: An optional callback that is called when the kernel has stopped.

        This method requires that a [Caller][async_kernel.caller.Caller] instance does not already exist in the current thread.
        """
        try:
            async with self:
                # Wait forever. This will exit when stop is called.
                await async_sleep_forever()
        finally:
            if stopped:
                stopped()

    def stop(self) -> None:
        """
        Stop the kernel and this interface.
        """
        self.stopping.set_result(None)
        if not self.started.done():
            self.started.cancel("Stopped early")
            if BaseInterface._instance is self:
                BaseInterface._instance = None

    def input_request(self, prompt: str, *, password: bool = False) -> str:
        """
        Forward an input request to the frontend.

        Args:
            prompt: The user prompt.
            password: If the prompt should be considered as a password.
        """
        raise NotImplementedError

    def msg(
        self,
        msg_type: str | MsgType,
        *,
        content: T | None = None,
        parent: Message | dict[str, Any] | None = None,
        header: MsgHeader | dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        channel: Channel = Channel.shell,
    ) -> Message[T]:
        """
        Create a new message.
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


class HasInterface(Generic[T_interface_co]):
    """
    A mixin class providing a reference to the global [interface][async_kernel.interface.base.BaseInterface].

    This class is designed to be compatible with [Configurable][] objects enabling the sharing
    of configuration and log objects. The global _interface_ must exist before creating subclass
    instances using this mixin.
    """

    _interface: weakref.ref

    @property
    def parent(self) -> T_interface_co:
        "The interface at the time of creation."
        return self._interface()  # pyright: ignore[reportReturnType]

    @parent.setter
    def parent(self, value: Any):
        pass

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
