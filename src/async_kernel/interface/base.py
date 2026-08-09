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
from typing import TYPE_CHECKING, Any, Generic, Literal, Self, final

import anyio
from aiologic import BinarySemaphore
from aiologic.lowlevel import AsyncLibraryNotFoundError, current_async_library
from traitlets import import_item, traitlets
from traitlets.config import Config, Configurable
from traitlets.config.application import Application, ClassesType
from typing_extensions import override

import async_kernel
import async_kernel.event_loop
from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed
from async_kernel.pending import PendingMessage, ProtectedPending
from async_kernel.typing import (
    Backend,
    BuffersType,
    Channel,
    Content,
    Hosts,
    Message,
    MsgHeader,
    MsgType,
    NoValue,
    RunSettings,
    T_interface_co,
    T_shell_co,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from async_kernel.connection.base import Connection
    from async_kernel.kernel import Kernel

__all__ = ["HasInterface", "Interface", "PendingMessage"]


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


class Interface(Application, anyio.AsyncContextManagerMixin, Generic[T_shell_co]):
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

    kernel_name = traitlets.Unicode("async").tag(config=True)
    "The kernel's name."

    classes: ClassesType = final([])
    "The classes registered with the interface."

    aliases: dict[str | tuple[str, ...], str] = (  # pyright: ignore[reportIncompatibleVariableOverride]
        Application.aliases
        | {
            ("name", "n"): "Interface.kernel_name",
            ("f", "connection_file"): "ZMQConnection.connection_file",
            "launcher": "Interface.launcher",
            "timeout": "BaseShell.timeout",
            "kernel_class": "Interface.kernel_class",
            "shell_class": "Interface.shell_class",
            "help_links": "Kernel.help_links",
            "supported_features": "Kernel.supported_features",
            "interface_class": "Interface.interface_class",
            "host": "Interface.host",
            "host_options": "Interface.host_options",
            "backend_options": "Interface.backend_options",
            "backend": "Interface.backend",
        }
        | Application.aliases
    )
    ""
    flags = {
        "quiet": ({"Interface": {"quiet": True}}, "Only send stdout/stderr to output stream."),
        "no-quiet": ({"Interface": {"quiet": False}}, "Only send stdout/stderr to output stream."),
    } | Application.flags
    ""

    host: traitlets.TraitType[Hosts | None, Hosts | None] = traitlets.UseEnum(
        Hosts, default_value=None, allow_none=True
    ).tag(config=True)
    """The name of a (gui) event loop (if one is used)."""

    host_options = DictValueLiteralEval(allow_none=True).tag(config=True)
    """Options for starting the loop."""

    backend: traitlets.TraitType[Backend, Backend] = traitlets.UseEnum(Backend).tag(config=True)
    """The type of asynchronous backend used. Options are 'asyncio' or 'trio'."""

    backend_options = DictValueLiteralEval(allow_none=True).tag(config=True)
    """Options for starting the backend."""

    interface_class: traitlets.Type[type[Self], type[Self] | str] = traitlets.Type(
        "async_kernel.interface.base.Interface"
    ).tag(  # pyright: ignore[reportAssignmentType]
        config=True
    )
    """The interface class to use when launching."""

    kernel_class: traitlets.Type[type[Kernel[Self, T_shell_co]], type[Kernel[Self, T_shell_co]] | str] = traitlets.Type(
        "async_kernel.Kernel"
    ).tag(  # pyright: ignore[reportAssignmentType]
        config=True
    )
    """The Kernel class to use when creating the kernel."""

    shell_class: traitlets.Type[type[T_shell_co], type[T_shell_co] | str] = traitlets.Type(
        "async_kernel.shell.ipshell.IPShell", "async_kernel.shell.BaseShell"
    ).tag(  # pyright: ignore[reportAssignmentType]
        config=True
    )
    """The class to use for shells and subshells."""

    quiet = traitlets.Bool(True).tag(config=True)
    """Only send stdout/stderr to output stream."""

    launcher = traitlets.Unicode("").tag(config=True)
    """The value used to import the interface using [async_kernel.kernelspec.import_launcher][]."""

    force_shutdown_delay = traitlets.Float(2 if not utils.LAUNCHED_BY_DEBUGPY else 1e6)
    "The time in seconds to wait after stop is called before stop with force enabled is called."

    callers: Fixed[Self, dict[Literal[Channel.shell, Channel.control], Caller]] = Fixed(dict)
    """The callers used by the messaging application."""

    started = Fixed(ProtectedPending)
    """A Pending that is set when the application has started."""

    stopping = Fixed(ProtectedPending)
    """A Pending that is set when stop is called."""

    stopped: Fixed[Self, ProtectedPending] = Fixed(
        ProtectedPending, created=lambda c: c["obj"].add_done_callback(c["owner"]._on_stopped)
    )
    """A Pending that is set once the application is stopped."""

    kernel: Fixed[Self, Kernel[Self, T_shell_co]] = Fixed(
        lambda c: c["owner"].kernel_class(c["owner"], c["owner"].shell_class)
    )
    """The kernel."""

    autostart_connections = traitlets.List().tag(config=True)
    "A list of connections to start with the app."

    _connections: tuple[Connection[Self], ...] = ()
    "The connections to the interface for messaging."

    _connections_lock = Fixed(BinarySemaphore)

    shell: Fixed[Self, T_shell_co] = Fixed(lambda c: c["owner"].kernel.main_shell)
    "The main shell."

    _instance: Self | None = None

    @property
    def summary(self) -> str:
        return f"name={self.kernel_name!r} backend={str(self.backend)!r}"

    @property
    def connections(self) -> tuple[Connection[Self], ...]:
        return self._connections

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

    @traitlets.default("autostart_connections")
    def _default_autostart_connections(self) -> list[str]:
        return ["async_kernel.connection.zmq.ZMQConnection"] if sys.platform != "emscripten" else []

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
        if Interface._instance:
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
        if Interface._instance:
            msg = "An interface already exists!"
            raise RuntimeError(msg)
        Interface._instance = inst = super().__new__(cls, **kwargs)
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

        # Cache iopub until started.
        iopub_cache = []

        def cache_iopub_send(*args, **kwargs) -> None:  # pragma: no cover
            # Cache iopub messages, send when started or discard if stopped early.
            iopub_cache.append((args, kwargs))

        self.iopub_send, self._iopub_cache = cache_iopub_send, iopub_cache

        for name, value in [("kernel_class", kernel_class), ("shell_class", shell_class)]:
            if value:
                self.set_trait(name, value)
        self.initialize(argv)
        if async_kernel.utils.PYTEST_LOG_CLI_DEBUG:  # pragma: no cover
            # We apply some patches when pytest logging / debugging pytest so that log messages
            # aren't sent to stdout, but do get sent to to the cli.
            self.log_level = 10
            self.log.setLevel(logging.DEBUG)
            for handler in self.log.handlers:
                handler.setLevel(logging.WARNING if handler.name == "console" else logging.DEBUG)
            for handler in logging.getLogger().handlers:
                if handler.__class__ is logging.StreamHandler and handler not in self.log.handlers:
                    self.log.addHandler(handler)

    def _on_stopped(self, _) -> None:
        if Interface._instance is self:
            Interface._instance = None
        self.log.info("%s, stopped", self)

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
        if Interface._instance is not self:
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
            self.stopped.set_result(None)

    def stop(self) -> None:
        """Stop the kernel and this interface."""
        if not self.stopped.done():
            self.stopping.set_result(None)
            if not self.callers:
                self.stopped.set_result(None)

    def refresh_connections(self, *new: Connection[Self]) -> None:
        """Refresh the list of connections.

        This method must be called from the control thread.

        Args:
            new: new connections to add.
        """
        with self._connections_lock:
            connections = []
            for c in (*self._connections, *new):
                if c.parent is self and not c.stopped.done() and c not in connections:
                    connections.append(c)
            self._connections = tuple(connections)

    @override
    def exit(self, exit_status: int | str | None = 0) -> None:
        self.stop()
        return super().exit(exit_status)

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

    @asynccontextmanager
    async def __asynccontextmanager__(self, *, set_started=True) -> AsyncGenerator[Self]:
        async def stopping(stopped=self.stopped, log=self.log, timeout=self.force_shutdown_delay) -> None:
            while not stopped.done():
                try:
                    await stopped.wait(timeout=timeout)
                except TimeoutError:
                    log.info("Attempting to initiate force stop")
                    await caller.call_soon(scope.cancel, "Force stop")
                    log.info("Cancel scope call succeeded.")

        try:
            if self.stopped.done():
                msg = "Stopped early"
                raise RuntimeError(msg)
            self.log.info("Starting kernel interface")
            self.backend = Backend(current_async_library())
            async with Caller() as caller:
                self.callers[Channel.shell] = caller
                self.callers[Channel.control] = caller_ctrl = caller.get(name="Control")
                try:
                    with anyio.CancelScope() as scope:
                        self.stopping.add_done_callback(lambda _: caller_ctrl.call_soon(stopping))
                        async with self.kernel:
                            if set_started:
                                await self._started()
                            yield self
                finally:
                    self.stopped.set_result(None)
                    await caller.wait([c.stop() for c in self.connections], shield=True)
        finally:
            self.stopped.set_result(None)

    async def _started(self) -> None:
        for pth in self.autostart_connections:
            try:
                cls: type[Connection[Self]] = import_item(pth)
                self.log.info("Starting connection for %s", pth)
                assert await cls().start().started in self._connections
            except Exception as e:
                self.log.exception("Failed to start connection %s", pth, exc_info=e)
        del self.iopub_send
        while self._iopub_cache:
            self._iopub_cache.reverse()
            args, kwargs = self._iopub_cache.pop()
            self.iopub_send(*args, **kwargs)
        self.log.info("Interface started: %s", self.summary)
        self.started.set_result(None)

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
        connection = job["owner"]()
        pen_reply = connection.send_message(
            connection.msg(
                MsgType.input_request,
                content=Content(prompt=prompt, password=password),
                parent=job["msg"],
                channel=Channel.stdin,
                # The client is assumed to have set the 'identity' of the stdin socket to 'session.bsession'.
            ),
            # ident=job["msg"]["header"]["session"].encode(),
            ident=job["ident"],
        )
        if current_pen := self.callers[Channel.shell].current_pending():
            current_pen.add_done_callback(lambda _: pen_reply.cancel(""))
        return pen_reply

    def iopub_send(
        self,
        msg_or_type: MsgType | Message[Content] | dict[str, Any] | str,
        *,
        content: Content | None = None,
        metadata: dict[str, Any] | None = None,
        parent: dict[str, Any] | MsgHeader | NoValue | None = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        ident: bytes | list[bytes] | None = None,
        buffers: BuffersType = None,
    ) -> None:
        """Send an iopub message to each connection."""
        for c in self._connections:
            try:
                if isinstance(msg_or_type, dict):
                    content = content or msg_or_type.get("content")
                    buffers = buffers or msg_or_type.get("buffers")
                    msg_or_type = msg_or_type["header"]["msg_type"]
                msg = c.msg(
                    msg_type=MsgType(msg_or_type),
                    content=content,
                    parent=parent if parent is not NoValue else async_kernel.utils.get_parent_message(),  # pyright: ignore[reportArgumentType]
                    metadata=metadata,
                    channel=Channel.iopub,
                    buffers=buffers,
                )
                c.send_message_no_reply(msg, ident)
                self.log.debug("iopub_send: msg_type:%r %s", msg_or_type, msg_or_type)
            except Exception as e:
                self.log.exception("iopub_send failed for connection %r", c, exc_info=e)

    def get_connection_info(self) -> list[str]:
        """Ruturns a list of strings for connection details of each active connection which provides it."""
        return [info for connection in self.connections if (info := connection.connection_info())]


class HasInterface(Generic[T_interface_co]):
    """A mixin class providing a reference to the global [interface][async_kernel.interface.base.Interface].

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
            Interface.classes.insert(0, cls)

    def __new__(cls, *args, **kwargs) -> Self:

        if not (interface := Interface._instance):  # pyright: ignore[reportPrivateUsage]
            msg = "A global Interface has not been created yet!"
            raise RuntimeError(msg)
        inst = new_(cls) if (new_ := super().__new__) is object.__new__ else new_(cls, *args, **kwargs)
        inst._interface = weakref.ref(interface)
        return inst
