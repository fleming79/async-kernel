from __future__ import annotations

import functools
import gc
import importlib.util
import json
import logging
import os
import pathlib
import sys
import traceback
import uuid
from contextlib import asynccontextmanager
from logging import Logger, LoggerAdapter
from pathlib import Path
from types import CoroutineType
from typing import TYPE_CHECKING, Any, Literal, Self

import anyio
import traitlets
from aiologic import Event
from aiologic.lowlevel import current_async_library
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import CUnicode, Dict, HasTraits, Instance, Tuple, UseEnum
from typing_extensions import override

import async_kernel
from async_kernel import Caller, utils
from async_kernel.asyncshell import (
    AsyncInteractiveShell,
    AsyncInteractiveSubshell,
    KernelInterruptError,
    SubshellManager,
    SubshellPendingManager,
)
from async_kernel.comm import CommManager
from async_kernel.common import Fixed
from async_kernel.debugger import Debugger
from async_kernel.interface.interface import InterfaceBase
from async_kernel.typing import (
    Backend,
    Content,
    ExecuteContent,
    HandlerType,
    Job,
    KernelName,
    Message,
    MsgType,
    NoValue,
    RunMode,
    SocketID,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
    from types import CoroutineType


__all__ = ["Kernel", "KernelInterruptError"]


@functools.cache
def cache_wrap_handler(
    subshell_id: str | None,
    send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]],
    runner: Callable[[str | None, Callable[[Job, dict], CoroutineType[Any, Any, None]], HandlerType, Job]],
    handler: HandlerType,
) -> Callable[[Job], CoroutineType[Any, Any, None]]:
    @functools.wraps(handler)
    async def wrap_handler(job: Job) -> None:
        await runner(subshell_id, send_reply, handler, job)

    return wrap_handler


class Kernel(HasTraits, anyio.AsyncContextManagerMixin):
    """
    A Jupyter kernel that supports concurrent execution providing an [IPython InteractiveShell][async_kernel.asyncshell.AsyncInteractiveShell].

    Info:
        Only one instance of a kernel is created at a time per subprocess. The instance can be obtained
        with `Kernel()` or [get_kernel].

    Starting the kernel:
        The kernel should appear in the list of kernels just as other kernels are. Variants of the kernel
        can with custom configuration can be added at the [command line][command.command_line].

        === "From the shell"

            ``` shell
            async-kernel -f .
            ```

        === "Blocking"

            ```python
            Kernel().run()
            ```

        === "Inside a coroutine"

            ```python
            async with Kernel():
                await anyio.sleep_forever()
            ```

    Warning:
        Starting the kernel outside the main thread has the following implicatations:
            - Execute requests won't be run in the main thread.
            - Interrupts via signals won't work, so thread blocking calls in the shell cannot be interrupted.

    Origins:
        - [IPyKernel Kernel][ipykernel.kernelbase.Kernel]
        - [IPyKernel IPKernelApp][ipykernel.kernelapp.IPKernelApp]
        - [IPyKernel IPythonKernel][ipykernel.ipkernel.IPythonKernel]
    """

    _instance: Self | None = None
    _initialised = False

    _settings = Fixed(dict)

    interface = traitlets.Instance(InterfaceBase)
    "The abstraction to communicate with the kernel."

    callers: Fixed[Self, dict[Literal[SocketID.shell, SocketID.control], Caller]] = Fixed(dict)
    "The caller associated with the kernel once it has started."
    ""
    subshell_manager = Fixed(SubshellManager)
    "Dedicated to management of sub shells."

    # Public traits
    anyio_backend: traitlets.Container[Backend] = UseEnum(Backend)  # pyright: ignore[reportAssignmentType]
    "The anyio configured backend used to run the event loops."

    anyio_backend_options: Dict[Backend, dict[str, Any] | None] = Dict(allow_none=True)
    "Default options to use with [anyio.run][]. See also: `Kernel.handle_message_request`."

    help_links = Tuple()
    ""
    quiet = traitlets.Bool(True)
    "Only send stdout/stderr to output stream."

    print_kernel_messages = traitlets.Bool(True)
    "When enabled the kernel will print startup, shutdown and terminal errors."

    connection_file: traitlets.TraitType[Path, Path | str] = traitlets.TraitType()
    """
    JSON file in which to store connection info 
    
    `"kernel-<pid>.json"`

    This file will contain the IP, ports, and authentication key needed to connect
    clients to this kernel. By default, this file will be created in the security dir
    of the current profile, but can be specified by absolute path.
    """

    kernel_name = CUnicode()
    "The kernels name - if it contains 'trio' a trio backend will be used instead of an asyncio backend."

    log = Instance(logging.LoggerAdapter)
    "The logging adapter."

    # Public fixed
    main_shell = Fixed(lambda _: AsyncInteractiveShell.instance())
    "The interactive shell."

    debugger = Fixed(Debugger)
    "Handles [debug requests](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request)."

    comm_manager = Fixed(CommManager)
    "Creates [async_kernel.comm.Comm][] instances and maintains a mapping to `comm_id` to `Comm` instances."

    event_started = Fixed(Event)
    "An event that occurs when the kernel is started."

    event_stopped = Fixed(Event)
    "An event that occurs when the kernel is stopped."

    def __new__(cls, settings: dict | None = None, /) -> Self:  # noqa: ARG004
        #  There is only one instance (including subclasses).
        if not (instance := Kernel._instance):
            Kernel._instance = instance = super().__new__(cls)
        return instance  # pyright: ignore[reportReturnType]

    def __init__(self, settings: dict | None = None, /) -> None:
        if not self._initialised:
            self._initialised = True
            super().__init__()
            if not os.environ.get("MPLBACKEND"):
                os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"
        if settings:
            self.load_settings(settings)

    @override
    def __repr__(self) -> str:
        info = [f"{k}={v}" for k, v in self.settings.items()]
        return f"{self.__class__.__name__}<{', '.join(info)}>"

    @traitlets.default("log")
    def _default_log(self) -> LoggerAdapter[Logger]:
        return logging.LoggerAdapter(logging.getLogger(self.__class__.__name__))

    @traitlets.default("kernel_name")
    def _default_kernel_name(self) -> Literal[KernelName.trio, KernelName.asyncio]:
        try:
            if current_async_library() == "trio":
                return KernelName.trio
        except Exception:
            pass
        return KernelName.asyncio

    @traitlets.default("interface")
    def default_interface(self):
        if sys.platform == "emscripten":
            from async_kernel.interface._callable_interface import CallableInterface  # noqa: PLC0415

            return CallableInterface()

        from async_kernel.interface._zmq_interface import ZMQ_Interface  # noqa: PLC0415

        return ZMQ_Interface()

    @traitlets.default("connection_file")
    def _default_connection_file(self) -> Path:
        return Path(jupyter_runtime_dir()).joinpath(f"kernel-{uuid.uuid4()}.json")

    @traitlets.default("anyio_backend_options")
    def _default_anyio_backend_options(self):
        use_uv = importlib.util.find_spec("winloop") or importlib.util.find_spec("uvloop")
        return {Backend.asyncio: {"use_uvloop": True} if use_uv else {}, Backend.trio: None}

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

    @traitlets.observe("connection_file")
    def _observe_connection_file(self, change) -> None:
        if not self.interface.callers and (path := self.connection_file).exists():
            self.log.debug("Loading connection file %s", path)
            with path.open("r") as f:
                self.load_connection_info(json.load(f))

    @traitlets.validate("connection_file")
    def _validate_connection_file(self, proposal) -> Path:
        return pathlib.Path(proposal.value)

    @property
    def settings(self) -> dict[str, Any]:
        "Settings that have been set to customise the behaviour of the kernel."
        return {k: getattr(self, k) for k in ("kernel_name", "connection_file")} | self._settings

    @property
    def shell(self) -> AsyncInteractiveShell | AsyncInteractiveSubshell:
        """
        The shell given the current context.

        Notes:
            - The `subshell_id` of the main shell is `None`.
        """
        return self.subshell_manager.get_shell()

    @property
    def caller(self) -> Caller:
        "The caller for the shell channel."
        return self.callers[SocketID.shell]

    @property
    def transport(self):
        return getattr(self.interface, "transport", "")

    @transport.setter
    def transport(self, value):
        if not self.interface.callers and self.interface.has_trait("transport"):
            self.interface.set_trait("transport", value)

    @property
    def kernel_info(self) -> dict[str, str | dict[str, str | dict[str, str | int]] | Any | tuple[Any, ...] | bool]:
        "A dict of detail sent in reply to for a 'kernel_info_request'."
        return {
            "protocol_version": async_kernel.kernel_protocol_version,
            "implementation": "async_kernel",
            "implementation_version": async_kernel.__version__,
            "language_info": async_kernel.kernel_protocol_version_info,
            "banner": self.shell.banner,
            "help_links": self.help_links,
            "debugger": not utils.LAUNCHED_BY_DEBUGPY,
            "kernel_name": self.kernel_name,
            "supported_features": ["kernel subshells"],
        }

    def load_settings(self, settings: dict[str, Any]) -> None:
        """
        Load settings into the kernel.

        Permitted until the kernel async context has been entered.

        Args:
            settings:
                key: dotted.path.of.attribute.
                value: The value to set.
        """
        if self.event_started:
            msg = "It is too late to load settings!"
            raise RuntimeError(msg)
        settings_ = self._settings or {"kernel_name": self.kernel_name}
        for k, v in settings.items():
            settings_ |= utils.setattr_nested(self, k, v)
        self._settings.update(settings_)

    def load_connection_info(self, info: dict[str, Any]) -> None:
        """
        Load connection info from a dict containing connection info.

        Typically this data comes from a connection file
        and is called by load_connection_file.

        Args:
            info: Dictionary containing connection_info. See the connection_file spec for details.
        """
        self.interface.load_connection_info(info)

    def run(self, wait_exit: Callable[[], Awaitable] = anyio.sleep_forever, /):
        """
        Run the kernel (blocking).

        Args:
            wait_exit: The kernel will stop when the awaitable is complete.

        Warning:
            Running the kernel in a thread other than the 'MainThread' is permitted, but discouraged.

            - Blocking calls can only be interrupted in the 'MainThread' because [*'threads cannot be destroyed, stopped, suspended, resumed, or interrupted'*](https://docs.python.org/3/library/threading.html#module-threading).
            - Some libraries may assume the call is occurring in the 'MainThread'.
            - If there is an asyncio or trio event loop already running in the 'MainThread. Simply use `async with kernel` instead.
        """
        if getattr(self, "_started", False):
            raise RuntimeError
        self._started = True

        async def _run() -> None:
            async with self:
                await wait_exit()

        if not self.trait_has_value("anyio_backend") and "trio" in self.kernel_name.lower():
            self.anyio_backend = Backend.trio
        backend = self.anyio_backend
        backend_options = self.anyio_backend_options.get(backend)
        anyio.run(_run, backend=backend, backend_options=backend_options)

    @staticmethod
    def stop() -> None:
        """
        A [staticmethod][] to stop the running kernel.

        Once an instance of a kernel is stopped the instance cannot be restarted.
        Instead a new instance should be started.
        """
        if (instance := Kernel._instance) and (stop := getattr(instance, "_stop", None)):
            stop()

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        """Start the kernel in an already running anyio event loop."""
        assert self.main_shell
        self.anyio_backend = Backend(current_async_library())
        try:
            async with self.interface:
                self.callers.update(self.interface.callers)
                with anyio.CancelScope() as scope:
                    self._stop = lambda: self.caller.call_direct(scope.cancel, "Stopping kernel")
                    sys.excepthook = self.excepthook
                    sys.unraisablehook = self.unraisablehook

                    self.comm_manager.patch_comm()
                    try:
                        self.comm_manager.kernel = self
                        self.event_started.set()
                        self.log.info("Kernel started: %s", self)
                        yield self
                    except BaseException:
                        if not scope.cancel_called:
                            raise
                    finally:
                        self.comm_manager.kernel = None
                        self.event_stopped.set()
        finally:
            self.shell.reset(new_session=False)
            self.subshell_manager.stop_all_subshells(force=True)
            self.callers.clear()
            Kernel._instance = None
            AsyncInteractiveShell.clear_instance()
            with anyio.CancelScope(shield=True):
                await anyio.sleep(0.1)
            self.log.info("Kernel stopped: %s", self)
            gc.collect()

    def iopub_send(
        self,
        msg_or_type: Message[dict[str, Any]] | dict[str, Any] | str,
        *,
        content: Content | None = None,
        metadata: dict[str, Any] | None = None,
        parent: Message[dict[str, Any]] | dict[str, Any] | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        ident: bytes | list[bytes] | None = None,
        buffers: list[bytes] | None = None,
    ) -> None:
        """Send a message on the iopub socket."""
        self.interface.iopub_send(
            msg_or_type,
            content=content,
            metadata=metadata,
            parent=parent,
            ident=ident,
            buffers=buffers,
        )

    def topic(self, topic) -> bytes:
        """prefixed topic for IOPub messages."""
        return (f"kernel.{topic}").encode()

    def msg_handler(
        self,
        socket_id: Literal[SocketID.shell, SocketID.control],
        msg_type: MsgType,
        job: Job,
        send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]],
        /,
    ):
        """Schedule a message to be executed."""
        # Note: There are never any active pending trackers in this context.
        try:
            subshell_id = job["msg"]["content"]["subshell_id"]
        except KeyError:
            try:
                subshell_id = job["msg"]["header"]["subshell_id"]  # pyright: ignore[reportTypedDictNotRequiredAccess]
            except KeyError:
                subshell_id = None
        handler = cache_wrap_handler(subshell_id, send_reply, self.run_handler, self.get_handler(msg_type))
        run_mode = self.get_run_mode(msg_type, socket_id=socket_id, job=job)
        match run_mode:
            case RunMode.direct:
                self.callers[socket_id].call_direct(handler, job)
            case RunMode.queue:
                self.callers[socket_id].queue_call(handler, job)
            case RunMode.task:
                self.callers[socket_id].call_soon(handler, job)
            case RunMode.thread:
                self.callers[socket_id].to_thread(handler, job)
        self.log.debug("%s %s %s %s", msg_type, handler, run_mode, job)

    def get_handler(self, msg_type: MsgType) -> HandlerType:
        if not callable(f := getattr(self, msg_type, None)):
            msg = f"A handler was not found for {msg_type=}"
            raise TypeError(msg)
        return f  # pyright: ignore[reportReturnType]

    async def run_handler(
        self,
        subshell_id: str | None,
        send_reply: Callable[[Job, dict], CoroutineType[Any, Any, None]],
        handler: HandlerType,
        job: Job[dict],
    ) -> None:
        """
        Asynchronously run a message handler for a given job, managing reply sending and execution state.

        Args:
            subshell_id: The id of the subshell to set the context of the handler.
            send_reply: A coroutine function responsible for sending the reply.
            handler: A coroutine function to handle the job / message.

                - It is a method on the kernel whose name corresponds to the [message type that it handles][async_kernel.typing.MsgType].
                - The handler should return a dict to use as 'content'in a reply.
                - If status is not included in the dict it gets added automatically as `{'status': 'ok'}`.
                - If a reply is not expected the handler should return `None`.

            job: The job dictionary containing message, socket, and identification information.

        Workflow:
            - Sets the current job and subshell_id context variables.
            - Sends a "busy" status message on the IOPub channel.
            - Awaits the handler; if the handler returns a content dict, a reply is sent using send_reply.
            - On exception, sends an error reply and logs the exception.
            - Resets the job and subshell_id context variables.
            - Sends an "idle" status message on the IOPub channel.

        Notes:
            - Replies are sent even if exceptions occur in the handler.
            - The reply message type is derived from the original request type.
        """
        job_token = utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        subshell_token = SubshellPendingManager._contextvar.set(subshell_id)  # pyright: ignore[reportPrivateUsage]

        try:
            self.iopub_send(
                msg_or_type="status",
                parent=job["msg"],
                content={"execution_state": "busy"},
                ident=self.topic(topic="status"),
            )
            if (content := await handler(job)) is not None:
                await send_reply(job, content)
        except Exception as e:
            await send_reply(job, utils.error_to_content(e))
            self.log.exception("Exception in message handler:", exc_info=e)
        finally:
            utils._job_var.reset(job_token)  # pyright: ignore[reportPrivateUsage]
            SubshellPendingManager._contextvar.reset(subshell_token)  # pyright: ignore[reportPrivateUsage]
            self.iopub_send(
                msg_or_type="status",
                parent=job["msg"],
                content={"execution_state": "idle"},
                ident=self.topic("status"),
            )
            del job

    def get_run_mode(
        self,
        msg_type: MsgType,
        *,
        socket_id: Literal[SocketID.shell, SocketID.control] = SocketID.shell,
        job: Job | None = None,
    ) -> RunMode:
        """
        Determine the run mode for a given channel, message type and job.

        Args:
            socket_id: The socket ID the message was received on.
            msg_type: The type of the message.
            job: The job associated with the message, if any.

        Returns:
            The run mode for the message.
        """
        # receive_msg_loop - DEBUG WARNING

        # TODO: Are any of these options worth including?
        # if mode_from_metadata := job["msg"]["metadata"].get("run_mode"):
        #     return RunMode( mode_from_metadata)
        # if mode_from_header := job["msg"]["header"].get("run_mode"):
        #     return RunMode( mode_from_header)
        match (socket_id, msg_type):
            case _, MsgType.comm_msg:
                return RunMode.queue
            case SocketID.control, MsgType.execute_request:
                return RunMode.queue
            case _, MsgType.execute_request:
                if job:
                    if content := job["msg"].get("content", {}):
                        if code := content.get("code"):
                            try:
                                if (code := code.strip().split("\n", maxsplit=1)[0]).startswith(("# ", "##")):
                                    return RunMode(code[2:])
                                if code.startswith("RunMode."):
                                    return RunMode(code.removeprefix("RunMode."))
                            except ValueError:
                                pass
                        if content.get("silent"):
                            return RunMode.task
                    if mode_ := set(utils.get_tags(job)).intersection(RunMode):
                        return RunMode(next(iter(mode_)))
                return RunMode.queue
            case (
                SocketID.shell,
                MsgType.shutdown_request
                | MsgType.debug_request
                | MsgType.create_subshell_request
                | MsgType.delete_subshell_request
                | MsgType.list_subshell_request,
            ):
                msg = f"{msg_type=} not allowed on shell!"
                raise ValueError(msg)
            case _, MsgType.debug_request:
                return RunMode.queue
            case (
                _,
                MsgType.complete_request
                | MsgType.inspect_request
                | MsgType.history_request
                | MsgType.create_subshell_request
                | MsgType.delete_subshell_request
                | MsgType.is_complete_request,
            ):
                return RunMode.thread
            case _:
                pass
        return RunMode.direct

    def all_concurrency_run_modes(
        self,
        socket_ids: Iterable[Literal[SocketID.shell, SocketID.control]] = (SocketID.shell, SocketID.control),
        msg_types: Iterable[MsgType] = MsgType,
    ) -> dict[
        Literal["SocketID", "MsgType", "RunMode"],
        tuple[SocketID, MsgType, RunMode | None],
    ]:
        """
        Generates a dictionary containing all combinations of SocketID, and MsgType, along with their
        corresponding RunMode (if available).
        """
        data: list[Any] = []
        for socket_id in socket_ids:
            for msg_type in msg_types:
                try:
                    mode = self.get_run_mode(msg_type, socket_id=socket_id)
                except ValueError:
                    mode = None
                data.append((socket_id, msg_type, mode))
        data_ = zip(*data, strict=True)
        return dict(zip(["SocketID", "MsgType", "RunMode"], data_, strict=True))

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
        return await self.shell.execute_request(**job["msg"]["content"])  # pyright: ignore[reportArgumentType]

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
        """Handle an [interrupt request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-interrupt) (control only)."""
        self.interface.interrupt()
        return {}

    async def shutdown_request(self, job: Job[Content], /) -> Content:
        """Handle a [shutdown request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-shutdown) (control only)."""
        self.stop()
        return {"restart": job["msg"]["content"].get("restart", False)}

    async def debug_request(self, job: Job[Content], /) -> Content:
        """Handle a [debug request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request) (control only)."""
        return await self.debugger.process_request(job["msg"]["content"])

    async def create_subshell_request(self: Kernel, job: Job[Content], /) -> Content:
        """Handle a [create subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#create-subshell) (control only)."""

        return {"subshell_id": self.subshell_manager.create_subshell(protected=False).subshell_id}

    async def delete_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle a [delete subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#delete-subshell) (control only)."""
        self.subshell_manager.delete_subshell(job["msg"]["content"]["subshell_id"])
        return {}

    async def list_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle a [list subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#list-subshells) (control only)."""
        return {"subshell_id": list(self.subshell_manager.list_subshells())}

    def excepthook(self, etype, evalue, tb) -> None:
        """Handle an exception."""
        # write uncaught traceback to 'real' stderr, not zmq-forwarder
        if self.print_kernel_messages:
            traceback.print_exception(etype, evalue, tb, file=sys.__stderr__)

    def unraisablehook(self, unraisable: sys.UnraisableHookArgs, /) -> None:
        "Handle unraisable exceptions (during gc for instance)."
        exc_info = (
            unraisable.exc_type,
            unraisable.exc_value or unraisable.exc_type(unraisable.err_msg),
            unraisable.exc_traceback,
        )
        self.log.exception(unraisable.err_msg, exc_info=exc_info, extra={"object": unraisable.object})

    def get_connection_info(self) -> dict[str, Any]:
        """Return the connection info as a dict."""
        with self.connection_file.open("r") as f:
            return json.load(f)

    def get_parent(self) -> Message[dict[str, Any]] | None:
        """
        A convenience method to access the 'message' in the current context if there is one.

        'parent' is the parameter name used by [Session.send][jupyter_client.session.Session.send] to provide context when sending a reply.

        See also:
            - [Kernel.iopub_send][Kernel.iopub_send]
            - [ipywidgets.Output][ipywidgets.widgets.widget_output.Output]:
                Uses `get_ipython().kernel.get_parent()` to obtain the `msg_id` which
                is used to 'capture' output when its context has been acquired.
        """
        return utils.get_parent()
