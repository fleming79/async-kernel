from __future__ import annotations

import json
import logging
import os
import pathlib
import sys
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from logging import Logger, LoggerAdapter
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self

import anyio
from aiologic import Event
from aiologic.lowlevel import current_async_library
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import traitlets
from typing_extensions import override

from async_kernel import Caller, utils
from async_kernel.asyncshell import (
    AsyncInteractiveShell,
    AsyncInteractiveSubshell,
    SubshellManager,
)
from async_kernel.comm import CommManager
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.interface.base import BaseKernelInterface
from async_kernel.typing import Channel, Content, ExecuteContent, Job, Message, NoValue

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from async_kernel.interface.zmq import ZMQKernelInterface


__all__ = ["Kernel", "KernelInterrupt"]


class Kernel(traitlets.HasTraits):
    """
    A Jupyter kernel providing an [IPython InteractiveShell][async_kernel.asyncshell.AsyncInteractiveShell].

    Starting the kernel:

        === "From the shell"

            ``` shell
            async-kernel --kernel-name=async -f .
            ```

        === "Blocking"

            ```python
            import async_kernel.interface

            settings = {}  # Dotted name key/value pairs

            async_kernel.interface.start_kernel_zmq_interface(settings)
            ```

        === "Async"

            ```python
            settings = {}  # Dotted name key/value pairs
            async with Kernel(settings).interface:
                ...
            ```

    Warning:
        Starting the kernel outside the main thread has the following implicatations:
            - Execute requests are run in the thread where the kernel is started.
            - The signal based kernel interrupt is not possible.

    Origins:
        - [IPyKernel Kernel][ipykernel.kernelbase.Kernel]
        - [IPyKernel IPKernelApp][ipykernel.kernelapp.IPKernelApp]
        - [IPyKernel IPythonKernel][ipykernel.ipkernel.IPythonKernel]
    """

    _instance: Self | None = None
    _initialised = False
    _restart = False

    _settings = Fixed(dict)

    interface: traitlets.Instance[BaseKernelInterface] = traitlets.Instance(BaseKernelInterface)
    "The abstraction to interface with the kernel."

    subshell_manager = SubshellManager
    "Dedicated to management of sub shells."

    quiet = traitlets.Bool(True)
    "Only send stdout/stderr to output stream."

    print_kernel_messages = traitlets.Bool(True)
    "When enabled the kernel will print startup, shutdown and terminal errors."

    connection_file: traitlets.TraitType[Path, Path | str] = traitlets.TraitType()
    """
    JSON file in which to store connection info.
    
    `"kernel-<pid>.json"`

    This file will contain the IP, ports, and authentication key needed to connect
    clients to this kernel. By default, this file will be created in the security dir
    of the current profile, but can be specified by absolute path.
    """

    kernel_name = traitlets.CUnicode()
    "The kernels name - if it contains 'trio' a trio backend will be used instead of an asyncio backend."

    log = traitlets.Instance(logging.LoggerAdapter)
    "The logging adapter."

    # Public fixed
    main_shell: Fixed[Self, AsyncInteractiveShell] = Fixed(AsyncInteractiveShell)
    "The interactive shell."

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
    def _default_kernel_name(self) -> Literal["async-trio", "async"]:
        return "async-trio" if current_async_library(failsafe=True) == "trio" else "async"

    @traitlets.default("interface")
    def default_interface(self) -> ZMQKernelInterface:
        from async_kernel.interface.zmq import ZMQKernelInterface  # noqa: PLC0415

        return ZMQKernelInterface()

    @traitlets.default("connection_file")
    def _default_connection_file(self) -> Path:
        return Path(jupyter_runtime_dir()).joinpath(f"kernel-{uuid.uuid4()}.json")

    @traitlets.observe("connection_file")
    def _observe_connection_file(self, change) -> None:
        if not self.interface.callers and (path := self.connection_file).exists():
            self.log.debug("Loading connection file %s", path)
            self.load_connection_info(json.loads(path.read_bytes()))

    @traitlets.validate("connection_file")
    def _validate_connection_file(self, proposal) -> Path:
        return pathlib.Path(proposal.value).expanduser()

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
        return self.interface.callers[Channel.shell]

    @property
    def kernel_info(self) -> dict[str, Any]:
        "A dict of detail sent in reply to for a 'kernel_info_request'."
        return self.main_shell.kernel_info

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

    @staticmethod
    def stop() -> None:
        """
        A [staticmethod][] to stop the running kernel.

        Once an instance of a kernel is stopped the instance cannot be restarted.
        Instead a new instance should be started.
        """
        if (kernel := Kernel._instance) and (scope := getattr(kernel, "_scope", None)):
            del kernel._scope
            kernel.log.info("Stopping kernel: %s", kernel)
            kernel.caller.call_direct(scope.cancel, "Stopping kernel")
            kernel.event_stopped.set()

    @asynccontextmanager
    async def _start(self) -> AsyncGenerator[Self]:
        # This context gets entered by the interface.
        assert self.main_shell
        original_sys_hooks = sys.excepthook, sys.unraisablehook
        try:
            with anyio.CancelScope() as scope:
                self._scope = scope
                sys.excepthook, sys.unraisablehook = self.excepthook, self.unraisablehook
                self.comm_manager.patch_comm()
                self.event_started.set()
                self.log.info("Kernel started: %s", self)
                yield self
        finally:
            self.stop()
            sys.excepthook, sys.unraisablehook = original_sys_hooks
            with anyio.CancelScope(shield=True):
                await self.do_shutdown(self._restart)

    async def do_shutdown(self, restart: bool) -> None:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_shutdown][]."
        assert self.event_stopped
        self.log.info("Kernel shutdown: %s", self)
        await anyio.sleep(0.1)
        self.shell.stop(force=True)
        for comm in tuple(self.comm_manager.comms.values()):
            comm.close(deleting=True)
        self.comm_manager.comms.clear()
        await anyio.sleep(0.1)
        CommManager._instance = None  # pyright: ignore[reportPrivateUsage]
        Kernel._instance = None
        self.log.info("Kernel shutdown complete: %s", self)

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
        if not self.event_stopped:
            self.interface.iopub_send(
                msg_or_type,
                content=content,
                metadata=metadata,
                parent=parent,
                ident=ident,
                buffers=buffers,
            )

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
        return await self.shell.execute_request(
            cell_id=job["msg"]["metadata"].get("cellId"),
            received_time=job["received_time"],
            **job["msg"]["content"],  # pyright: ignore[reportArgumentType]
        )

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
        self._restart = job["msg"]["content"].get("restart", False)
        self.stop()
        return {"restart": self._restart}

    async def debug_request(self, job: Job[Content], /) -> Content:
        """Handle a [debug request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request) (control only)."""
        return await self.shell.debugger.process_request(job["msg"]["content"])  # pyright: ignore[reportOptionalMemberAccess]

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
        return json.loads(self.connection_file.read_bytes())

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

    async def do_complete(self, code: str, cursor_pos: int | None) -> Content:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_history][]."
        return await self.shell.do_complete_request(code=code, cursor_pos=cursor_pos)

    async def do_inspect(self, code: str, cursor_pos: int | None, detail_level=0, omit_sections=()) -> Content:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_history][]."
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
        user_expressions: dict | None = None,
        allow_stdin=False,
        *,
        cell_id: str | None = None,
        **_ignored,
    ) -> Content:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_execute][]."
        content = ExecuteContent(
            code=code,
            silent=silent,
            store_history=store_history,
            user_expressions=user_expressions or {},
            allow_stdin=allow_stdin,
            stop_on_error=False,
        )
        msg = self.interface.msg("execute_request", content=content)  # pyright: ignore[reportArgumentType]
        job = Job(msg=msg, ident=[], received_time=time.monotonic())
        token = utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        try:
            return await self.shell.execute_request(
                code=code,
                silent=silent,
                store_history=store_history,
                user_expressions=user_expressions,
                allow_stdin=allow_stdin,
                cell_id=cell_id,
                received_time=job["received_time"],
            )
        finally:
            utils._job_var.reset(token)  # pyright: ignore[reportPrivateUsage]

    def getpass(self, prompt="", stream=None) -> str:
        "Matches signature of [ipykernel.kernelbase.Kernel.getpass][]."
        return self.interface.getpass(prompt)

    def raw_input(self, prompt="") -> str:
        "Matches signature of [ipykernel.kernelbase.Kernel.raw_input][]."
        return self.interface.raw_input(prompt)
