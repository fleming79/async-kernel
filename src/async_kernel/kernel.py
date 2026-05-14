from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING, Any, Self, cast

import anyio
from aiologic import Event
from traitlets import traitlets
from traitlets.config.configurable import LoggingConfigurable
from wrapt import lazy_import

import async_kernel
from async_kernel import Caller, utils
from async_kernel.asyncshell import AsyncInteractiveShell, AsyncInteractiveSubshell, SubshellManager
from async_kernel.comm import CommManager
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.debugger import Debugger
from async_kernel.typing import Channel, Content, ExecuteContent, Job, Message

globals()["BaseKernelInterface"] = lazy_import("async_kernel.interface.base", "BaseKernelInterface")

if TYPE_CHECKING:
    from async_kernel.interface.base import BaseKernelInterface  # noqa: TC004


__all__ = ["Kernel", "KernelInterrupt"]


class Kernel(LoggingConfigurable):
    """
    A Jupyter kernel.

    Warning:
        Starting the kernel outside the main thread has the following implicatations:
            - Execute requests are run in the thread where the kernel is started.
            - The signal based kernel interrupt is not possible.
    """

    _cls: type[Self] | None = None
    _instance: Self | None = None
    _initialised = False
    _restart = False

    interface: traitlets.Instance[BaseKernelInterface] = traitlets.Instance(
        "async_kernel.interface.base.BaseKernelInterface", ()
    )
    "The abstraction to interface with the kernel."

    subshell_manager = SubshellManager
    "Dedicated to management of sub shells."

    debugger = traitlets.Instance(Debugger)
    "The debugger for handling debug requests."

    help_links = traitlets.List(trait=traitlets.Dict()).tag(config=True)
    "A list of links provided kernel info request."

    banner = traitlets.Unicode().tag(config=True)
    "The banner to display in a console."

    supported_features = traitlets.List(traitlets.Unicode())
    "A list of features supported by the kernel."

    # Public fixed
    main_shell: Fixed[Self, AsyncInteractiveShell] = Fixed(AsyncInteractiveShell)
    "The interactive shell."

    comm_manager = Fixed(CommManager)
    "Creates [async_kernel.comm.Comm][] instances and maintains a mapping to `comm_id` to `Comm` instances."

    event_started = Fixed(Event)
    "An event that occurs when the kernel is started."

    event_stopped = Fixed(Event)
    "An event that occurs when the kernel is stopped."

    def __init_subclass__(cls) -> None:
        if cls._instance:
            msg = "It is too late to subclass the kernel!"
            raise RuntimeError(msg)
        Kernel._cls = cls
        super().__init_subclass__()

    def __new__(cls) -> Self:
        #  There is only one instance allowed - ever - (including subclasses).
        if not (kernel := Kernel._instance):
            if not (interface := BaseKernelInterface._instance):  # pyright: ignore[reportPrivateUsage]
                msg = "An interface must be created prior to creating the kernel."
                raise RuntimeError(msg)
            # Identify the preferred kernel
            cls_ = cls
            if interface.trait_has_value("kernel_class"):
                # A subclass specified on the interface
                cls_ = cast("Any", interface.kernel_class)
            elif cls is Kernel:
                # The last defined subclass
                cls_ = cast("Any", Kernel._cls or cls)
            Kernel._instance = kernel = super().__new__(cls_)
        if not isinstance(kernel, cls):
            msg = f"A different kernel was loaded {cls=} {kernel=}"
            raise TypeError(msg)
        return kernel  # pyright: ignore[reportReturnType]

    def __init__(self) -> None:
        if not self._initialised:
            self._initialised = True
            super().__init__(config=self.interface.config)
            assert self.main_shell

    @traitlets.default("debugger")
    def _default_debugger(self) -> Debugger:
        return Debugger(parent=self)

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
        if self.debugger:
            features.append("debugger")
        return features

    @traitlets.default("banner")
    def _default_banner(self) -> str:
        return self.main_shell.banner

    @property
    def kernel_info(self) -> dict[str, Any]:
        "Info provided to a kernel info request."

        return {
            "protocol_version": async_kernel.kernel_protocol_version,
            "implementation": async_kernel.distribution_name,
            "implementation_version": async_kernel.__version__,
            "language_info": async_kernel.kernel_protocol_version_info,
            "banner": self.banner,
            "help_links": self.help_links,
            "debugger": bool((not utils.LAUNCHED_BY_DEBUGPY) and sys.platform != "emscripten"),
            "supported_features": self.supported_features,
        }

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

    async def do_shutdown(self, restart: bool) -> None:
        "Matches signature of [ipykernel.kernelbase.Kernel.do_shutdown][]."
        assert self.event_stopped
        self.log.info("Kernel shutdown started")
        await anyio.sleep(0.1)
        self.main_shell.stop(force=True)
        for comm in tuple(self.comm_manager.comms.values()):
            comm.close(deleting=True)
        self.comm_manager.comms.clear()
        await anyio.sleep(0.1)
        CommManager._instance = None  # pyright: ignore[reportPrivateUsage]
        Kernel._instance = None
        self.log.info("Kernel shutdown complete")

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
        """Handle an [interrupt request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-interrupt)."""
        self.interface.interrupt()
        return {}

    async def shutdown_request(self, job: Job[Content], /) -> Content:
        """Handle a [shutdown request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-shutdown)."""
        self._restart = job["msg"]["content"].get("restart", False)
        self.interface.stop()
        return {"restart": self._restart}

    async def debug_request(self, job: Job[Content], /) -> Content:
        """Handle a [debug request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request)."""
        return await self.debugger.process_request(job["msg"]["content"])

    async def create_subshell_request(self: Kernel, job: Job[Content], /) -> Content:
        """Handle a [create subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#create-subshell)."""

        return {"subshell_id": self.subshell_manager.create_subshell(protected=False).subshell_id}

    async def delete_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle a [delete subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#delete-subshell)."""
        self.subshell_manager.delete_subshell(job["msg"]["content"]["subshell_id"])
        return {}

    async def list_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle a [list subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#list-subshells)."""
        return {"subshell_id": list(self.subshell_manager.list_subshells())}

    def get_parent(self) -> Message[dict[str, Any]] | None:
        """
        A convenience method to access the 'message' in the current context if there is one.

        'parent' is the parameter name used by [Session.send][jupyter_client.session.Session.send] to provide context when sending a reply.

        See also:
            - [ipywidgets.Output][ipywidgets.widgets.widget_output.Output]:
                Uses `get_ipython().kernel.get_parent()` to obtain the `msg_id` which
                is used to 'capture' output when its context has been acquired.
        """
        return utils.get_parent()

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
