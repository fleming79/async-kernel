from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING, Any, Self

import async_kernel
from async_kernel import utils
from async_kernel.comm import CommManager
from async_kernel.common import Fixed, KernelInterrupt
from async_kernel.debugger import Debugger
from async_kernel.interface import HasInterface
from async_kernel.typing import Channel, Content, ExecuteContent, Job, Message

if TYPE_CHECKING:
    from async_kernel.asyncshell import AsyncInteractiveShell
    from async_kernel.caller import Caller

__all__ = ["Kernel", "KernelInterrupt"]


class Kernel(HasInterface):
    """
    A Jupyter kernel.

    Warning:
        Starting the kernel outside the main thread has the following implicatations:
            - Execute requests are run in the thread where the kernel is started.
            - The signal based kernel interrupt is not possible.
    """

    __slots__ = []

    callers: Fixed[Self, dict] = Fixed(lambda c: c["owner"].parent.callers)
    "A shortcut to the callers dict on the parent."

    caller: Fixed[Self, Caller] = Fixed(lambda c: c["owner"].callers[Channel.shell])
    "The caller for the shell thread."

    main_shell: Fixed[Self, AsyncInteractiveShell] = Fixed(lambda c: c["owner"].parent.shell)
    "The main shell"

    debugger = Fixed(Debugger)
    "The debugger for handling debug requests."

    comm_manager = Fixed(CommManager)
    "Creates [async_kernel.comm.Comm][] instances and maintains a mapping to `comm_id` to `Comm` instances."

    @property
    def kernel_info(self) -> dict[str, Any]:
        "Info provided to a kernel info request."

        features = ["kernel subshells"]
        if self.debugger:
            features.append("debugger")
        return {
            "protocol_version": async_kernel.kernel_protocol_version,
            "implementation": async_kernel.distribution_name,
            "implementation_version": async_kernel.__version__,
            "language_info": async_kernel.kernel_protocol_version_info,
            "banner": self.shell.banner,
            "help_links": self.parent.help_links,
            "debugger": bool((not utils.LAUNCHED_BY_DEBUGPY) and sys.platform != "emscripten"),
            "supported_features": self.parent.supported_features,
        }

    @property
    def shell(self) -> AsyncInteractiveShell:
        """
        The shell given the current context.

        Notes:
            - The `subshell_id` of the main shell is `None`.
        """
        return self.parent.get_shell()

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
        self.parent.interrupt()
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
            shell = await self.parent.create_subshell(protected=False)
            return {"subshell_id": shell.subshell_id}

    async def delete_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle a [delete subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#delete-subshell)."""
        if (subshell_id := job["msg"]["content"]["subshell_id"]) and (
            subshell := self.parent.subshells.get(subshell_id)
        ):
            subshell.stop()
        return {}

    async def list_subshell_request(self, job: Job[Content], /) -> Content:
        """Handle a [list subshell request](https://jupyter.org/enhancement-proposals/91-kernel-subshells/kernel-subshells.html#list-subshells)."""
        return {"subshell_id": list(self.parent.subshells)}

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
        msg = self.parent.msg("execute_request", content=content)  # pyright: ignore[reportArgumentType]
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
        return self.parent.getpass(prompt)

    def raw_input(self, prompt="") -> str:
        "Matches signature of [ipykernel.kernelbase.Kernel.raw_input][]."
        return self.parent.raw_input(prompt)
