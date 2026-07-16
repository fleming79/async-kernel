from __future__ import annotations

import os
import re
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import anyio.abc
from aiologic import BinarySemaphore, Event
from aiologic.lowlevel import create_async_waiter
from traitlets import traitlets
from traitlets.config import LoggingConfigurable

import async_kernel.shell
from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.common import Fixed
from async_kernel.compat.json import pack_json_bytes, unpack_json
from async_kernel.interface import HasInterface
from async_kernel.pending import Pending, PendingTracker

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_kernel.kernel import Kernel
    from async_kernel.typing import DebugMessage

if "PYDEVD_IPYTHON_COMPATIBLE_DEBUGGING" not in os.environ:
    os.environ["PYDEVD_IPYTHON_COMPATIBLE_DEBUGGING"] = "1"

_host_port: None | tuple[str, int] = None


class _FakeCode:
    """Fake code class.  Origin: [IPyKernel][ipykernel.debugger._FakeCode]."""

    def __init__(self, co_filename, co_name) -> None:
        """Init."""
        self.co_filename = co_filename
        self.co_name = co_name


class _FakeFrame:
    """Fake frame class. Origin: [IPyKernel][ipykernel.debugger._FakeFrame]."""

    def __init__(self, f_code, f_globals, f_locals) -> None:
        """Init."""
        self.f_code = f_code
        self.f_globals = f_globals
        self.f_locals = f_locals
        self.f_back = None


class _DummyPyDB:
    """Fake PyDb class. Origin: [IPyKernel][ipykernel.debugger._DummyPyDB]."""

    def __init__(self) -> None:
        """Init."""
        from _pydevd_bundle.pydevd_api import PyDevdAPI  # type: ignore[attr-defined]  # noqa: PLC0415

        self.variable_presentation = PyDevdAPI.VariablePresentation()


class VariableExplorer(HasInterface, traitlets.HasTraits):
    """
    A variable explorer.

    Origin: [IPyKernel][ipykernel.debugger.VariableExplorer]
    """

    def __init__(self) -> None:
        """Initialize the explorer."""
        super().__init__()
        # This import is apparently required to provide _pydevd_bundle imports
        import debugpy.server.api  # noqa: F401, I001, PLC0415  # pyright: ignore[reportUnusedImport]
        from _pydevd_bundle.pydevd_suspended_frames import SuspendedFramesManager, _FramesTracker  # type: ignore[attr-defined]  # noqa: PLC0415

        self.suspended_frame_manager = SuspendedFramesManager()
        self.py_db = _DummyPyDB()
        self.tracker = _FramesTracker(self.suspended_frame_manager, self.py_db)
        self.frame = None

    def track(self) -> None:
        """Start tracking."""
        from _pydevd_bundle import pydevd_frame_utils  # type: ignore[attr-defined]  # noqa: PLC0415

        shell = self.parent.kernel.main_shell
        assert isinstance(shell, async_kernel.shell.IPShell)
        var = shell.user_ns
        self.frame = _FakeFrame(_FakeCode("<module>", shell.compile.get_file_name("sys._getframe()")), var, var)
        self.tracker.track("thread1", pydevd_frame_utils.create_frames_list_from_frame(self.frame))

    def untrack_all(self) -> None:
        """Stop tracking."""
        self.tracker.untrack_all()

    def get_children_variables(self, variable_ref=None) -> list[Any]:
        """Get the child variables for a variable reference."""
        var_ref = variable_ref
        if not var_ref:
            var_ref = id(self.frame)
        try:
            variables = self.suspended_frame_manager.get_variable(var_ref)
        except KeyError:
            return []
        return [x.get_var_data() for x in variables.get_children_variables()]


class DebugpyClient(HasInterface, LoggingConfigurable):
    """A client for debugpy. Origin: [IPyKernel][ipykernel.debugger.DebugpyClient]."""

    HEADER = b"Content-Length: "
    SEPARATOR = b"\r\n\r\n"
    SEPARATOR_LENGTH = 4
    tcp_buffer = b""
    _result_responses: traitlets.Dict[int, Pending] = traitlets.Dict()
    capabilities = traitlets.Dict()
    kernel: Fixed[Self, Kernel] = Fixed(lambda c: c["owner"].parent.kernel)
    _socketstream: anyio.abc.SocketStream | None = None
    _send_lock = traitlets.Instance(BinarySemaphore, ())

    @property
    def connected(self) -> bool:
        return bool(self._socketstream)

    async def send_request(self, request: dict) -> Pending[dict[str, Any]]:
        if not (socketstream := self._socketstream):
            raise RuntimeError
        async with self._send_lock:
            self._result_responses[request["seq"]] = pen = Pending(None, PendingTracker)
            content = pack_json_bytes(request)
            content_length = str(len(content)).encode()
            buf = self.HEADER + content_length + self.SEPARATOR
            buf += content
            self.log.debug("DEBUGPYCLIENT: request %s", buf)
            await socketstream.send(buf)
            return pen

    def put_tcp_frame(self, frame: bytes) -> None:
        """Buffer the frame and process the buffer."""
        self.tcp_buffer += frame
        data = self.tcp_buffer.split(self.HEADER)
        if len(data) > 1:
            for buf in data[1:]:
                size, raw_msg = buf.split(self.SEPARATOR, maxsplit=1)
                size = int(size)
                msg: DebugMessage = unpack_json(raw_msg[:size])
                self.log.debug("_put_message :%s %s", msg["type"], msg)
                if msg["type"] == "event":
                    self.kernel.debugger.handle_event(msg)
                elif result := self._result_responses.pop(msg["request_seq"], None):
                    result.set_result(msg)
            self.tcp_buffer = b""

    async def _connect_tcp_socket(self, ready: Callable[[], Any]) -> None:
        """Connect to the tcp socket."""
        global _host_port  # noqa: PLW0603
        if not _host_port:
            import debugpy  # noqa: PLC0415

            _host_port = debugpy.listen(0)
        try:
            self.log.debug("debugpy socketstream connecting")
            async with await anyio.connect_tcp(*_host_port) as socketstream:
                self._socketstream = socketstream
                self.log.debug("debugpy socketstream connected")
                ready()
                while True:
                    data = await socketstream.receive()
                    self.put_tcp_frame(data)
        except anyio.EndOfStream:
            self.log.debug("debugpy socketstream disconnected")
            return
        except anyio.get_cancelled_exc_class():
            msg = {
                "type": "request",
                "seq": self.kernel.debugger.next_seq(),
                "command": "configurationDone",
            }
            with anyio.CancelScope(shield=True):
                await self.kernel.debugger.do_disconnect(msg)
            raise
        finally:
            self._socketstream = None


class Debugger(HasInterface, LoggingConfigurable):
    """The debugger class. Origin: [IPyKernel][ipykernel.debugger.DebugpyClient]."""

    no_debug = traitlets.List(["IPythonHistorySavingThread"]).tag(config=True)
    """A list of thread names to excluded when using debugy (on startup)."""

    breakpoint_list = traitlets.Dict()
    capabilities = traitlets.Dict()
    stopped_threads = traitlets.Set()
    just_my_code = traitlets.Bool(True)
    init_event = traitlets.Instance(Event, ())
    variable_explorer = traitlets.Instance(VariableExplorer, ())
    debugpy_client = Fixed(DebugpyClient)
    kernel: Fixed[Self, Kernel] = Fixed(lambda c: c["owner"].parent.kernel)
    _seq = 0

    @property
    def enabled(self) -> bool:
        return bool(
            not utils.LAUNCHED_BY_DEBUGPY
            and sys.platform != "emscripten"
            and isinstance((self.parent.kernel.main_shell), async_kernel.shell.IPShell)
        )

    def __init__(self, **kwargs) -> None:
        """Initialize the debugger."""
        super().__init__(**kwargs)
        self.started_debug_handlers = {
            "setBreakpoints": self.do_set_breakpoints,
            "stackTrace": self.do_stack_trace,
            "variables": self.do_variables,
            "attach": self.do_attach,
            "configurationDone": self.do_configuration_done,
            "copyToGlobals": self.do_copy_to_globals,
            "disconnect": self.do_disconnect,
        }
        self.static_debug_handlers = {
            "initialize": self.do_initialize,
            "dumpCell": self.do_dump_cell,
            "source": self.do_source,
            "debugInfo": self.do_debug_info,
            "inspectVariables": self.do_inspect_variables,
            "richInspectVariables": self.do_rich_inspect_variables,
            "modules": self.do_modules,
        }
        self._forbidden_names = tuple(self.parent.kernel.main_shell.user_ns_hidden.copy())

    async def send_dap_request(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Sends a DAP request to the debug server, waits for and returns the corresponding response."""
        return await (await self.debugpy_client.send_request(msg))

    def next_seq(self) -> int:
        "A monotonically decreasing negative number so as not to clash with the frontend seq."
        self._seq = self._seq - 1
        return self._seq

    def handle_event(self, event) -> None:
        if event["event"] == "stopped":

            async def _handle_stopped_event() -> None:
                names = {t.name for t in threading.enumerate() if not getattr(t, "pydev_do_not_trace", False)}
                msg = {"seq": self.next_seq(), "type": "request", "command": "threads"}
                rep = await self.send_dap_request(msg)
                for thread in rep["body"]["threads"]:
                    if thread["name"] in names:
                        self.stopped_threads.add(thread["id"])
                self._publish_event(event)

            Caller().call_soon(_handle_stopped_event)
            return

        if event["event"] == "continued":
            self.stopped_threads.clear()
        elif event["event"] == "initialized":
            self.init_event.set()
        self._publish_event(event)

    def _publish_event(self, event: dict) -> None:
        self.kernel.parent.iopub_send(
            msg_or_type="debug_event",
            content=event,
            ident=b"kernel.debug_event",
            parent=None,
        )

    def _build_variables_response(self, request, variables) -> dict[str, Any]:
        var_list = [var for var in variables if self._accept_variable(var["name"])]
        return {
            "seq": request["seq"],
            "type": "response",
            "request_seq": request["seq"],
            "success": True,
            "command": request["command"],
            "body": {"variables": var_list},
        }

    def _accept_variable(self, variable_name) -> bool:
        """Accept a variable by name."""
        return (
            variable_name not in self._forbidden_names
            and not bool(re.search(r"^_\d", variable_name))
            and not variable_name.startswith("_i")
        )

    async def process_request(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Process a request."""
        command = msg["command"]
        if handler := self.static_debug_handlers.get(command):
            return await handler(msg)
        if not self.debugpy_client.connected:
            msg_ = "Debugpy client not connected."
            raise RuntimeError(msg_)
        if handler := self.started_debug_handlers.get(command):
            return await handler(msg)

        return await self.send_dap_request(msg)

    ## Static handlers

    async def do_initialize(self, msg: DebugMessage, /) -> dict[str, Any]:
        "Initialize debugpy server starting as required."

        utils.mark_thread_pydev_do_not_trace()
        for thread in threading.enumerate():
            if thread.name in self.no_debug:
                utils.mark_thread_pydev_do_not_trace(thread)
        if not self.debugpy_client.connected:
            ready = create_async_waiter()
            Caller().call_soon(self._debupy_socket_connection, ready.wake)
            await ready

        reply = await self.send_dap_request(msg)
        if capabilities := reply.get("body"):
            self.capabilities = capabilities
        return reply

    async def _debupy_socket_connection(self, ready: Callable[[], Any]) -> None:
        "Maintain a connection to the debugger"
        msg = {
            "type": "request",
            "seq": self.next_seq(),
            "command": "configurationDone",
        }
        async with anyio.create_task_group() as tg:
            tg.start_soon(self.debugpy_client._connect_tcp_socket, ready)  # pyright: ignore[reportPrivateUsage]
            await self.parent.stopping
            await self.do_disconnect(msg)
            tg.cancel_scope.cancel()

    async def do_debug_info(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an debug info message."""
        breakpoint_list = []
        for key, value in self.breakpoint_list.items():
            breakpoint_list.append({"source": key, "breakpoints": value})
        assert isinstance((self.parent.kernel.main_shell), async_kernel.shell.IPShell)
        compiler = self.parent.kernel.main_shell.compile
        return {
            "type": "response",
            "request_seq": msg["seq"],
            "success": True,
            "command": msg["command"],
            "body": {
                "isStarted": self.debugpy_client.connected,
                "hashMethod": compiler.hash_method,
                "hashSeed": compiler.hash_seed,
                "tmpFilePrefix": compiler.tmp_file_prefix,
                "tmpFileSuffix": compiler.tmp_file_suffix,
                "breakpoints": breakpoint_list,
                "stoppedThreads": sorted(self.stopped_threads),
                "richRendering": True,
                "exceptionPaths": ["Python Exceptions"],
                "copyToGlobals": True,
            },
        }

    async def do_inspect_variables(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an inspect variables message."""
        self.variable_explorer.untrack_all()
        # looks like the implementation of untrack_all in ptvsd
        # destroys objects we need in track. We have no choice but
        # reinstantiate the object
        self.variable_explorer = VariableExplorer()
        self.variable_explorer.track()
        variables = self.variable_explorer.get_children_variables()
        return self._build_variables_response(msg, variables)

    async def do_rich_inspect_variables(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an rich inspect variables message."""
        reply = {
            "type": "response",
            "sequence_seq": msg["seq"],
            "success": False,
            "command": msg["command"],
        }
        variable_name = msg["arguments"].get("variableName", "")
        if not str.isidentifier(variable_name):
            reply["body"] = {"data": {}, "metadata": {}}
            if variable_name in {"special variables", "function variables"}:
                reply["success"] = True
            return reply
        repr_data = {}
        repr_metadata = {}
        if not self.stopped_threads:
            # The code did not hit a breakpoint, we use the interpreter
            # to get the rich representation of the variable
            if isinstance((shell := self.parent.kernel.main_shell), async_kernel.shell.IPShell):
                result = shell.user_expressions({"var": variable_name})["var"]
                if result.get("status", "error") == "ok":
                    repr_data = result.get("data", {})
                    repr_metadata = result.get("metadata", {})
        else:
            # The code has stopped on a breakpoint, we use the evaluate
            # request to get the rich representation of the variable
            code = f"get_ipython().display_formatter.format({variable_name})"
            reply = await self.send_dap_request(
                {
                    "type": "request",
                    "command": "evaluate",
                    "seq": self.next_seq(),
                    "arguments": {"expression": code, "context": "clipboard"} | msg["arguments"],
                }
            )
            if reply["success"]:
                repr_data, repr_metadata = eval(reply["body"]["result"], {}, {})
        body = {
            "data": repr_data,
            "metadata": {k: v for k, v in repr_metadata.items() if k in repr_data},
        }
        reply["body"] = body
        reply["success"] = True
        return reply

    async def do_modules(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an modules message."""
        modules = list(sys.modules.values())
        startModule = msg.get("startModule", 0)
        moduleCount = msg.get("moduleCount", len(modules))
        mods = []
        for i in range(startModule, moduleCount):
            module = modules[i]
            filename = getattr(getattr(module, "__spec__", None), "origin", None)
            if filename and filename.endswith(".py"):
                mods.append({"id": i, "name": module.__name__, "path": filename})
        return {"body": {"modules": mods, "totalModules": len(modules)}}

    async def do_dump_cell(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an dump cell message."""
        code = msg["arguments"]["code"]
        assert isinstance((self.parent.kernel.main_shell), async_kernel.shell.IPShell)
        path = self.parent.kernel.main_shell.compile.get_file_name(code)
        path.parent.mkdir(exist_ok=True)
        with path.open("w") as f:
            f.write(code)
        return {
            "type": "response",
            "request_seq": msg["seq"],
            "success": True,
            "command": msg["command"],
            "body": {"sourcePath": str(path)},
        }

    async def do_copy_to_globals(self, msg: DebugMessage, /) -> dict[str, Any]:
        dst_var_name = msg["arguments"]["dstVariableName"]
        src_var_name = msg["arguments"]["srcVariableName"]
        src_frame_id = msg["arguments"]["srcFrameId"]
        # Copy the variable to the user_ns
        await self.send_dap_request(
            {
                "type": "request",
                "command": "evaluate",
                "seq": self.next_seq(),
                "arguments": {
                    "expression": f"get_ipython().user_ns['{dst_var_name}'] = {src_var_name}",
                    "frameId": src_frame_id,
                    "context": "repl",
                },
            }
        )
        return await self.send_dap_request(
            {
                "type": "request",
                "command": "evaluate",
                "seq": msg["seq"],
                "arguments": {
                    "expression": f"globals()['{dst_var_name}'] = {src_var_name}",
                    "frameId": src_frame_id,
                    "context": "repl",
                },
            }
        )

    async def do_set_breakpoints(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an set breakpoints message."""
        source = msg["arguments"]["source"]["path"]
        self.breakpoint_list[source] = msg["arguments"]["breakpoints"]
        message_response = await self.send_dap_request(msg)
        # debugpy can set breakpoints on different lines than the ones requested,
        # so we want to record the breakpoints that were actually added
        if message_response.get("success"):
            self.breakpoint_list[source] = [
                {"line": breakpoint["line"]} for breakpoint in message_response["body"]["breakpoints"]
            ]
        return message_response

    async def do_source(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an source message."""
        reply = {"type": "response", "request_seq": msg["seq"], "command": msg["command"]}
        if (path := Path(msg["arguments"].get("source", {}).get("path", "missing"))).is_file():
            with path.open("r", encoding="utf-8") as f:
                reply["success"] = True
                reply["body"] = {"content": f.read()}
        else:
            reply["success"] = False
            reply["message"] = "source unavailable"
            reply["body"] = {}

        return reply

    async def do_stack_trace(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an stack trace message."""
        reply = await self.send_dap_request(msg)
        # The stackFrames array can have the following content:
        # { frames from the notebook}
        # ...
        # { 'id': xxx, 'name': '<module>', ... } <= this is the first frame of the code from the notebook
        # { frames from async_kernel }
        # ...
        # {'id': yyy, 'name': '<module>', ... } <= this is the first frame of async_kernel code
        # or only the frames from the notebook.
        # We want to remove all the frames from async_kernel when they are present.
        try:
            sf_list = reply["body"]["stackFrames"]
            module_idx = len(sf_list) - next(
                i for i, v in enumerate(reversed(sf_list), 1) if v["name"] == "<module>" and i != 1
            )
            reply["body"]["stackFrames"] = reply["body"]["stackFrames"][: module_idx + 1]
        except StopIteration:
            pass
        return reply

    async def do_variables(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an variables message."""
        reply = {}
        if not self.stopped_threads:
            variables = self.variable_explorer.get_children_variables(msg["arguments"]["variablesReference"])
            return self._build_variables_response(msg, variables)
        reply = await self.send_dap_request(msg)
        if "body" in reply:
            variables = [var for var in reply["body"]["variables"] if self._accept_variable(var["name"])]
            reply["body"]["variables"] = variables
        return reply

    async def do_attach(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an attach message."""
        assert _host_port
        msg["arguments"]["connect"] = {"host": _host_port[0], "port": _host_port[1]}
        if self.just_my_code:
            msg["arguments"]["debugOptions"] = ["justMyCode"]
        reply = await self.debugpy_client.send_request(msg)
        await self.init_event
        await self.send_dap_request(
            {
                "type": "request",
                "seq": self.next_seq(),
                "command": "configurationDone",
            }
        )
        return await reply

    async def do_configuration_done(self, msg: DebugMessage, /) -> dict[str, Any]:
        """Handle an configuration done message."""
        # This is only supposed to be called during initialize but can come at anytime. Ref: https://microsoft.github.io/debug-adapter-protocol/specification#Events_Initialized
        # see : https://github.com/jupyterlab/jupyterlab/issues/17673
        return {
            "seq": msg["seq"],
            "type": "response",
            "request_seq": msg["seq"],
            "success": True,
            "command": msg["command"],
        }

    async def do_disconnect(self, msg: DebugMessage, /) -> dict[str, Any]:
        response = await self.send_dap_request(msg)
        # Restore the leading whitespace remove transform.
        assert isinstance((self.parent.kernel.main_shell), async_kernel.shell.IPShell)
        self.init_event = Event()
        self.breakpoint_list = {}
        return response
