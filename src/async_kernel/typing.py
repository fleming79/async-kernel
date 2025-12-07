from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Generic, Literal, NotRequired, ParamSpec, TypedDict, TypeVar

from typing_extensions import Sentinel, override

if TYPE_CHECKING:
    import logging

    import zmq


__all__ = [
    "Backend",
    "CallerCreateOptions",
    "CallerState",
    "Content",
    "DebugMessage",
    "ExecuteContent",
    "FixedCreate",
    "FixedCreated",
    "HandlerType",
    "Job",
    "KernelName",
    "Message",
    "MsgHeader",
    "MsgType",
    "NoValue",
    "PendingCreateOptions",
    "RunMode",
    "SocketID",
    "Tags",
]

NoValue = Sentinel("NoValue")
"A sentinel to indicate a value has not been provided."


S = TypeVar("S")
T = TypeVar("T")
D = TypeVar("D", bound=dict)
P = ParamSpec("P")


class Backend(enum.StrEnum):
    asyncio = "asyncio"
    trio = "trio"


class KernelName(enum.StrEnum):
    asyncio = "async"
    trio = "async-trio"


class SocketID(enum.StrEnum):
    "Mapping of `Kernel.port_<id>` for sockets. [Ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introduction)."

    heartbeat = "hb"
    ""
    shell = "shell"
    ""
    stdin = "stdin"
    ""
    control = "control"
    ""
    iopub = "iopub"
    ""


class RunMode(enum.StrEnum):
    """
    An Enum of the run modes available for handling [Messages][async_kernel.typing.Message].

    [receive_msg_loop][async_kernel.kernel.Kernel.receive_msg_loop] uses [get_run_mode][async_kernel.kernel.Kernel.get_run_mode]
    to map the message type and channel (`shell` or `control`) to the `RunMode`.

    Cell overrides:
        The user can also specify an execution mode in execute requests.

        Top line comment:
            ```python
            # task
            ```
            or

            ```python
            ##task
            ```
    """

    @override
    def __str__(self):
        return f"# {self.name}"

    @override
    def __eq__(self, value: object, /) -> bool:
        return str(value) in {self.name, str(self), repr(self)}

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    queue = "queue"
    "Run the message handler using [async_kernel.caller.Caller.queue_call][]."

    task = "task"
    "Run the message handler using [async_kernel.caller.Caller.call_soon][]."

    thread = "thread"
    "Run the message handler using [async_kernel.caller.Caller.to_thread][] to start use a 'worker'."

    direct = "direct"
    """
    Run the message handler using [async_kernel.caller.Caller.call_direct][].
    
    Warning: 
        - This mode runs directly in the caller scheduler as soon as it is received.
        - Use this only for fast running high priority code.
    """


class MsgType(enum.StrEnum):
    """
    An enumeration of Message `msg_type` for [shell and control messages]( https://jupyter-client.readthedocs.io/en/stable/messaging.html#messages-on-the-shell-router-dealer-channel).

    Some message types are on the [control channel](https://jupyter-client.readthedocs.io/en/stable/messaging.html#messages-on-the-control-router-dealer-channel) only.
    """

    kernel_info_request = "kernel_info_request"
    "[async_kernel.kernel.Kernel.kernel_info_request][]"

    comm_info_request = "comm_info_request"
    "[async_kernel.kernel.Kernel.comm_info_request][]"

    execute_request = "execute_request"
    "[async_kernel.kernel.Kernel.execute_request][]"

    complete_request = "complete_request"
    "[async_kernel.kernel.Kernel.complete_request][]"

    is_complete_request = "is_complete_request"
    "[async_kernel.kernel.Kernel.is_complete_request][]"

    inspect_request = "inspect_request"
    "[async_kernel.kernel.Kernel.inspect_request][]"

    history_request = "history_request"
    "[async_kernel.kernel.Kernel.history_request][]"

    comm_open = "comm_open"
    "[async_kernel.kernel.Kernel.comm_open][]"

    comm_msg = "comm_msg"
    "[async_kernel.kernel.Kernel.comm_msg][]"

    comm_close = "comm_close"
    "[async_kernel.kernel.Kernel.comm_close][]"

    # Control
    interrupt_request = "interrupt_request"
    "[async_kernel.kernel.Kernel.interrupt_request][] (control channel only)"

    shutdown_request = "shutdown_request"
    "[async_kernel.kernel.Kernel.shutdown_request][] (control channel only)"

    debug_request = "debug_request"
    "[async_kernel.kernel.Kernel.debug_request][] (control channel only)"

    create_subshell_request = "create_subshell_request"
    "[async_kernel.kernel.Kernel.create_subshell_request][] (control channel only)"

    delete_subshell_request = "delete_subshell_request"
    "[async_kernel.kernel.Kernel.delete_subshell_request][] (control channel only)"

    list_subshell_request = "list_subshell_request"
    "[async_kernel.kernel.Kernel.debug_request][] (control channel only)"


class Tags(enum.StrEnum):
    """
    Tags recognised by the [shell][async_kernel.asyncshell.AsyncInteractiveShell].

    Info:
        Tags are can be added per cell.

        - Jupyter: via the [right side bar](https://jupyterlab.readthedocs.io/en/stable/user/interface.html#left-and-right-sidebar).
        - VScode: via [Jupyter variables explorer](https://code.visualstudio.com/docs/python/jupyter-support-py#_variables-explorer-and-data-viewer)
    """

    @override
    def __eq__(self, value: object, /) -> bool:
        return str(value).replace("-", "_").split("=")[0] == self.name

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    def get_bool(self, value: str | Tags, default: bool = True) -> bool:
        try:
            return value.split("=")[1].lower() == "true"
        except Exception:
            return default

    def get_float(self, value: str | Tags, default: float = 0.0) -> float:
        try:
            return float(value.split("=")[1])
        except Exception:
            return default

    def get_string(self, value: str | Tags, default: str = "") -> str:
        try:
            return value.split("=")[1]
        except Exception:
            return default

    raises_exception = "raises-exception"
    """
    Indicates the cell should expect an exception to be raised. 
    
    Notes:
        - When an exception is raised, stop_on_error is False/
        - When an exception is **not** raised an exception will be raise and stop_on_error is True.
    """

    suppress_error = "suppress-error"
    """
    Suppress exceptions that occur during execution of the code cell.

    The default message is '⚠'

    Examples:

        - suppress-error 
        - suppress-error=The suppression message.

    Warning:
        The code block will return as 'ok' (not published).
    """

    stop_on_error = "stop-on-error"
    """
    Override `stop_on_error`.

    Examples:

        - True
            - stop_on_error=true
            - stop_on_error=True
        - False
            - stop_on_error=False
    """

    timeout = "timeout="
    """
    Specify a timeout in seconds for code execution to complete.

    Examples:

        - timeout=0.0 (no timeout)
        - timeout=0.1 (100 ms)
    """


class PendingTrackerState(enum.Enum):
    "The state of a [async_kernel.pending.PendingManager][]."

    idle = enum.auto()
    active = enum.auto()
    exiting = enum.auto()
    stopped = enum.auto()


class CallerState(enum.Enum):
    "The State of a [async_kernel.caller.Caller][]."

    initial = enum.auto()
    start_sync = enum.auto()
    running = enum.auto()
    stopping = enum.auto()
    stopped = enum.auto()


class MsgHeader(TypedDict):
    "A [message header](https://jupyter-client.readthedocs.io/en/stable/messaging.html#message-header)."

    msg_id: str
    ""
    session: str
    ""
    username: str
    ""
    date: str
    ""
    msg_type: MsgType
    ""
    version: str
    ""


class Message(TypedDict, Generic[T]):
    "A [message](https://jupyter-client.readthedocs.io/en/stable/messaging.html#general-message-format)."

    header: MsgHeader
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#message-header)"

    parent_header: MsgHeader
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#parent-header)"

    metadata: dict[str, Any]
    "[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#metadata)"

    content: T | Content
    """[ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#metadata)
    
    See also:
        - [ExecuteContent][]
    """
    buffers: list[bytearray | bytes]
    ""


class Job(TypedDict, Generic[T]):
    "A `Message` bundle."

    msg: Message[T]
    "The message received over the socket."
    socket_id: Literal[SocketID.control, SocketID.shell]
    "The channel over which the socket was received."
    ident: bytes | list[bytes]
    "The ident associated with the message and its origin."
    received_time: float
    "The time the message was received."


class ExecuteContent(TypedDict):
    "[Ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)."

    code: str
    "The code to execute."
    silent: bool
    ""
    store_history: bool
    ""
    user_expressions: dict[str, str]
    ""
    allow_stdin: bool
    ""
    stop_on_error: bool
    ""


class FixedCreate(TypedDict, Generic[S]):
    "A TypedDict relevant to Fixed."

    name: str
    owner: S


class FixedCreated(TypedDict, Generic[S, T]):
    "A TypedDict relevant to Fixed."

    name: str
    owner: S
    obj: T


class PendingCreateOptions(TypedDict):
    "Options to pass when creating a new [Pending][async_kernel.pending.Pending]."

    allow_tracking: NotRequired[bool]
    "Add the pending to all [pending trackers][async_kernel.pending.PendingTracker] (default=`True`)."


class CallerCreateOptions(TypedDict):
    "Options to use when creating an instance of a [Caller][async_kernel.caller.Caller]."

    name: NotRequired[str]
    "The name for the new caller instance."
    log: NotRequired[logging.LoggerAdapter]
    "A logging adapter to use for the caller."
    backend: NotRequired[Backend | Literal["trio", "asyncio"]]
    "The backend to specify when calling [anyio.run][]."
    backend_options: NotRequired[dict | None]
    "Options to pass when calling [anyio.run][]."
    protected: NotRequired[bool]
    "The caller should be protected against accidental closure (default=`False`)."
    zmq_context: NotRequired[zmq.Context[Any]]
    "A zmq Context to use."


DebugMessage = dict[str, Any]
"""
A TypeAlias for a debug message.
"""

Content = dict[str, Any]
"""
A TypeAlias for the content in `Message`.
"""

HandlerType = Callable[[Job], Awaitable[Content | None]]
"""
A TypeAlias for the handler of message requests.
"""
