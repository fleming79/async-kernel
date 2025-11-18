from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Generic, Literal, NotRequired, ParamSpec, TypedDict, TypeVar

from typing_extensions import Sentinel, override

if TYPE_CHECKING:
    import logging
    import threading
    from collections.abc import Mapping

    import zmq

    from async_kernel.kernelspec import Backend

__all__ = [
    "CallerCreateOptions",
    "Content",
    "DebugMessage",
    "ExecuteContent",
    "FixedCreate",
    "FixedCreated",
    "HandlerType",
    "Job",
    "Message",
    "MetadataKeys",
    "MsgHeader",
    "MsgType",
    "NoValue",
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

    [receive_msg_loop][async_kernel.Kernel.receive_msg_loop] uses [get_run_mode][async_kernel.Kernel.get_run_mode]
    to map the message type and channel (`shell` or `control`) to the `RunMode`.

    Cell overrides:
        The user can also specify an execution mode in execute requests.

        Top line comment:
            ```python
            ##task
            ```
        Tag:
            see: [async_kernel.typing.MetadataKeys][].
    """

    @override
    def __str__(self):
        return f"##{self.name}"

    @override
    def __eq__(self, value: object, /) -> bool:
        return str(value) in (self.name, str(self), repr(self))

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    @classmethod
    def get_mode(cls, code: str) -> RunMode | None:
        "Get a RunMode from the code if it is found."
        try:
            if (code := code.strip().split("\n")[0].strip()).startswith("##"):
                return RunMode(code.removeprefix("##"))
            if code.startswith("RunMode."):
                return RunMode(code.removeprefix("RunMode."))
        except ValueError:
            return None

    queue = "queue"
    "Run the message handler using [async_kernel.Caller.queue_call][]."

    task = "task"
    "Run the message handler using [async_kernel.Caller.call_soon][]."

    thread = "thread"
    "Run the message handler using [async_kernel.Caller.to_thread][] to start use a 'worker'."

    direct = "direct"
    """
    Run the message handler using [async_kernel.Caller.call_direct][].
    
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
    "[async_kernel.Kernel.kernel_info_request][]"

    comm_info_request = "comm_info_request"
    "[async_kernel.Kernel.comm_info_request][]"

    execute_request = "execute_request"
    "[async_kernel.Kernel.execute_request][]"

    complete_request = "complete_request"
    "[async_kernel.Kernel.complete_request][]"

    is_complete_request = "is_complete_request"
    "[async_kernel.Kernel.is_complete_request][]"

    inspect_request = "inspect_request"
    "[async_kernel.Kernel.inspect_request][]"

    history_request = "history_request"
    "[async_kernel.Kernel.history_request][]"

    comm_open = "comm_open"
    "[async_kernel.Kernel.comm_open][]"

    comm_msg = "comm_msg"
    "[async_kernel.Kernel.comm_msg][]"

    comm_close = "comm_close"
    "[async_kernel.Kernel.comm_close][]"

    # Control
    interrupt_request = "interrupt_request"
    "[async_kernel.Kernel.interrupt_request][] (control channel only)"

    shutdown_request = "shutdown_request"
    "[async_kernel.Kernel.shutdown_request][] (control channel only)"

    debug_request = "debug_request"
    "[async_kernel.Kernel.debug_request][] (control channel only)"


class MetadataKeys(enum.StrEnum):
    """
    This is an enum of keys for [metadata in kernel messages](https://jupyter-client.readthedocs.io/en/stable/messaging.html#metadata)
    that are used in async_kernel.

    Notes:
        Metadata can be edited in Jupyter lab "Advanced tools" and Tags can be added using "common tools" in the [right side bar](https://jupyterlab.readthedocs.io/en/stable/user/interface.html#left-and-right-sidebar).
    """

    @override
    def __eq__(self, value: object, /) -> bool:
        return str(value) in (self.name, str(self))

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    tags = "tags"
    """
    The `tags` metadata key corresponds to is a list of strings. 
    
    The list can be edited by the user in a notebook.
    see also: [Tags][async_kernel.typing.Tags].
    """
    timeout = "timeout"
    """
    The `timeout` metadata key is used to specify a timeout for execution of the code.
    
    The value should be a floating point value of the timeout in seconds.
    """
    suppress_error_message = "suppress-error-message"
    """
    A message to print when the error has been suppressed using [async_kernel.typing.Tags.suppress_error][]. 
    
    Notes:
        - The default message is '⚠'.
    """


class Tags(enum.StrEnum):
    """
    Tags recognised by the kernel.

    Info:
        Tags are can be added per cell.

        - Jupyter: via the [right side bar](https://jupyterlab.readthedocs.io/en/stable/user/interface.html#left-and-right-sidebar).
        - VScode: via [Jupyter variables explorer](https://code.visualstudio.com/docs/python/jupyter-support-py#_variables-explorer-and-data-viewer)
    """

    @override
    def __eq__(self, value: object, /) -> bool:
        return str(value) in (self.name, str(self))

    @override
    def __hash__(self) -> int:
        return hash(self.name)

    suppress_error = "suppress-error"
    """
    Suppress exceptions that occur during execution of the code cell.
    
    Warning:
        The code block will return as 'ok' and there will be no message recorded.
    """


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

    metadata: Mapping[MetadataKeys | str, Any]
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
    socket: zmq.Socket
    "The actual socket."
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


class CallerCreateOptions(TypedDict):
    "Options for creating a new [Caller][async_kernel.caller.Caller]."

    name: NotRequired[str | None]
    """The name to use for the caller."""
    thread: NotRequired[threading.Thread | None]
    "The thread of the caller. (current thread)"
    log: NotRequired[logging.LoggerAdapter]
    "A logging adapter to use to log exceptions."
    backend: NotRequired[Backend | Literal["trio", "asyncio"]]
    "The anyio backend to use (1. Inherited. 2. current_async_library 3. From  [async_kernel.kernel.Kernel.anyio_backend][])."
    backend_options: NotRequired[dict | None]
    "Options to use when calling [anyio.run][] inside the new thread (1. Inherited. 2. From [async_kernel.kernel.Kernel.anyio_backend_options][])."
    protected: NotRequired[bool]
    "The caller should be protected against accidental closure (False)."
    zmq_context: NotRequired[zmq.Context[Any]]
    "A zmq Context to use "


CallerGetModeType = Literal["auto", "existing", "MainThread"]
"""The mode to use in [async_kernel.caller.Caller.get][].

- "auto": (Default) A new instance is created if no existing instance is found.
- "existing": Only checks for existing instances. 
- "MainThread": Shorthand for kwargs = `{"thread":threading.main_thread()}`
"""


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
