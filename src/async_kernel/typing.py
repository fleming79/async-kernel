from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Generic, Literal, NotRequired, ParamSpec, TypedDict, TypeVar

from typing_extensions import Sentinel, override

if TYPE_CHECKING:
    import logging
    from collections.abc import Mapping

    import zmq

    from async_kernel.kernelspec import Backend

__all__ = [
    "DebugMessage",
    "Job",
    "Message",
    "MetadataKeys",
    "MsgHeader",
    "MsgType",
    "RunMode",
    "SocketID",
    "Tags",
]

NoValue = Sentinel("NoValue")


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
    An Enum of the run modes available for altering how message requests are run.

    When [receive_msg_loop][async_kernel.Kernel.receive_msg_loop] receives a message it uses
    [get_run_mode[async_kernel.Kernel.get_run_mode] to get the run mode.

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
        **This mode runs directly in the caller scheduler blocking other tasks from being scheduled.** 
        
        Use this for short running messages that should be processed as soon as it is received.
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
        - VScode: via [Jupyter variables explorer](https://code.visualstudio.com/docs/python/jupyter-support-py#_variables-explorer-and-data-viewer)/
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

    - [ExecuteContent][async_kernel.typing.ExecuteContent]
    """
    buffers: list[bytearray | bytes]
    ""


class Job(TypedDict, Generic[T]):
    "An [async_kernel.typing.Message][] bundled with sockit_id, socket and ident."

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
    "[Ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute).  see also: [Message][async_kernel.typing.Message]"

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


class CallerStartNewOptions(TypedDict):
    "Options for [async_kernel.caller.Caller.start_new][]."

    name: NotRequired[str | None]
    """
    The name to assign to a new thread when creating the new caller.
    """
    log: NotRequired[logging.LoggerAdapter]
    "A logging adapter to use to log exceptions."
    backend: NotRequired[Backend]
    "The anyio backend to use."
    backend_options: NotRequired[dict | None]
    "Options to use when calling [anyio.run][] inside the new thread."
    protected: NotRequired[bool]
    "The caller should be protected against accidental closure."
    daemon: NotRequired[bool]
    "Passed to the new [Thread][threading.Thread]."


DebugMessage = dict[str, Any]
"""
A TypeAlias for a debug message.

Used by:
    - [async_kernel.debugger.Debugger.process_request][]. 
"""

Content = dict[str, Any]
"""
A TypeAlias for the content in [async_kernel.typing.Message][].
"""

HandlerType = Callable[[Job], Awaitable[Content | None]]
"""
A TypeAlias for the handler of message requests.

Used by:
    - [async_kernel.Kernel.run_handler][]
"""
