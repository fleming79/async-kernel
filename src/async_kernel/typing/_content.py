"""Provides typing definitions for message content."""

from __future__ import annotations

from typing import Any, Literal, NotRequired

from typing_extensions import ReadOnly, TypedDict, TypeVar


class Content(TypedDict):
    """Content in a message."""


class AnyContent(TypedDict, extra_items=ReadOnly[Any]):
    """Generic content when the type has not been defined."""


class RequestContent(Content):
    """"""


class ReplyContent_(Content):
    """"""


class ReplyContent(ReplyContent_):
    status: Literal["ok"]


class ErrorReply(ReplyContent_):
    """Content of a reply to a request that is an error."""

    status: Literal["error"]
    ename: str
    evalue: str
    traceback: list[str]


class AnyReplyContent(ReplyContent, extra_items=Any):
    """"""


T_content_co = TypeVar("T_content_co", bound=Content, covariant=True, default=AnyContent)
"""A typevar for message content."""


T_reply_content_co = TypeVar("T_reply_content_co", bound=ReplyContent_, covariant=True, default=AnyReplyContent)
"""A typevar for message content."""


class HelpLinkType(TypedDict):
    """Help links to display in the notebook."""

    text: str
    """"""
    url: str
    """"""


class LanguageInfo(TypedDict):
    """Used in `KernelInfoReply`."""

    name: str
    """Name of the programming language."""

    version: str
    """Language version number eg: 'X.Y.Z'."""

    mimetype: str
    """_mimetype_ for script files in this language."""

    file_extension: str
    """Extension including the dot, e.g. '.py'."""

    pygments_lexer: NotRequired[str]
    """Pygments lexer, for highlighting (Only needed if it differs from the 'name' field)."""

    codemirror_mode: NotRequired[str | dict[str, Any]]
    """Codemirror mode, for highlighting in the notebook, (Only needed if it differs from the 'name' field)."""

    nbconvert_exporter: NotRequired[str]
    """Nbconvert exporter, if notebooks written with this kernel should be exported with something other than the general 'script' exporter."""


# KernelInfo
class KernelInfoRequest(RequestContent):
    """"""


class KernelInfoReply(ReplyContent):
    protocol_version: str
    "Version of messaging protocol."

    implementation: str
    """The version number of the kernel's implementation."""

    implementation_version: str
    """The version of the kernel (module) eg: 'X.Y.Z'."""

    language_info: LanguageInfo
    """Information about the language of code for the kernel."""

    banner: str
    """A banner of information about the kernel which may be displayed in console environments."""

    help_links: list[HelpLinkType]
    """A mapping of help links to display in the notebook."""

    supported_features: list[str]
    """A list of optional features such as 'debugger."""


# CommInfo
class CommInfoItem(TypedDict):
    """"""

    target_name: str
    """"""


class CommInfoRequest(RequestContent):
    """"""

    target_name: NotRequired[str]
    """"""


class CommInfoReply(ReplyContent):
    """A reply to a CommInfo request."""

    comms: dict[str, CommInfoItem]
    """A dictionary of the comms, indexed by the comm id (UUID)."""


# Execute
class MIMEBundle(TypedDict, extra_items=Any):
    data: dict[str, Any]
    metadata: dict[str, Any]


class ExecuteRequest(RequestContent):
    """[Ref](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)."""

    code: str
    """The code to execute."""
    silent: bool
    """"""
    store_history: bool
    """"""
    allow_stdin: bool
    """"""
    stop_on_error: bool
    """"""
    subshell_id: NotRequired[str | None]
    """"""
    user_expressions: NotRequired[dict[str, str]]
    """"""


class ExecuteReply(ReplyContent):
    """"""

    execution_count: int
    """"""

    user_expressions: dict[str, MIMEBundle]
    """"""


class ExecuteErrorReply(ErrorReply):
    """"""

    execution_count: int
    """"""
    user_expressions: dict[str, MIMEBundle]
    """"""


# Complete
class CompleteRequest(RequestContent):
    """"""

    code: str
    """"""

    cursor_pos: int | None
    """"""


class CompleteReply(ReplyContent):
    """"""

    matches: list
    cursor_end: int
    cursor_start: int
    metadata: NotRequired[dict]


# Interrupt
class InterruptRequest(RequestContent):
    """"""


class InterruptReply(ReplyContent):
    """"""


# Shutdown
class ShutdownRequest(RequestContent):
    """"""

    restart: bool
    """"""


class ShutdownReply(ReplyContent):
    """A reply to a shutdown request."""

    restart: bool
    """If restart is expected."""


# IsComplete
class IsCompleteRequest(RequestContent):
    """"""

    code: str
    """"""


class IsCompleteReply(ReplyContent_):
    """A reply to an `is_complete_request`. https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness."""

    status: Literal["complete", "incomplete", "invalid", "unknown"]
    indent: NotRequired[str]


# Inspect
class InspectRequest(RequestContent):
    """"""

    code: str
    """"""
    cursor_pos: int | None
    """"""
    detail_level: int
    """"""


class InspectReply(ReplyContent):
    """"""

    data: dict[str, Any]
    """"""
    metadata: dict[str, Any]
    """"""
    found: bool
    """"""


# History
class HistoryRequest(RequestContent):
    """"""

    output: bool
    """ If True, also return output history in the resulting dict."""

    raw: bool
    """If True, return the raw input history, else the transformed input."""

    hist_access_type: str
    # So far, this can be 'range', 'tail' or 'search'.
    # If hist_access_type is 'range', get a range of input cells. session
    # is a number counting up each time the kernel starts; you can give
    # a positive session number, or a negative number to count back from
    # the current session.

    session: NotRequired[int]
    start: NotRequired[int]
    stop: NotRequired[int]
    # start and stop are line (cell) numbers within that session.

    # If hist_access_type is 'tail' or 'search', get the last n cells.
    n: NotRequired[int]

    # If hist_access_type is 'search', get cells matching the specified glob
    # pattern (with * and ? as wildcards).
    pattern: NotRequired[str]

    # If hist_access_type is 'search' and unique is true, do not
    # include duplicated history.  Default is false.
    unique: NotRequired[bool]


class HistoryReply(ReplyContent):
    """"""

    history: list[tuple]


# Input
class InputRequest(RequestContent):
    """"""

    prompt: str
    """"""
    password: bool
    """"""


class InputReply(ReplyContent):
    """A reply to an input request."""

    value: str
    ""


# Debug
class DebugRequest(RequestContent, extra_items=Any):
    """A TypeAlias for a debug message."""

    seq: int
    """"""

    type: Literal["request"]
    """"""

    command: Literal["inspectVariables", "threads", "evaluate", "configurationDone"]
    """"""

    arguments: dict[str, Any]
    """"""


class DebugReply(ReplyContent_, extra_items=Any):
    """"""


# CreateSubshell
class CreateSubshellRequest(RequestContent):
    """"""


class CreateSubshellReply(ReplyContent):
    """"""

    subshell_id: str
    """"""


# DeleteSubshell
class DeleteSubshellRequest(RequestContent):
    """"""


class DeleteSubshellReply(ReplyContent):
    """"""


# ListSubshell
class ListSubshellRequest(RequestContent):
    """"""


class ListSubshellReply(ReplyContent):
    """"""

    subshell_id: list[str]
    """"""
