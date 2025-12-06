from __future__ import annotations

import math
import sys
import threading
import traceback
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from typing_extensions import TypeVar

import async_kernel
from async_kernel.asyncshell import SubshellPendingManager
from async_kernel.typing import Tags

if TYPE_CHECKING:
    from collections.abc import Generator

    from async_kernel.kernel import Kernel
    from async_kernel.typing import Content, Job, Message

__all__ = [
    "error_to_content",
    "error_to_content",
    "get_execution_count",
    "get_job",
    "get_kernel",
    "get_metadata",
    "get_parent",
    "get_subshell_id",
    "get_tag_value",
    "get_tags",
    "get_timeout",
    "mark_thread_pydev_do_not_trace",
    "setattr_nested",
    "subshell_context",
]

LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules

_job_var: ContextVar[Job] = ContextVar("job")
_execute_request_timeout: ContextVar[float | None] = ContextVar("timeout", default=None)


def mark_thread_pydev_do_not_trace(thread: threading.Thread | None = None, *, remove=False) -> None:
    """Modifies the given thread's attributes to hide or unhide it from the debugger (e.g., debugpy)."""
    thread = thread or threading.current_thread()
    thread.pydev_do_not_trace = not remove  # pyright: ignore[reportAttributeAccessIssue]
    thread.is_pydev_daemon_thread = not remove  # pyright: ignore[reportAttributeAccessIssue]
    return


def get_kernel() -> Kernel:
    "Get the current kernel."
    return async_kernel.Kernel()


def get_job() -> Job[dict] | dict:
    "Get the job for the current context."
    try:
        return _job_var.get()
    except Exception:
        return {}


def get_parent(job: Job | None = None, /) -> Message[dict[str, Any]] | None:
    "Get the [parent message]() for the current context."
    return (job or get_job()).get("msg")


def get_subshell_id() -> str | None:
    "Get the `subshell_id` for the current context."
    return SubshellPendingManager._contextvar.get()  # pyright: ignore[reportPrivateUsage]


@contextmanager
def subshell_context(subshell_id: str | None) -> Generator[None, Any, None]:
    """A context manager to work in the context of a shell or subshell.

    Args:
        subshell_id: An existing subshell or the main shell if subshell_id is None.
    """
    shell = get_kernel().subshell_manager.get_shell(subshell_id)  # use the shell for validation.
    token = SubshellPendingManager._contextvar.set(shell.subshell_id)  # pyright: ignore[reportPrivateUsage]
    try:
        yield
    finally:
        SubshellPendingManager._contextvar.reset(token)  # pyright: ignore[reportPrivateUsage]


def get_metadata(job: Job | None = None, /) -> dict[str, Any]:
    "Gets the metadata for the current context."
    try:
        return (job or _job_var.get())["msg"]["metadata"]
    except Exception:
        return {}


def get_tags(job: Job | None = None, /) -> list[str]:
    "Gets the tags for the current context."
    try:
        return get_metadata(job)["tags"]
    except Exception:
        return []


_TagType = TypeVar("_TagType", str, float, int, bool)


def get_tag_value(tag: Tags, default: _TagType, /, *, tags: list[str] | None = None) -> _TagType:
    """
    Get the value for the tag from a collection of tags.

    Args:
        tag: The tag to get the value from.
        default: The default value if a tag is not found. The default is also used to determine the type for conversion of the value.
        tags: A list of tags to search. When not provide [get_tags][] is used.

    The tag value is the value trailing behind <tag>=<value>. The value is transformed according to
    the type of the default.
    """
    for t in tags if tags is not None else get_tags():
        if t == tag:
            if isinstance(default, float):
                return tag.get_float(t, default)
            if isinstance(default, bool):
                return tag.get_bool(t, default)
            if isinstance(default, str):
                return tag.get_string(t, default)
            return int(tag.get_float(t, default))
    return default


def get_timeout(*, tags: list[str] | None = None) -> float:
    "Gets the timeout from tags or using the current context."
    if math.isnan(timeout := get_tag_value(Tags.timeout, math.nan, tags=tags)):
        return get_kernel().shell.timeout
    return max(timeout, 0.0)


def get_execution_count() -> int:
    "Gets the execution count for the current context, defaults to the current kernel count."

    return get_kernel().shell.execution_count


def setattr_nested(obj: object, name: str, value: str | Any) -> dict[str, Any]:
    """
    Replace an existing nested attribute/trait of an object.

    If the attribute name contains dots, it is interpreted as a nested attribute.
    For example, if name is "a.b.c", then the code will attempt to set obj.a.b.c to value.

    Args:
        obj: The object to set the attribute on.
        name: The name of the attribute to set.
        value: The value to set the attribute to.

    Returns:
        The mapping of the name to the set value if the value has been set.
        An empty dict indicates the value was not set.
    """
    import traitlets  # noqa: PLC0415

    if len(bits := name.split(".")) > 1:
        try:
            obj = getattr(obj, bits[0])
        except Exception:
            return {}
        setattr_nested(obj, ".".join(bits[1:]), value)
    if (isinstance(obj, traitlets.HasTraits) and obj.has_trait(name)) or hasattr(obj, name):
        try:
            setattr(obj, name, value)
        except Exception:
            setattr(obj, name, eval(value))
        return {name: getattr(obj, name)}
    return {}


def error_to_content(error: BaseException, /) -> Content:
    """
    Convert the error to a dict.

    ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#request-reply
    """
    return {
        "status": "error",
        "ename": type(error).__name__,
        "evalue": str(error),
        "traceback": traceback.format_exception(error),
    }
