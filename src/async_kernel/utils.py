from __future__ import annotations

import sys
import threading
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

import traitlets

import async_kernel
from async_kernel.typing import Message, MetadataKeys

if TYPE_CHECKING:
    from collections.abc import Mapping

    from async_kernel.kernel import Kernel
    from async_kernel.typing import Job

__all__ = [
    "get_execute_request_timeout",
    "get_execution_count",
    "get_job",
    "get_metadata",
    "get_parent",
    "get_tags",
    "mark_thread_pydev_do_not_trace",
    "setattr_nested",
]

LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules

_job_var = ContextVar("job")
_execution_count_var: ContextVar[int] = ContextVar("execution_count")
_execute_request_timeout: ContextVar[float | None] = ContextVar("execute_request_timeout", default=None)


def mark_thread_pydev_do_not_trace(thread: threading.Thread | None = None, *, remove=False):
    """Modifies the given thread's attributes to hide or unhide it from the debugger (e.g., debugpy)."""
    thread = thread or threading.current_thread()
    thread.pydev_do_not_trace = not remove  # pyright: ignore[reportAttributeAccessIssue]


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


def get_metadata(job: Job | None = None, /) -> Mapping[str, Any]:
    "Gets [metadata]() for the current context."
    return (job or get_job()).get("msg", {}).get("metadata", {})


def get_tags(job: Job | None = None, /) -> list[str]:
    "Gets the [tags]() for the current context."
    return get_metadata(job).get("tags", [])


def get_execute_request_timeout(job: Job | None = None, /) -> float | None:
    "Gets the execute_request_timeout for the current context."
    try:
        if timeout := get_metadata(job).get(MetadataKeys.timeout):
            return float(timeout)
        return get_kernel().shell.execute_request_timeout
    except Exception:
        return None


def get_execution_count() -> int:
    "Gets the execution count for the current context, defaults to the current kernel count."

    return _execution_count_var.get(None) or async_kernel.Kernel()._execution_count  # pyright: ignore[reportPrivateUsage]


def setattr_nested(obj: object, name: str, value: str | Any) -> dict[str, Any]:
    """
    Set a nested attribute of an object.

    If the attribute name contains dots, it is interpreted as a nested attribute.
    For example, if name is "a.b.c", then the code will attempt to set obj.a.b.c to value.

    This is primarily intended for use with [async_kernel.command.command_line][]
    to set the nesteded attributes on on kernels.

    Args:
        obj: The object to set the attribute on.
        name: The name of the attribute to set.
        value: The value to set the attribute to.

    Returns:
        The mapping of the name to the set value if the value has been set.
        An empty dict indicates the value was not set.

    """
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
