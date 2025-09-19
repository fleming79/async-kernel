from __future__ import annotations

import contextlib
import sys
import threading
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

import anyio
import anyio.to_thread
import traitlets

import async_kernel
from async_kernel.typing import Message, MetadataKeys

if TYPE_CHECKING:
    from collections.abc import Mapping

    from async_kernel.kernel import Kernel
    from async_kernel.typing import Job

__all__ = [
    "do_not_debug_this_thread",
    "get_execute_request_timeout",
    "get_execution_count",
    "get_job",
    "get_metadata",
    "get_parent",
    "get_tags",
    "mark_thread_pydev_do_not_trace",
    "setattr_nested",
    "wait_thread_event",
]

LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules

_job_var = ContextVar("job")
_execution_count_var: ContextVar[int] = ContextVar("execution_count")
_execute_request_timeout: ContextVar[float | None] = ContextVar("execute_request_timeout", default=None)


def mark_thread_pydev_do_not_trace(thread: threading.Thread, *, remove=False):
    """Modifies the given thread's attributes to hide or unhide it from the debugger (e.g., debugpy)."""
    thread.pydev_do_not_trace = not remove  # pyright: ignore[reportAttributeAccessIssue]


@contextlib.contextmanager
def do_not_debug_this_thread():
    "A context to mark the thread for debugpy to not debug."
    if not LAUNCHED_BY_DEBUGPY:
        mark_thread_pydev_do_not_trace(threading.current_thread())
    try:
        yield
    finally:
        if not LAUNCHED_BY_DEBUGPY:
            mark_thread_pydev_do_not_trace(threading.current_thread(), remove=True)


async def wait_thread_event(event: threading.Event):
    """
    Wait for the threading event using an anyio worker thread.

    !!! info

        - If the event is already set anyio.sleep(0) is used instead.
        - On external cancellation the `event` is set here to prevent the thread from waiting forever.
    """
    if event.is_set():
        # Required  - allow other tasks to run.
        await anyio.sleep(0)
        return

    def _in_thread_call():
        with do_not_debug_this_thread():
            event.wait()

    try:
        await anyio.to_thread.run_sync(_in_thread_call)
    finally:
        event.set()


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
