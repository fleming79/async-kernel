from __future__ import annotations

import sys
import threading
import traceback
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from traitlets import traitlets
from typing_extensions import TypeVar

import async_kernel.interface

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Mapping
    from contextlib import _SupportsRedirect, _SupportsRedirectT  # pyright: ignore[reportPrivateUsage]

    from async_kernel.kernel import Kernel
    from async_kernel.shell import BaseShell
    from async_kernel.typing import Content, Job, Message, Tags

__all__ = [
    "apply_settings",
    "error_to_content",
    "error_to_content",
    "get_job",
    "get_kernel",
    "get_metadata",
    "get_parent_message",
    "get_subshell_id",
    "get_tag_value",
    "get_tags",
    "mark_thread_pydev_do_not_trace",
    "redirect_stderr",
    "redirect_stdout",
    "setattr_nested",
    "show_result",
    "subshell_context",
]
LAUNCHED_BY_PYTEST = "pytest" in sys.modules
LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules
LAUNCHED_BY_DEBUGPY_PYTEST = LAUNCHED_BY_DEBUGPY and LAUNCHED_BY_PYTEST
"Useful to allow exceptiosn to be raised with the debugger launched with pytest and debugger."


_job_var: ContextVar[Job[Any]] = ContextVar("async-kernel job")
_cell_id_var: ContextVar[str | None] = ContextVar("async-kernel cell_id", default=None)
_stdout_context: ContextVar[_SupportsRedirect | None] = ContextVar("async-kernel std_out", default=None)
_stderr_context: ContextVar[_SupportsRedirect | None] = ContextVar("async-kernel stderr", default=None)
_show_result_context: ContextVar[bool] = ContextVar("async-kernel show_result", default=False)


def mark_thread_pydev_do_not_trace(thread: threading.Thread | None = None, *, remove=False) -> None:
    """Modifies the given thread's attributes to hide or unhide it from the debugger (e.g., debugpy)."""
    thread = thread or threading.current_thread()
    thread.pydev_do_not_trace = not remove  # pyright: ignore[reportAttributeAccessIssue]
    thread.is_pydev_daemon_thread = not remove  # pyright: ignore[reportAttributeAccessIssue]


def get_kernel() -> Kernel:
    """Get the current kernel."""
    if not async_kernel.interface.BaseInterface.initialized():
        msg = "A kernel interface is not started!"
        raise RuntimeError(msg)
    return async_kernel.interface.BaseInterface.instance().kernel


def get_ipython() -> BaseShell:
    """Get the shell in the current context."""
    return get_kernel().get_shell()


def get_job() -> Job[Any]:
    """Get the job for the current context.

    Raises:
        LookupError: If there is no job in the current context.
    """
    return _job_var.get()


def get_parent_message(job: Job | None = None, /) -> Message[dict[str, Any]] | None:
    """Get the parent message for the current context."""
    try:
        return (job or get_job()).get("msg")
    except LookupError:
        return None


def get_subshell_id() -> str | None:
    """Get the `subshell_id` for the current context."""
    return get_kernel().shell.subshell_id


@contextmanager
def subshell_context(subshell_id: str | None) -> Generator[None, Any, None]:
    """A context manager to work in the context of a shell or subshell.

    Args:
        subshell_id: An existing subshell or the main shell if subshell_id is None.
    """
    with get_kernel().get_shell(subshell_id).context():
        yield


def get_metadata(job: Job | None = None, /) -> dict[str, Any] | None:
    """Gets the metadata for the current context."""
    try:
        return (job or _job_var.get())["msg"]["metadata"]
    except Exception:
        return None


def get_tags(job: Job | None = None, /) -> list[str]:
    """Gets the tags for the current context."""
    try:
        return get_metadata(job)["tags"]  # pyright: ignore[reportOptionalSubscript]
    except Exception:
        return []


def get_cell_id(job: Job | None = None, /) -> str | None:
    """The `cell_id` for the current context."""
    return _cell_id_var.get() or (metadata.get("cellId") if (metadata := get_metadata(job)) else None)


_TagType = TypeVar("_TagType", str, float, int, bool)


def get_tag_value(tag: Tags, default: _TagType, /, *, tags: Iterable[str] | None = None) -> _TagType:
    """Get the value for the tag from a collection of tags.

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


def setattr_nested(obj: object, name: str, value: str | Any, *, _return_value=False) -> dict[str, Any]:
    """Replace an existing nested attribute/trait of an object.

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
    try:
        if len(bits := name.split(".")) > 1:
            try:
                obj = getattr(obj, bits[0])
            except Exception:
                return {}
            value = setattr_nested(obj, ".".join(bits[1:]), value, _return_value=True)
            return value if _return_value else {name: value}
        if (isinstance(obj, traitlets.HasTraits) and obj.has_trait(name)) or hasattr(obj, name):
            try:
                setattr(obj, name, value)
            except Exception:
                setattr(obj, name, eval(value))
            return value if _return_value else {name: value}  # pyright: ignore[reportReturnType]
    except Exception:
        if not _return_value:
            raise
    return {}


def apply_settings(obj: object, settings: Mapping[str, Any]) -> dict[str, Any]:
    """Apply the settings onto the object.

    Returns:
        dict: A copy of the settings that were applied.

    Notes:
        - If flags are included using the pattern `'flags': iterable(str)`, the flags
            are interpreted as boolean values. Generally, it is preferred to specify
            the value in the settings explicitly.
    """
    values = {}
    for k, v in settings.items():
        if k == "flags":
            for flag in (f.strip("-") for f in v):
                values.update(setattr_nested(obj, flag.strip("no-"), not flag.startswith("no-")))
        else:
            values.update(setattr_nested(obj, k, v))
    return values


def error_to_content(error: BaseException, /) -> Content:
    """Convert the error to a dict.

    ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#request-reply
    """
    return {
        "status": "error",
        "ename": type(error).__name__,
        "evalue": str(error),
        "traceback": traceback.format_exception(error),
    }


@contextmanager
def redirect_stdout(stream: _SupportsRedirectT, /) -> Generator[_SupportsRedirectT, Any, None]:
    """Re-direct [sys.stdout][] generated in the current context.

    See Also:
        - [contextlib.redirect_stdout][]
    """
    token = _stdout_context.set(stream)
    try:
        yield stream
    finally:
        _stdout_context.reset(token)


@contextmanager
def redirect_stderr(stream: _SupportsRedirectT, /) -> Generator[_SupportsRedirectT, Any, None]:
    """Re-direct [sys.stderr][] generated in the current context.

    See Also:
        - [contextlib.redirect_stderr][]
    """
    token = _stderr_context.set(stream)
    try:
        yield stream
    finally:
        _stderr_context.reset(token)


@contextmanager
def show_result(show: bool = True, /) -> Generator[None, Any, None]:
    """A context where `show_result` is enabled/disabled."""
    token = _show_result_context.set(show)
    try:
        yield
    finally:
        if token:
            _show_result_context.reset(token)


def show_result_enabled() -> bool:
    """Is the output suppressed in the current context?"""
    return _show_result_context.get()
