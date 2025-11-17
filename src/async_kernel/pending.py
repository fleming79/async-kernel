from __future__ import annotations

import contextlib
import reprlib
from collections import deque
from collections.abc import Awaitable, Callable, Generator
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, overload

import anyio
from aiologic import Event
from aiologic.lowlevel import create_async_event
from typing_extensions import override

from async_kernel.typing import T

__all__ = ["InvalidStateError", "Pending", "PendingCancelled"]

truncated_rep = reprlib.Repr()
truncated_rep.maxlevel = 1
truncated_rep.maxother = 100
truncated_rep.fillvalue = "…"


class PendingCancelled(anyio.ClosedResourceError):
    "Used to indicate the pending is cancelled."


class InvalidStateError(RuntimeError):
    "An invalid state of the pending."


class Pending(Awaitable[T]):
    """
    A thread-safe, awaitable object representing a pending asynchronous result.

    The `Pending` class provides a mechanism for waiting on a result or exception to be set,
    either asynchronously or synchronously. It supports cancellation, metadata storage, and
    callback registration for completion events.

    Attributes:
        **metadata: Arbitrary keyword arguments to associate as metadata with the instance.

    Properties:
        metadata (dict[str, Any]): Metadata passed during creation.
    """

    __slots__ = ["__weakref__", "_cancelled", "_canceller", "_done", "_done_callbacks", "_exception", "_result"]

    REPR_OMIT: ClassVar[set[str]] = {"func", "args", "kwargs"}
    "Keys of metadata to omit when creating a repr of the instance."

    _metadata_mappings: ClassVar[dict[int, dict[str, Any]]] = {}
    "A mapping of instance's id its metadata."

    _cancelled: str
    _canceller: Callable[[str | None], Any]
    _exception: Exception
    _done: bool
    _result: T

    @property
    def metadata(self) -> dict[str, Any]:
        """
        The metadata passed as keyword arguments to the instance during creation.
        """
        return self._metadata_mappings[id(self)]

    def __init__(self, **metadata) -> None:
        self._done_callbacks: deque[Callable[[Self], Any]] = deque()
        self._metadata_mappings[id(self)] = metadata
        self._done = False

    def __del__(self):
        self._metadata_mappings.pop(id(self), None)

    @override
    def __repr__(self) -> str:
        rep = "<Pending" + (" ⛔" if self.cancelled() else "") + (" 🏁" if self._done else " 🏃")
        rep = f"{rep} at {id(self)}"
        with contextlib.suppress(Exception):
            md = self.metadata
            if "func" in md:
                items = [f"{k}={truncated_rep.repr(v)}" for k, v in md.items() if k not in self.REPR_OMIT]
                rep += f" | {md['func']} {' | '.join(items) if items else ''}"
            else:
                rep += f" {truncated_rep.repr(md)}" if md else ""
        return rep + " >"

    @override
    def __await__(self) -> Generator[Any, None, T]:
        return self.wait().__await__()

    if TYPE_CHECKING:

        @overload
        async def wait(
            self, *, timeout: float | None = ..., shield: bool = False | ..., result: Literal[True] = True
        ) -> T: ...

        @overload
        async def wait(self, *, timeout: float | None = ..., shield: bool = ..., result: Literal[False]) -> None: ...

    async def wait(self, *, timeout: float | None = None, shield: bool = False, result: bool = True) -> T | None:
        """
        Wait for a result or exception to be set (thread-safe) returning the pending if specified.

        Args:
            timeout: Timeout in seconds.
            shield: Shield the instance from external cancellation.
            result: Whether the result should be returned (use `result=False` to avoid exceptions raised by [Pending.result][]).

        Raises:
            TimeoutError: When the timeout expires and a result or exception has not been set.
            PendingCancelled: If `result=True` and the pending has been cancelled.
            Exception: If `result=True` and an exception was set on the pending.
        """
        try:
            if not self._done or self._done_callbacks:
                event = create_async_event()
                self._done_callbacks.appendleft(lambda _: event.set())
                with anyio.fail_after(timeout):
                    if not self._done or self._done_callbacks:
                        await event
            return self.result() if result else None
        finally:
            if not self._done and not shield:
                self.cancel("Cancelled with waiter cancellation.")

    if TYPE_CHECKING:

        @overload
        def wait_sync(self, *, timeout: float | None = ..., result: Literal[True] = True) -> T: ...

        @overload
        def wait_sync(self, *, timeout: float | None = ..., result: Literal[False]) -> None: ...

    def wait_sync(self, *, timeout: float | None = None, result: bool = True) -> T | None:
        """
        Wait synchronously for the a result or exception to be set (thread-safe) blocking the current thread.

        Args:
            timeout: Timeout in seconds.
            result: Whether the result should be returned (use `result=False` to avoid exceptions raised by [Pending.result][]).

        Raises:
            TimeoutError: When the timeout expires and a result or exception has not been set.
            PendingCancelled: If `result=True` and the pending has been cancelled.
            Exception: If `result=True` and an exception was set on the pending.

        Warning:
            **Blocking the thread in which the result or exception is set will cause in deadlock.**
        """
        if not self._done:
            done = Event()
            self.add_done_callback(lambda _: done.set())
            if not self._done:
                done.wait(timeout)
            if not self._done:
                msg = f"Timeout waiting for {self}"
                raise TimeoutError(msg)

        return self.result() if result else None

    def _set_done(self, mode: Literal["result", "exception"], value) -> None:
        if self._done:
            raise InvalidStateError
        self._done = True
        setattr(self, "_" + mode, value)
        while self._done_callbacks:
            cb = self._done_callbacks.pop()
            try:
                cb(self)
            except Exception:
                pass

    def set_result(self, value: T) -> None:
        "Set the result (thread-safe)."
        self._set_done("result", value)

    def set_exception(self, exception: BaseException) -> None:
        "Set the exception (thread-safe)."
        self._set_done("exception", exception)

    def cancel(self, msg: str | None = None) -> bool:
        """
        Cancel the instance.

        Args:
            msg: The message to use when cancelling.

        Notes:
            - Cancellation cannot be undone.
            - The result will not be *done* until either [Pending.set_result][] or [Pending.set_exception][] is called.

        Returns: If it has been cancelled.
        """
        if not self._done:
            cancelled = getattr(self, "_cancelled", "")
            if msg and isinstance(cancelled, str):
                msg = f"{cancelled}\n{msg}"
            self._cancelled = msg or cancelled
            if canceller := getattr(self, "_canceller", None):
                canceller(msg)
        return self.cancelled()

    def cancelled(self) -> bool:
        """Return True if the pending is cancelled."""
        return isinstance(getattr(self, "_cancelled", None), str)

    def set_canceller(self, canceller: Callable[[str | None], Any]) -> None:
        """
        Set a callback to handle cancellation.

        Args:
            canceller: A callback that performs the cancellation of the pending.
                - It must accept the cancellation message as the first argument.
                - The cancellation call is not thread-safe.

        Notes:
            - `set_result` must be called to mark the pending as completed.

        Example:
            ```python
            pen = Pending()
            pen.cancel()
            assert not pen.done()
            pen.set_canceller(lambda msg: pen.set_result(None))
            assert pen.done()
            ```
        """
        if self._done or hasattr(self, "_canceller"):
            raise InvalidStateError
        self._canceller = canceller
        if self.cancelled():
            self.cancel()

    def done(self) -> bool:
        """
        Returns True if a result or exception has been set.
        """
        return self._done

    def add_done_callback(self, fn: Callable[[Self], Any]) -> None:
        """
        Add a callback for when the pending is done (not thread-safe).

        If the pending is already done it will called immediately.
        """
        if not self._done:
            self._done_callbacks.append(fn)
        else:
            fn(self)

    def remove_done_callback(self, fn: Callable[[Self], object], /) -> int:
        """
        Remove all instances of a callback from the callbacks list.

        Returns the number of callbacks removed.
        """
        n = 0
        while fn in self._done_callbacks:
            n += 1
            self._done_callbacks.remove(fn)
        return n

    def result(self) -> T:
        """
        Return the result.

        Raises:
            PendingCancelled: If the pending has been cancelled.
            InvalidStateError: If the pending isn't done yet.
        """
        if not self._done and not self.cancelled():
            raise InvalidStateError
        if e := self.exception():
            raise e
        return self._result

    def exception(self) -> BaseException | None:
        """
        Return the exception.

        Raises:
            PendingCancelled: If the instance has been cancelled.
            InvalidStateError: If the instance isn't done yet.
        """
        if hasattr(self, "_cancelled"):
            raise PendingCancelled(self._cancelled)
        if not self._done:
            raise InvalidStateError
        return getattr(self, "_exception", None)
