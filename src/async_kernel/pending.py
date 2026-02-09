from __future__ import annotations

import contextlib
import contextvars
import reprlib
import uuid
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, overload

import anyio
from aiologic import Event
from aiologic.lowlevel import async_checkpoint, create_async_event, enable_signal_safety, green_checkpoint
from typing_extensions import override

import async_kernel
from async_kernel.common import Fixed
from async_kernel.typing import T

__all__ = ["InvalidStateError", "Pending", "PendingCancelled", "PendingGroup", "PendingManager", "PendingTracker"]

truncated_rep = reprlib.Repr()
truncated_rep.maxlevel = 1
truncated_rep.maxother = 100
truncated_rep.fillvalue = "…"


class PendingCancelled(anyio.ClosedResourceError):
    "Used to indicate the pending is cancelled."


class InvalidStateError(RuntimeError):
    "An invalid state of the pending."


class PendingTracker:
    """
    The base class for tracking [Pending][async_kernel.pending.Pending].
    """

    _active_classes: ClassVar[set[type[Self]]] = set()
    _active_trackers: ClassVar[dict[str, Self]] = {}
    _contextvar: ClassVar[contextvars.ContextVar[str | None]] = contextvars.ContextVar("PendingManager", default=None)

    _active = False
    _pending: Fixed[Self, set[Pending[Any]]] = Fixed(set)
    _tracking: bool = False
    _parent_context_id: None | str = None

    context_id: Fixed[Self, str] = Fixed(lambda _: str(uuid.uuid4()))
    "The context id (per instance)."

    @property
    def active(self) -> bool:
        return self._active

    @property
    def pending(self) -> set[Pending[Any]]:
        return self._pending.copy()

    def __init_subclass__(cls) -> None:
        # Each subclass is assigned a new context variable.
        cls._contextvar = contextvars.ContextVar(f"{cls.__module__}.{cls.__name__}", default=None)
        return super().__init_subclass__()

    @classmethod
    def add_to_pending_trackers(cls, pen: Pending) -> None:
        """
        Add to all active pending trackers (including subclasses of PendingTracker) in the current context.

        This method gets called automatically by [Pending][async_kernel.pending.Pending.__init__]
        for all new instances (except for those that opt-out).
        """
        # Called by `Pending` when a new instance for each new instance.
        if trackers := pen.trackers:
            for cls_ in cls._active_classes:
                if (
                    issubclass(cls_, trackers)
                    and (id_ := cls_._contextvar.get())
                    and (pm := cls._active_trackers.get(id_))
                ):
                    pm.add(pen)

    @classmethod
    def current(cls) -> Self | None:
        "The current instance of this class for the current context."
        if (id_ := cls._contextvar.get()) and (current := cls._active_trackers.get(id_)):
            return current
        return None

    def start_tracking(self) -> contextvars.Token[str | None]:
        """
        Start tracking `Pending` in the  current context.
        """
        if self._tracking or not self.active:
            raise InvalidStateError
        assert self._active
        self._active_classes.add(self.__class__)
        self._active_trackers[self.context_id] = self
        self._parent_context_id = self._contextvar.get()
        self._tracking = True
        return self._contextvar.set(self.context_id)

    def stop_tracking(self, token: contextvars.Token[str | None]) -> None:
        """
        Stop tracking using the token.

        Args:
            token: The token returned from [start_tracking][].
        """
        self._contextvar.reset(token)
        self._tracking = False
        self._parent_context_id = None

    def add(self, pen: Pending) -> None:
        "Track `Pending` until it is done."

        if self._active and isinstance(self, pen.trackers) and (pen not in self._pending):
            self._pending.add(pen)
            pen.add_done_callback(self.on_pending_done)
        if (id_ := self._parent_context_id) and (parent := self._active_trackers.get(id_)):
            parent.add(pen)

    def on_pending_done(self, pen: Pending) -> None:
        "A done_callback that is registered with pen when it is added (don't call directly)."
        self._pending.discard(pen)

    def remove(self, pen: Pending) -> None:
        "Remove a `Pending`."
        self._pending.remove(pen)
        pen.remove_done_callback(self.discard)

    def discard(self, pen: Pending) -> None:
        "Discard the `Pending`."
        try:
            self.remove(pen)
        except IndexError:
            pass


class PendingManager(PendingTracker):
    """
    PendingManager is a context-aware manager for tracking [Pending][async_kernel.pending.Pending].

    This class maintains a registry of Pending created within a given context, allowing for activation,
    deactivation, and context management using Python's contextvars. It supports manual addition and
    removal of Pending, and can automatically cancel outstanding tasks when deactivated.
    """

    def activate(self) -> Self:
        """
        Enter the active state to begin tracking pending.
        """
        assert not self._active
        self._active_trackers[self.context_id] = self
        self._active_classes.add(self.__class__)
        self._active = True
        return self

    def deactivate(self) -> None:
        """
        Leave the active state cancelling all pending.
        """
        self._active = False
        self._active_trackers.pop(self.context_id, None)
        for pen in self._pending.copy():
            pen.cancel(f"{self} has been deactivated")


class PendingGroup(PendingTracker, anyio.AsyncContextManagerMixin):
    """
    An asynchronous context manager that automatically registers pending created in its context.

    All pending created within the context of `PendingGroup` provided that the `PendingGroup` is an instance
    of [Pending.trackers][] will be automatically added to the group (default for `Pending`).

    If any pending fails, is cancelled (with the result/exception set) or the pending group is cancelled;
    the context will exit, and all pending will be cancelled.

    Features:
        - The context will exit after all tracked pending are done or removed.
        - Cancelled or failed pending will cancel all other pending in the group.
        - Pending can be manually removed from the group while the group is active.

    Args:
        shield: [Shield][anyio.CancelScope.shield] from external cancellation.

    Usage:
        Enter the async context and create new pending.

        ```python
        async with PendingGroup() as pg:
            assert pg.caller.to_thread(lambda: None) in pg.pending
        ```
    """

    _cancel_scope: anyio.CancelScope
    _cancelled: str | None = None
    cancellation_timeout = 10
    "The maximum time to wait for cancelled pending to be done."

    caller = Fixed(lambda _: async_kernel.Caller())

    def __init__(self, *, shield: bool = False) -> None:
        self.caller  # noqa: B018
        self._shield = shield
        super().__init__()

    @contextlib.asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        self._cancel_scope = anyio.CancelScope(shield=self._shield)
        self._all_done = create_async_event()
        self._active = True
        self._leaving_context = False
        token = self.start_tracking()
        try:
            with self._cancel_scope:
                try:
                    yield self
                    self._leaving_context = True
                    if self._pending:
                        await self._all_done
                except (anyio.get_cancelled_exc_class(), Exception) as e:
                    self.cancel(f"An error occurred: {e!r}")
                    raise
            if self._cancelled is not None:
                raise PendingCancelled(self._cancelled)
        finally:
            self._leaving_context = True
            self.stop_tracking(token)
            if self._pending:
                if self._all_done or self._all_done.cancelled():
                    self._all_done = create_async_event()
                if self._pending and not self._all_done:
                    with anyio.CancelScope(shield=True), anyio.move_on_after(self.cancellation_timeout):
                        await self._all_done
            self._active = False

    @override
    def add(self, pen: Pending):
        assert self._active
        if self._cancelled is not None:
            msg = f"Trying to add to a cancelled PendingGroup.\nCancellation messages: {self._cancelled}"
            pen.cancel(msg)
        else:
            super().add(pen)

    @override
    def on_pending_done(self, pen: Pending) -> None:
        try:
            self._pending.remove(pen)
            if self._active and (not pen.cancelled() and (pen.exception())):
                self.cancel(f"Exception in member: {pen}")
        except KeyError:
            pass
        if self._leaving_context and not self._pending:
            self._all_done.set()

    @enable_signal_safety
    def cancel(self, msg: str | None = None) -> bool:
        "Cancel the pending group (thread-safe)."
        if self._active:
            self._cancelled = "\n".join(((self._cancelled or ""), msg or ""))
            if not self._cancel_scope.cancel_called:
                self.caller.call_direct(self._cancel_scope.cancel, msg)
                for pen_ in self.pending:
                    pen_.cancel(msg)
        return self.cancelled()

    def cancelled(self) -> bool:
        """Return True if the pending group is cancelled."""
        return bool(self._cancelled)


class Pending(Awaitable[T]):
    """
    A thread-safe, cancellable, awaitable object representing a pending asynchronous result.

    The `Pending` class provides a mechanism for waiting on a result or exception to be set,
    either asynchronously or synchronously. It supports cancellation, metadata storage, and
    callback registration for completion events.
    """

    __slots__ = [
        "__weakref__",
        "_cancelled",
        "_canceller",
        "_done",
        "_done_callbacks",
        "_exception",
        "_result",
        "trackers",
    ]

    REPR_OMIT: ClassVar[set[str]] = {"func", "args", "kwargs"}
    "Keys of metadata to omit when creating a repr of the instance."

    _metadata_mappings: ClassVar[dict[int, dict[str, Any]]] = {}
    "A mapping of instance's id its metadata."

    _cancelled: str | None
    _canceller: Callable[[str | None], Any]
    _exception: Exception
    _done: bool
    _result: T
    trackers: type[PendingTracker] | tuple[type[PendingTracker], ...]
    """
    A tuple of [async_kernel.pending.PendingTracker][] subclasses that the pending is permitted to register with.

    Should be specified during init.
    
    For some pending it may not make sense for it to be added to a [PendingGroup][]
    Instead specify `(PendingManager,)` instead of `(PendingTracker,)`.
    """

    @property
    def metadata(self) -> dict[str, Any]:
        """
        The metadata passed as keyword arguments to the instance during creation.
        """
        return self._metadata_mappings[id(self)]

    def __init__(
        self, *, trackers: type[PendingTracker] | tuple[type[PendingTracker], ...] = PendingTracker, **metadata: Any
    ):
        """
        Initializes a new Pending object with optional creation options and metadata.

        Args:
            trackers: A subclass or tuple of `PendingTracker` subclasses to which the pending can be added given the context.
            **metadata: Arbitrary keyword arguments containing metadata to associate with this Pending instance.
                trackers: Enabled by default. To disable tracking pass `trackers=False`

        Behavior:
            - Initializes internal state for tracking completion and cancellation
            - Stores provided metadata in a class-level mapping
            - Registers with [async_kernel.pending.PendingTracker.add_to_pending_trackers][]
        """
        self._done_callbacks: deque[Callable[[Self], Any]] = deque()
        self._metadata_mappings[id(self)] = metadata
        self._done = False
        self._cancelled = None
        self.trackers = trackers
        if trackers:
            PendingTracker.add_to_pending_trackers(self)

    def __del__(self):
        self._metadata_mappings.pop(id(self), None)

    @override
    def __repr__(self) -> str:
        rep = (
            "<Pending"
            + ((" ⛔" + (f"message={self._cancelled!s}" if self._cancelled else "")) if self.cancelled() else "")
            + ((f" ❗ {e!r}" if (e := getattr(self, "_exception", None)) else " 🏁") if self._done else " 🏃")
        )
        rep = f"{rep} at {id(self)}"
        with contextlib.suppress(Exception):
            if md := self.metadata:
                rep = f"{rep} metadata:"
                if "func" in md:
                    items = [f"{k}={truncated_rep.repr(v)}" for k, v in md.items() if k not in self.REPR_OMIT]
                    rep += f" | {md['func']} {' | '.join(items) if items else ''}"
                else:
                    rep += f"{truncated_rep.repr(md)}"
        return rep + " >"

    @override
    def __await__(self) -> Generator[Any, None, T]:
        return self.wait().__await__()

    if TYPE_CHECKING:

        @overload
        async def wait(
            self, *, timeout: float | None = ..., protect: bool = False | ..., result: Literal[True] = True
        ) -> T: ...

        @overload
        async def wait(self, *, timeout: float | None = ..., protect: bool = ..., result: Literal[False]) -> None: ...

    async def wait(self, *, timeout: float | None = None, protect: bool = False, result: bool = True) -> T | None:
        """
        Wait for a result or exception to be set (thread-safe) returning the pending if specified.

        Args:
            timeout: Timeout in seconds.
            protect: Protect the instance from external cancellation.
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
            else:
                await async_checkpoint(force=True)
            return self.result() if result else None
        except (anyio.get_cancelled_exc_class(), TimeoutError) as e:
            if not protect:
                self.cancel(f"Cancelled due to cancellation or timeout: {e}.")
            raise

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
            self._done_callbacks.appendleft(lambda _: done.set())
            if not self._done:
                done.wait(timeout)
            if not self._done:
                msg = f"Timeout waiting for {self}"
                raise TimeoutError(msg)
        else:
            green_checkpoint(force=True)
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

    def set_result(self, value: T, *, reset: bool = False) -> None:
        """
        Set the result (low-level-thread-safe).

        Args:
            value: The result.
            reset: Revert to being not done.

        Warning:
            - When using reset ensure to proivide sufficient time for any waiters to retrieve the result.
        """
        self._set_done("result", value)
        if reset:
            self._done = False

    def set_exception(self, exception: BaseException) -> None:
        """
        Set the exception (low-level-thread-safe).
        """
        self._set_done("exception", exception)

    @enable_signal_safety
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
            if (cancelled := self._cancelled or "") and msg:
                msg = f"{cancelled}\n{msg}"
            self._cancelled = msg or cancelled
            if canceller := getattr(self, "_canceller", None):
                canceller(msg)
        return self.cancelled()

    def cancelled(self) -> bool:
        """Return True if the pending is cancelled."""
        return self._cancelled is not None

    def set_canceller(self, canceller: Callable[[str | None], Any]) -> None:
        """
        Set a callback to handle cancellation (low-level).

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
        if e := self.exception():
            raise e from None
        try:
            return self._result
        except AttributeError:
            raise InvalidStateError from None

    def exception(self) -> BaseException | None:
        """
        Return the exception.

        Raises:
            PendingCancelled: If the instance has been cancelled.
        """
        if self._cancelled is not None:
            raise PendingCancelled(self._cancelled)
        return getattr(self, "_exception", None)
