from __future__ import annotations

import contextlib
import contextvars
import reprlib
import uuid
import weakref
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, final, overload

import anyio
from aiologic import Event
from aiologic.lowlevel import create_async_event, enable_signal_safety
from typing_extensions import override

import async_kernel
from async_kernel.common import Fixed
from async_kernel.typing import T

if TYPE_CHECKING:
    from _contextvars import Token

    from async_kernel.caller import Caller

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

    _subclasses: ClassVar[tuple[type[Self], ...]] = ()
    _instances: ClassVar[weakref.WeakValueDictionary[str, Self]] = weakref.WeakValueDictionary()
    _id_contextvar: ClassVar[contextvars.ContextVar[str | None]]
    _pending: Fixed[Self, set[Pending[Any]]] = Fixed(set)

    id: Fixed[Self, str] = Fixed(lambda _: str(uuid.uuid4()))
    """The unique id of the pending tracker instance."""

    @property
    def pending(self) -> set[Pending[Any]]:
        "The pending currently associated with this instance."
        return self._pending.copy()

    def __init_subclass__(cls) -> None:
        if cls.__name__ != "PendingManager":
            PendingTracker._subclasses = (*cls._subclasses, cls)
            # Each subclass is assigned a new context variable.
            cls._id_contextvar = contextvars.ContextVar(f"{cls.__module__}.{cls.__name__}", default=None)
        return super().__init_subclass__()

    @classmethod
    def current(cls) -> Self | None:
        "The instance of the active tracker in the current context."
        if (id_ := cls._id_contextvar.get()) and (current := cls._instances.get(id_)):
            return current
        return None

    @classmethod
    def active_id(cls) -> str | None:
        "The id of the active tracker in the current context."
        return cls._id_contextvar.get()

    def __init__(self) -> None:
        self._instances[self.id] = self

    def _activate(self) -> Token[str | None]:
        try:
            return self._id_contextvar.set(self.id)
        except AttributeError as e:
            e.add_note("Pending tracker must be subclassed to use it!")
            raise

    def _deactivate(self, token: contextvars.Token[str | None]) -> None:
        self._id_contextvar.reset(token)

    def add(self, pen: Pending) -> None:
        "Track `Pending` until it is done."

        if pen not in self._pending:
            self._pending.add(pen)
            pen.add_done_callback(self._on_pending_done)

    def _on_pending_done(self, pen: Pending) -> None:
        "A done_callback that is registered with pen when it is added (don't call directly)."
        self._pending.discard(pen)


class PendingManager(PendingTracker):
    """
    PendingManager is a `PendingTracker` subclass for tracking the creation of [async_kernel.pending.Pending][]
    in multiple contexts.

    This class must be subclassed to be useful.

    For each subclass there is zero or one active trackers at a time. Activating a manager will 'replace' a
    previously active pending manager.

    Notes:

        - A subclass of [PendingManager][] is required to use its functionality.
        - Each subclass is assigned it's own context variable.
            - This means that only one instance is ever active in a specific context at any time.
        - It is proportionally expensive to subclass PendingManager so it's usage should
            be limited to cases where it is necessary to start and stop tracking from specific contexts.

    Usage:

        ```python
        class MyPendingManager(PendingManager):
            "Manages the context of ..."


        m = MyPendingManager()
        m2 = MyPendingManager()

        # In one or more contexts
        token = m.activate()
        try:
            ...
            try:
                token2 = m2.activate()
                pen = m2.caller.call_soon(lambda: 1 + 1)
                assert pen in m2.pending
                assert (
                    pen not in m.pending
                ), "pen is associated should only be associated with m2"
                ...
            finally:
                m2.deactivate(token2)

        finally:
            m.deactivate(token)
        ```
    """

    def activate(self) -> contextvars.Token[str | None]:
        """
        Start tracking `Pending` in the  current context.
        """
        return self._activate()

    def deactivate(self, token: contextvars.Token[str | None]) -> None:
        """
        Stop tracking using the token.

        Args:
            token: The token returned from [activate][].
        """
        self._deactivate(token)

    def remove(self, pen: Pending) -> None:
        """
        Remove a pending from the manager.
        """
        self._pending.remove(pen)

    @contextlib.contextmanager
    def context(self) -> Generator[None, Any, None]:
        """A context manager where the pending manager is activated."""
        token = self.activate()
        try:
            yield
        finally:
            self.deactivate(token)


@final
class PendingGroup(PendingTracker, anyio.AsyncContextManagerMixin):
    """
    An asynchronous context manager for tracking [Pending][async_kernel.pending.Pending] created in a context.

    Usage:
        Enter the async context and create new pending.

        ```python
        async with PendingGroup() as pg:
            assert pg.caller.to_thread(lambda: None) in pg.pending
        ```
    """

    _parent_id: None | str = None
    _cancel_scope: anyio.CancelScope
    _cancelled: str | None = None
    _leaving_context: bool = False
    _failed: Fixed[Self, list[Pending]] = Fixed(list)
    cancellation_timeout = 10
    "The maximum time to wait for cancelled pending to be done."

    caller: Fixed[Self, Caller] = Fixed(lambda _: async_kernel.Caller())
    "The caller where the pending group was instantiated."

    def __init__(self, *, shield: bool = False, mode: int = 0) -> None:
        """
        An async context to capture all pending (that opt in) created in the context.

        The pending group will only exit once all pending in the group are done.

        Pending can be added to and removed from the group manually.

        Args:
            shield: Passed to the cancel scope.
            mode: The mode.
                - 0: Ignore cancellation of pending.
                - 1: Cancel if any pending is cancelled - raise PendingCancelled on exit.
                - 2: Cancel if any pending is cancelled - exit quietly.
        """
        assert mode in [0, 1, 2]
        self._mode = mode
        self._shield = shield
        self.caller  # noqa: B018
        super().__init__()

    @override
    def __repr__(self) -> str:
        info = " ⛔ cancelled" if self._cancelled else ""
        return f"<PendingGroup at {id(self)}{info} | {len(self.pending)} pending | mode:{self._mode}>"

    @override
    def _activate(self) -> Token[str | None]:
        self._parent_id = None if (parent_id := self._id_contextvar.get()) == self.id else parent_id
        return super()._activate()

    @contextlib.asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        if self._leaving_context:
            msg = f"Re-entry of {self.__class__} is not supported!"
            raise InvalidStateError(msg)
        self._cancel_scope = anyio.CancelScope(shield=self._shield)
        self._all_done = create_async_event()
        token = self._activate()
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
                if exceptions := [e for pen in self._failed if isinstance(e := pen.exception(), Exception)]:
                    msg = f"One or more exceptions occurred in this context! {list(map(str, exceptions))}"
                    raise ExceptionGroup(msg, exceptions)
                if self._mode in [0, 1]:
                    raise PendingCancelled(self._cancelled)
        finally:
            self._leaving_context = True
            self._deactivate(token)
            self._instances.pop(self.id, None)
            if self._pending:
                if self._all_done or self._all_done.cancelled():
                    self._all_done = create_async_event()
                if self._pending and not self._all_done:
                    with anyio.CancelScope(shield=True), anyio.move_on_after(self.cancellation_timeout):
                        await self._all_done

    @override
    def add(self, pen: Pending) -> None:
        if pen not in self._pending:
            self._pending.add(pen)
            pen.add_done_callback(self._on_pending_done)
        if (id_ := self._parent_id) and (parent := self._instances.get(id_)):
            parent.add(pen)

    @override
    def _on_pending_done(self, pen: Pending) -> None:
        try:
            self._pending.remove(pen)
            if pen.cancelled():
                if self._mode in [1, 2]:
                    self.cancel(f"A monitored pending was cancelled {pen=}")
            elif pen.exception():
                self._failed.append(pen)
                self.cancel(f"Exception in member: {pen}")
        except KeyError:
            pass
        if self._leaving_context and not self._pending:
            self._all_done.set()

    @enable_signal_safety
    def cancel(self, msg: str | None = None) -> bool:
        "Cancel the pending group (internally synchronised)."
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
    Pending is an internally synchronised, cancellable, awaitable class influenced by [asyncio.Future][].

    - It can be wait/awaited and cancelled from any thread (not considering deadlocks)
    - Provides metadata storage
    - Has built in support for tracking pending created in a specific context (see: [PendingManager][] and [PendingGroup][]).

    **Properties**

    - [Pending.metadata][]: Metadata associated with the pending.
    - [Pending.context][]: The context associated with the pending.

    **High level methods**

    - [Pending.wait][]: Wait asynchronously for the pending to be complete.
    - [Pending.wait_sync][]: Wait synchronously for the pending to be complete.
    - [Pending.cancel][]: Cancel the pending (result must be set to finalize cancellation).
    - [Pending.result][]: Get the result of the pending.
    - [Pending.exception][]: Get the exception of the pending.

    **Low level methods**

    - [Pending.add_done_callback][]: Add a callback that is called once the pending is complete.
    - [Pending.remove_done_callback][]: Remove a previously added callback.
    - [Pending.set_canceller][]: Set a callback to handle cancellation.
    """

    __slots__ = [
        "__weakref__",
        "_cancelled",
        "_canceller",
        "_done",
        "_done_callbacks",
        "_exception",
        "_result",
        "_waiting",
        "context",
    ]

    _REPR_OMIT: ClassVar[list[str]] = ["func", "args", "kwargs", "caller"]
    "Keys of metadata to omit when creating a repr of the pending."

    _metadata_mappings: ClassVar[dict[int, dict[str, Any]]] = {}
    "A mapping of pending ids to metadata."

    _cancelled: str | None
    _canceller: Callable[[str | None], Any]
    _exception: Exception
    _done: bool
    _waiting: bool
    _done_event = Fixed(Event)
    _result: T
    context: contextvars.Context | None
    """The context associated with Pending."""

    @property
    def metadata(self) -> dict[str, Any]:
        """
        The metadata associated with the pending.
        """
        return self._metadata_mappings[id(self)]

    def __init__(
        self,
        context: contextvars.Context | None = None,
        trackers: type[PendingTracker] | tuple[type[PendingTracker], ...] = (),
        /,
        **metadata: Any,
    ) -> None:
        """
        Initializes a new pending object with optional creation options and metadata.

        Args:
            context: A context to associate with the pending, if provided it is copied.
            trackers: A subclass or tuple of `PendingTracker` subclasses to which the pending can be added in the current context.
            **metadata: Arbitrary keyword arguments containing metadata to associate with this Pending instance.

        Behavior:
            - Initializes internal state for tracking completion and cancellation.
            - Stores provided metadata in a class-level mapping.
        """
        self._done_callbacks: list[Callable[[Self], Any]] = []
        self._metadata_mappings[id(self)] = metadata
        self._done = False
        self._waiting = False
        self._cancelled = None

        if trackers or context:
            # A copy the context is required to avoid `PendingTracker.id` leakage.
            context = context.copy() if context else contextvars.copy_context()
        self.context = context

        # PendingTacker registration.
        if context:
            for cls in PendingTracker._subclasses:  # pyright: ignore[reportPrivateUsage]
                if id_ := context.get(cls._id_contextvar):  # pyright: ignore[reportPrivateUsage]
                    if trackers and issubclass(cls, trackers) and (tracker := PendingTracker._instances.get(id_)):  # pyright: ignore[reportPrivateUsage]
                        tracker.add(self)
                    else:
                        # Clear `PendingTracker.id`.
                        context.run(cls._id_contextvar.set, None)  # pyright: ignore[reportPrivateUsage]

    def __del__(self) -> None:
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
                    items = [f"{k}={truncated_rep.repr(v)}" for k, v in md.items() if k not in self._REPR_OMIT]
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
        Wait for `result` or `exception` to be set (internally synchronised) returning the result if specified.

        Args:
            timeout: Timeout in seconds.
            protect: Protect the pending from external cancellation.
            result: If `result` should be returned.

        Raises:
            TimeoutError: When the timeout expires and a result or exception has not been set.
            PendingCancelled: If `result=True` and the pending has been cancelled.
            Exception: If `result=True` and an exception was set on the pending.

        Tip:
            To wait for a cancelled pending to complete use `await pen.wait(result=False)`.
        """
        try:
            if not self._done or self._done_callbacks:
                self._waiting = True
                if timeout is None:
                    await self._done_event
                else:
                    with anyio.fail_after(timeout):
                        await self._done_event
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
        Wait synchronously for `result` or `exception` (internally synchronised) returning the result if specified.

        Args:
            timeout: Timeout in seconds.
            result: When `True` the result is returned using `result`.

        Raises:
            TimeoutError: When the timeout expires and a result or exception has not been set.
            PendingCancelled: If `result=True` and the pending has been cancelled.
            Exception: If `result=True` and an exception was set on the pending.

        Warning:
            **Calling this method in the same thread where the result or exception is set
            will result in deadlock unless a greenlet based event library is in use.**
        """
        if not self._done or self._done_callbacks:
            self._waiting = True
            self._done_event.wait(timeout)
            if not self._done:
                msg = f"Timeout waiting for {self}"
                raise TimeoutError(msg)
        return self.result() if result else None

    def _set_done(self, mode: Literal["result", "exception"], value) -> None:
        if self._done:
            raise InvalidStateError
        self._done = True
        callbacks = self._done_callbacks
        callbacks.reverse()
        setattr(self, "_" + mode, None if self._cancelled else value)
        e = None
        try:
            while callbacks:
                try:
                    callbacks.pop()(self)
                except Exception:
                    pass
                except BaseException as exc:
                    e = exc
        finally:
            if self._waiting:
                self._done_event.set()
            if e:
                raise e from None

    def set_result(self, value: T) -> None:
        """
        Set the result (low-level internally synchronised).

        Args:
            value: The result to set.

        Raises:
            InvalidStateError: If the pending is already done.
        """
        self._set_done("result", value)

    def set_exception(self, exception: BaseException) -> None:
        """
        Set the exception (low-level internally synchronised).

        Args:
            exception: The value to set as the exception.

        Raises:
            InvalidStateError: If the pending is already done.
        """
        self._set_done("exception", exception)

    @enable_signal_safety
    def cancel(self, msg: str | None = None) -> bool:
        """
        Cancel the pending if the it is not already `done`.

        Args:
            msg: The message to use when a [PendingCancelled][] when accessing `result` or `exception`.
                The msg of for multiple cancellation calls are concatenated.

        Notes:
            - Cancellation cannot be undone.
            - If a canceller has already been set using [set_canceller][] it will be called.
            - The result will not be *done* until either [set_result][] or [set_exception][] is called.

        Returns: If the pending was cancelled.
        """
        if not self._done:
            if (cancelled := self._cancelled or "") and msg:
                msg = f"{cancelled}\n{msg}"
            self._cancelled = msg or cancelled
            if canceller := getattr(self, "_canceller", None):
                canceller(msg)
        return self.cancelled()

    async def cancel_wait_done(self, msg: str | None, *, timeout: float | None = None) -> None:
        "Cancel the pending and wait for it to be `done`."
        if not self._done:
            self.cancel(msg)
            await self.wait(result=False, timeout=timeout)

    def cancelled(self) -> bool:
        """
        Returns `True` if the pending is cancelled.

        Notes:

            - This can return `True` before a pending is `done`.
            - To wait for a pending to complete (assuming a canceller is or will be set); use
                `await pen.wait(result=False)` or `pen.wait_sync(result=False)`
        """
        return self._cancelled is not None

    def set_canceller(self, canceller: Callable[[str | None], Any]) -> None:
        """
        Set a callback to handle cancellation (low-level).

        Args:
            canceller: A callback that performs the cancellation of the pending.

                - It must accept the cancellation message as the first argument.
                - The canceller must be externally synchronised.

        Notes:
            - [set_result][] or [set_exception][] must be called by the `canceller` to mark the pending as completed.

        Raises:
            InvalidStateError: If already `done` or the canceller is already set.

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
        Add a callback for when the pending is done.

        The callback should be internally synchronised.

        Callbacks added prior to the pending being done are handled FIFO
        otherwise `fn` is called immediately.
        """
        if not self._done:
            self._done_callbacks.append(fn)
        else:
            fn(self)

    def remove_done_callback(self, fn: Callable[[Self], object], /) -> int:
        """
        Remove `fn` from the done callback list.

        Returns the number of items removed.
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
            PendingCancelled: If the pending has been cancelled.
        """
        if self._cancelled is not None:
            raise PendingCancelled(self._cancelled)
        return getattr(self, "_exception", None)
