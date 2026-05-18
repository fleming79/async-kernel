from __future__ import annotations

import inspect
import weakref
from collections import deque
from types import coroutine
from typing import TYPE_CHECKING, Any, Generic, Literal, Never, Self

import aiologic.meta
from aiologic.lowlevel import THREAD_DUMMY_LOCK, create_async_event, create_thread_oncelock
from sniffio import current_async_library
from wrapt import lazy_import

from async_kernel.typing import Backend, FixedCreate, FixedCreated, S, T

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable

__all__ = ["Fixed", "KernelInterrupt", "SingleAsyncQueue", "import_item"]


trio_checkpoint: Callable[[], Awaitable] = lazy_import("trio.lowlevel", "checkpoint")  # pyright: ignore[reportAssignmentType]
globals()["trio"] = lazy_import("trio")
globals()["BaseInterface"] = lazy_import("async_kernel.interface.base", "BaseInterface")


def import_item(dottedname: str) -> Any:
    """
    Import an item from a module, given its dotted name.

    Example:
        ```python
        import_item("os.path.join")
        ```
    """
    module, name = dottedname.rsplit(".", maxsplit=1)
    return aiologic.meta.import_from(module, name)


@coroutine
def asyncio_checkpoint():
    yield


def noop() -> None:
    pass


class KernelInterrupt(InterruptedError):
    "Raised to interrupt the kernel."


class Fixed(Generic[S, T]):
    """
    A property-like descriptor factory that always returns the same object.

    The descriptor is defined with a callable or importable string to a callable.

    On first access the object is obtained as the result of the callable, cached
    and returned. Subsequent access to the property returns the cached result.

    Args:
        obj:
            A class, callable or dotted path.

            The following types are accepted:

            - string: A dotted importable path to class or function to be used as callable.
            - class | callable: Called with zero or one positional argument [FixedCreate][].

        created: A per-instance optional callback that gets called on first-access to the property.
        use_weakref: Use a [weakref.WeakValueDictionary][] to store instances. This can be
            used to avoid circular-references.
        set_mode: How to handle when __set__ is called.
            - `'raise'`: Raise an error.
            - 'ignore': Ignore.
            - `'log.<...>'`: Make a log entry ignoring the input.

    Type Hints:
        - ``S``: Type of the owner class.
        - ``T``: Type of the managed class.

    Example:
        ```python
        class MyClass:
            a: Fixed[Self, dict] = Fixed(dict)
            b: Fixed[Self, dict] = Fixed(lambda c: id(c["owner"].a))
            c: Fixed[Self, list[str]] = Fixed(
                list, created=lambda c: c["obj"].append(c["name"])
            )
        ```

    Tip:
        You can use [import_item][] inside a callable to lazy import.
    """

    __slots__ = ["create", "created", "instances", "instances_locks", "name", "set_mode"]

    def __init__(
        self,
        obj: type[T] | Callable[[], T] | Callable[[FixedCreate[S]], T] | str,
        /,
        *,
        created: Callable[[FixedCreated[S, T]]] | None = None,
        use_weakref: bool = False,
        set_mode: Literal["raise", "ignore"] = "raise",
    ) -> None:
        if callable(obj) or isinstance(obj, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            self.create = obj
            self.created = created
            self.instances = weakref.WeakValueDictionary() if use_weakref else {}
            self.instances_locks = {}
            self.set_mode: Literal["raise", "ignore"] = set_mode
        else:
            msg = f"{obj=} is invalid! Use a lambda instead eg: lambda _: {obj}"  # pyright: ignore[reportUnreachable]
            raise TypeError(msg)

    def __set_name__(self, owner_cls: type[S], name: str) -> None:
        self.name = name

    def __get__(self, obj: S, objtype: type[S] | None = None) -> T:
        if obj is None:
            return self  # pyright: ignore[reportReturnType]
        key = id(obj)
        try:
            return self.instances[key]
        except KeyError:
            try:
                lock = self.instances_locks[key]
            except KeyError:
                lock = self.instances_locks.setdefault(key, create_thread_oncelock())
            lock.acquire()
            try:
                return self.instances[key]
            except KeyError:
                if lock._count > 1:
                    msg = f"Self-referencing creation detected for {obj.__class__.__name__}.{self.name}!"
                    raise RuntimeError(msg) from None
                return self.create_instance(obj, key)
            finally:
                lock.release()

    def create_instance(self, obj: S, key: int) -> T:
        if isinstance(create := self.create, str):
            self.create = create = import_item(create)
        try:
            instance = create()  # pyright: ignore[reportCallIssue, reportAssignmentType]
        except TypeError as e1:
            try:
                instance: T = create(FixedCreate(name=self.name, owner=obj))  # pyright: ignore[reportAssignmentType, reportCallIssue]
            except Exception as e2:
                if len(inspect.signature(create).parameters) == 0:
                    raise e1 from None
                raise e2 from None

        self.instances[key] = instance
        self.instances_locks[key] = THREAD_DUMMY_LOCK
        if not isinstance(self.instances, weakref.WeakValueDictionary):
            weakref.finalize(obj, self.instances.pop, key)
        weakref.finalize(obj, self.instances_locks.pop, key)
        if self.created:
            try:
                self.created({"owner": obj, "obj": instance, "name": self.name})
            except Exception:
                if log := getattr(obj, "log", None):
                    msg = f"Callback `created` failed for {obj.__class__}.{self.name}"
                    log.exception(msg, extra={"obj": self.created})
        return instance

    def __set__(self, obj: S, value: Self) -> Never:  # pyright: ignore[reportReturnType]
        # Note: We use `Self` as the type hint for the `value` above  to give a useful typing error.

        match self.set_mode:
            case "raise":
                msg = f"Setting `Fixed` parameter {obj.__class__.__module__}.{obj.__class__.__name__}.{self.name} is forbidden!"
                raise AttributeError(msg)
            case _:
                pass


class SingleAsyncQueue(Generic[T]):
    """
    A single-use asynchronous iterator with a queue.

    Notes:
        - Append to the queue from anywhere (internally synchronised).
        - The queue will only yield for one async iterator consumer.
        - When [SingleAsyncQueue.stop][] is called:
            - Any items in the queue are immediately rejected.
            - The async iterator is stopped.
        - Items added after stop is called will be rejected immediately.

    Usage:
        ```python
        q = SingleAsyncQueue(reject=lambda item: print("rejected", item))

        # In a task
        async for item in q:
            q

        # Other threads/tasks
        q.append(item)
        q.extent([item1, item2])

        # Stop the iterator
        q.stop()
        ```
    """

    __slots__ = ["__weakref__", "_active", "_reject", "_resume"]

    _active: bool | None
    queue: Fixed[Self, deque[T]] = Fixed(deque)

    def __init__(self, *, reject: Callable[[T], Any] | None = None) -> None:
        self._resume = noop
        self._active = None
        self._reject = reject

    async def __aiter__(self) -> AsyncGenerator[T]:
        if self._active is not None:
            return
        backend = Backend(current_async_library())
        checkpoint = asyncio_checkpoint if backend is Backend.asyncio else trio_checkpoint
        self._active = True
        queue = self.queue
        try:
            while self._active:
                if queue:
                    yield queue.popleft()
                    await checkpoint()
                else:
                    event = create_async_event()
                    self._resume = event.set
                    if not queue and self._active:
                        await event
                    self._resume = noop
        except IndexError:
            pass
        finally:
            self._resume = noop
            self.stop()

    def stop(self) -> None:
        """
        Stop the queue rejecting any items currently in the queue.
        """
        self._active = False
        if self._reject:
            while True:
                try:
                    self._reject(self.queue.popleft())
                except IndexError:
                    break
        else:
            self.queue.clear()
        self._resume()

    def append(self, item: T, /) -> None:
        """
        Append `item` to the queue.

        If the queue has been stopped `item` will be rejected immediately.
        """
        if self._active is False:
            if self._reject:
                self._reject(item)
        else:
            self.queue.append(item)
            self._resume()

    def appendleft(self, item: T, /) -> None:
        """
        Append `item` to the left side of the queue.

        If the queue has been stopped `item` will be rejected immediately.
        """
        if self._active is False:
            if self._reject:
                self._reject(item)
        else:
            self.queue.appendleft(item)
            self._resume()

    def extend(self, iterable: Iterable[T], /) -> None:
        """
        Append all items in `iterable` to the queue.

        If the queue has been stopped all items in `iterable` will be rejected immediately.
        """
        if self._active is False:
            if self._reject:
                for item in iterable:
                    self._reject(item)
        else:
            self.queue.extend(iterable)
            self._resume()

    @property
    def stopped(self) -> bool:
        """
        Will return `True` once stop has been called meaning there are no items left in the queue.
        """
        return self._active is False
