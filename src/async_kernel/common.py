from __future__ import annotations

import inspect
import weakref
from typing import TYPE_CHECKING, Any, Generic, Never, Self

import aiologic
import aiologic.meta
from typing_extensions import override

from async_kernel.typing import FixedCreate, FixedCreated, S, T

if TYPE_CHECKING:
    from collections import deque
    from collections.abc import Callable

__all__ = ["Fixed", "LiteEvent", "LiteLock", "import_item"]


def import_item(dottedname: str) -> Any:
    """Import an item from a module, given its dotted name.

    Example:
        ```python
        import_item("os.path.join")
        ```
    """
    module, name0 = dottedname.rsplit(".", maxsplit=1)
    return aiologic.meta.import_from(module, name0)


class LiteLock(aiologic.Lock):
    """A lightweight variant of [aiologic.Lock][]."""

    # SPDX-FileCopyrightText: 2024 Ilya Egorov <0x42005e1f@gmail.com>
    # SPDX-License-Identifier: ISC
    # Modified 2026

    def __new__(cls, /) -> Self:
        self = object.__new__(cls)
        self._owner = None
        self._releasing = False
        self._unlocked = [None]
        self._waiters = []
        return self

    @override
    def _release(self, /) -> None:
        waiters: list | deque = self._waiters

        while True:
            self._releasing = True
            while waiters:
                try:
                    event, self._owner, _ = waiters.pop(0)
                except IndexError:
                    break
                else:
                    if event.set():
                        return
            self._owner = None
            self._releasing = False
            self._unlocked.append(None)
            if waiters:
                try:
                    self._unlocked.pop()
                except IndexError:
                    break
            else:
                break


class LiteEvent(aiologic.Event):
    """A lightweight variant of [aiologic.Lock][]."""

    # SPDX-FileCopyrightText: 2024 Ilya Egorov <0x42005e1f@gmail.com>
    # SPDX-License-Identifier: ISC
    # Modified 2026

    def __new__(cls, /) -> Self:
        self = object.__new__(cls)
        self._is_unset = True
        self._waiters = []
        return self

    @override
    def _wakeup(self, /) -> None:
        waiters: list | deque = self._waiters
        while waiters:
            try:
                event = waiters.pop(0)
            except IndexError:
                break
            else:
                event.set()


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

            - class: A class that is called with no arguments.
            - string: A dotted importable path to class or function that is called with no arguments.
            - callable: A callable that is called with one positional argument [FixedCreate][].

        created: An optional callback that gets called with [FixedCreated][] whenever a
            value is _created_.

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

    __slots__ = ["create", "created", "instances", "lock", "name"]

    def __init__(
        self,
        obj: type[T] | Callable[[FixedCreate[S]], T] | str,
        /,
        *,
        created: Callable[[FixedCreated[S, T]]] | None = None,
    ) -> None:
        if isinstance(obj, str):
            self.create = lambda _: import_item(obj)()
        elif inspect.isclass(obj):
            self.create = lambda _: obj()
        elif callable(obj):
            self.create = obj
        else:
            msg = f"{obj=} is invalid! Use a lambda instead eg: lambda _: {obj}"  # pyright: ignore[reportUnreachable]
            raise TypeError(msg)
        self.created = created
        self.instances = {}
        self.lock = LiteLock()

    def __set_name__(self, owner_cls: type[S], name: str) -> None:
        self.name = name

    def __get__(self, obj: S, objtype: type[S] | None = None) -> T:
        try:
            return self.instances[id(obj)]
        except KeyError:
            if obj is None:
                return self  # pyright: ignore[reportReturnType]
            with self.lock:
                try:
                    return self.instances[id(obj)]
                except KeyError:
                    key = id(obj)
                    instance: T = self.create({"name": self.name, "owner": obj})  # pyright: ignore[reportAssignmentType]
                    self.instances[key] = instance
                    weakref.finalize(obj, self.instances.pop, key)
            if self.created:
                try:
                    self.created({"owner": obj, "obj": instance, "name": self.name})
                except Exception:
                    if log := getattr(obj, "log", None):
                        msg = f"Callback `created` failed for {obj.__class__}.{self.name}"
                        log.exception(msg, extra={"obj": self.created})
            return instance

    def __set__(self, obj: S, value: Self) -> Never:
        # Note: above we use `Self` for the `value` type hint to give a useful typing error
        msg = f"Setting `Fixed` parameter {obj.__class__.__name__}.{self.name} is forbidden!"
        raise AttributeError(msg)
