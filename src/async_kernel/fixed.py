from __future__ import annotations

import importlib
import inspect
import weakref
from typing import TYPE_CHECKING, Generic, Self

from aiologic import BinarySemaphore

from async_kernel.typing import FixedCreate, FixedCreated, S, T

if TYPE_CHECKING:
    from collections.abc import Callable


def import_item(dottedname: str):
    """Import an item from a module, given its dotted name.

    Example:
        ```python
        import_item("os.path.join")
        ```
    """
    modulename, objname = dottedname.rsplit(".", maxsplit=1)
    return getattr(importlib.import_module(modulename), objname)


class Fixed(Generic[S, T]):
    """
    A thread-safe descriptor factory for creating and caching an instance of a class equivalent
    to a cached property.

    The ``Fixed`` descriptor provisions for each instance of the owner class
    to dynamically load or import the managed class.  The managed instance
    is created on first access and then cached for subsequent access.

    Usage:

        TODO

    Type Hints:
        ``S``: Type of the owner class.
        ``T``: Type of the managed class.
    """

    __slots__ = ["create", "created", "instances", "lock", "name"]

    def __init__(
        self,
        obj: type[T] | Callable[[FixedCreate[S]], T] | str,
        /,
        *,
        created: Callable[[FixedCreated[S, T]]] | None = None,
    ):
        if isinstance(obj, str):
            self.create = lambda _: import_item(obj)()
        elif inspect.isclass(obj):
            self.create = lambda _: obj()
        elif callable(obj):
            self.create = obj
        else:
            msg = f"{obj=} is invalid. Wrap it with a lambda to make it 'constant'. Eg. lambda _: {obj}"  # pyright: ignore[reportUnreachable]
            raise TypeError(msg)
        self.created = created
        self.instances = weakref.WeakKeyDictionary()
        self.lock = BinarySemaphore()

    def __set_name__(self, owner_cls: type[S], name: str):
        self.name = name

    def __get__(self, obj: S, objtype: type[S] | None = None) -> T:
        if obj is None:
            return self  # pyright: ignore[reportReturnType]
        try:
            return self.instances[obj]
        except KeyError:
            with self.lock:
                if obj in self.instances:
                    return self.instances[obj]
                instance: T = self.create(FixedCreate(name=self.name, owner=obj))  # pyright: ignore[reportAssignmentType]
                self.instances[obj] = instance
                if self.created:
                    try:
                        self.created(FixedCreated(owner=obj, obj=instance, name=self.name))
                    except Exception:
                        if log := getattr(obj, "log", None):
                            msg = f"Callback `created` failed for {obj.__class__}.{self.name}"
                            log.exception(msg, extra={"obj": self.created})
                return instance

    def __set__(self, obj: S, value: Self):
        # Note: above we use `Self` for the `value` type hint to give a useful typing error
        msg = f"Setting `Fixed` parameter {obj.__class__.__name__}.{self.name} is forbidden!"
        raise AttributeError(msg)
