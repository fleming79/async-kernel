from __future__ import annotations

import importlib
import inspect
import weakref
from typing import TYPE_CHECKING, Any, Generic, Never, Self

from aiologic import Lock

from async_kernel.typing import FixedCreate, FixedCreated, S, T

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["Fixed", "import_item"]


def import_item(dottedname: str) -> Any:
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
    A thread-safe descriptor factory for creating and caching an object.

    The ``Fixed`` descriptor provisions for each instance of the owner class
    to dynamically load or import the managed class.  The managed instance
    is created on first access and then cached for subsequent access.

    Type Hints:
        - ``S``: Type of the owner class.
        - ``T``: Type of the managed class.

    Example:
        ```python
        class MyClass:
            a: Fixed[Self, dict] = Fixed(dict)
            b: Fixed[Self, int] = Fixed(lambda c: id(c["owner"].a))
            c: Fixed[Any, list[str]] = Fixed(list, created=lambda c: c["obj"].append(c["name"]))
        ```
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
        self.instances = weakref.WeakKeyDictionary()
        self.lock = Lock()

    def __set_name__(self, owner_cls: type[S], name: str) -> None:
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
                instance: T = self.create({"name": self.name, "owner": obj})  # pyright: ignore[reportAssignmentType]
                self.instances[obj] = instance
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
