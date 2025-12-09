from __future__ import annotations

import inspect
import weakref
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Generic, Never, Self

import aiologic.meta
from aiologic import Lock
from typing_extensions import override

from async_kernel.typing import FixedCreate, FixedCreated, S, T

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

__all__ = ["Fixed", "LastUpdatedDict", "import_item"]


def import_item(dottedname: str) -> Any:
    """Import an item from a module, given its dotted name.

    Example:
        ```python
        import_item("os.path.join")
        ```
    """
    module, name0 = dottedname.rsplit(".", maxsplit=1)
    return aiologic.meta.import_from(module, name0)


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
        self.instances = {}
        self.lock = Lock()

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


class LastUpdatedDict(OrderedDict):
    """
    A dictionary that moves the key to the beginning or end when the value is set.

    Args:
        *args: As per [dict][].
        last: Move the key to end if `True` or beginning if `False`.
        **kwargs: As per [dict][].

    [ref](https://docs.python.org/3/library/collections.html#ordereddict-examples-and-recipes)
    """

    _updating = False
    _last = True

    def __init__(self, *args: Mapping | Iterable, last: bool = True, **kwargs: Mapping) -> None:
        self._last = last
        super().__init__(*args, **kwargs)

    @override
    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, value)
        if not self._updating:
            self.move_to_end(key, self._last)

    @override
    def update(self, m, /, **kwargs) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        self._updating = True
        try:
            super().update(m, **kwargs)
        finally:
            self._updating = False
