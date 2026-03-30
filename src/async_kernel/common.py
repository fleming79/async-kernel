from __future__ import annotations

import weakref
from typing import TYPE_CHECKING, Any, Generic, Never, Self

import aiologic.meta
from aiologic.lowlevel import create_thread_oncelock

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
    module, name0 = dottedname.rsplit(".", maxsplit=1)
    return aiologic.meta.import_from(module, name0)


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

        created: An optional callback that is called with [FixedCreated][] whenever a
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

    __slots__ = ["create", "created", "instances", "instances_locks", "name"]

    def __init__(
        self,
        obj: type[T] | Callable[[FixedCreate[S]], T] | str,
        /,
        *,
        created: Callable[[FixedCreated[S, T]]] | None = None,
    ) -> None:
        if callable(obj) or isinstance(obj, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            self.create = obj
            self.created = created
            self.instances = {}
            self.instances_locks = {}
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
                self.instances_locks.pop(key, None)

    def create_instance(self, obj: S, key: int) -> T:
        if isinstance(create := self.create, str):
            self.create = create = import_item(create)
        try:
            instance = create()  # pyright: ignore[reportCallIssue, reportAssignmentType]
        except TypeError:
            instance: T = create(FixedCreate(name=self.name, owner=obj))  # pyright: ignore[reportAssignmentType, reportCallIssue]
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
