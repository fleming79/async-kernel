from __future__ import annotations

import gc
import os
import time
import weakref
from collections import deque as deque_ref
from typing import TYPE_CHECKING, Any

import anyio
import anyio.to_thread
import pytest
from aiologic import Event

from async_kernel.common import Fixed, LastUpdatedDict, import_item

if TYPE_CHECKING:
    from async_kernel.typing import Backend, FixedCreate, FixedCreated


class TestImportItem:
    def test_standard_function(self):
        join = import_item("os.path.join")
        assert join is os.path.join

    def test_standard_class(self):
        deque = import_item("collections.deque")
        assert deque is deque_ref

    def test_standard_module(self):
        # Should import the module itself
        path = import_item("os.path")
        import os.path as ospath  # noqa: PLC0415

        assert path is ospath

    def test_invalid_module(self):
        with pytest.raises(ModuleNotFoundError):
            import_item("nonexistent.module.Class")

    def test_invalid_object(self):
        with pytest.raises(ImportError):
            import_item("os.path.nonexistent_function")

    def test_builtin(self):
        abs_fn = import_item("builtins.abs")
        assert abs_fn is abs

    def test_typing(self):
        item = import_item("typing.TypeVar")
        from typing import TypeVar as TypeVarRef  # noqa: PLC0415

        assert item is TypeVarRef


@pytest.mark.anyio
class TestFixed:
    class Owner:
        def __init__(self):
            self.log = None  # For created callback error handling

    async def test_gc(self):
        collected = Event()

        class MyClass:
            fixed_dict = Fixed(dict)

        m = MyClass()
        assert isinstance(m.fixed_dict, dict)
        assert len(MyClass.fixed_dict.instances) == 1  # pyright: ignore[reportAttributeAccessIssue]
        weakref.finalize(m, collected.set)
        del m
        while not collected:
            gc.collect()
            await anyio.sleep(0)
        assert len(MyClass.fixed_dict.instances) == 0  # pyright: ignore[reportAttributeAccessIssue]

    def test_with_class(self):
        class MyClass:
            fixed_dict = Fixed(dict)

        a = MyClass()
        b = MyClass()
        assert isinstance(a.fixed_dict, dict)
        assert isinstance(b.fixed_dict, dict)
        assert a.fixed_dict is not b.fixed_dict
        # Should be cached
        assert a.fixed_dict is a.fixed_dict

    def test_with_lambda(self):
        class MyClass:
            fixed_val = Fixed(lambda c: id(c["owner"]))

        obj = MyClass()
        val1 = obj.fixed_val
        val2 = obj.fixed_val
        assert val1 == val2
        assert isinstance(val1, int)

    async def test_double_access(self, anyio_backend: Backend):
        def func(c):
            c["owner"].i += 1
            time.sleep(0.1)

        class MyClass:
            i = 0
            slow = Fixed(func)

        obj = MyClass()

        async with anyio.create_task_group() as tg:
            tg.start_soon(anyio.to_thread.run_sync, lambda: obj.slow)
            tg.start_soon(anyio.to_thread.run_sync, lambda: obj.slow)
        assert obj.i == 1

    def test_with_str_import(self):
        class MyClass:
            fixed_list = Fixed("builtins.list")

        obj = MyClass()
        assert isinstance(obj.fixed_list, list)

    def test_created_callback(self):
        called = {}

        def created_callback(c: FixedCreated[MyClass, dict]):
            called.update(c)
            called["during_get_okay"] = c["owner"].fixed is c["obj"]

        class MyClass:
            fixed = Fixed(dict, created=created_callback)

        obj = MyClass()
        val = obj.fixed
        assert called["owner"] is obj
        assert called["obj"] is val
        assert called["name"] == "fixed"
        assert called["during_get_okay"]

    def test_create_reenter(self):
        def reenter(c: FixedCreate[MyClass]):
            assert not c["owner"].fixed
            return True

        class MyClass:
            fixed = Fixed(reenter)

        obj = MyClass()
        with pytest.raises(RuntimeError, match="the current task is already holding this lock"):
            assert obj.fixed

    def test_set_forbidden(self):
        class MyClass:
            fixed: Fixed[Any, dict[str, object]] = Fixed(dict)

        obj = MyClass()
        with pytest.raises(AttributeError):
            obj.fixed = {}  # pyright: ignore[reportAttributeAccessIssue]

    def test_created_callback_exception_logs(self, mocker):
        log = mocker.Mock()

        def created(ctx):
            raise RuntimeError

        class MyClass:
            def __init__(self):
                self.log = log

            fixed = Fixed(dict, created=created)

        obj = MyClass()
        _ = obj.fixed
        log.exception.assert_called()

    def test_use_lambda(self):
        with pytest.raises(TypeError, match="is invalid! Use a lambda instead eg: lambda _:"):
            Fixed(1)  # pyright: ignore[reportArgumentType]

    def test_get_at_import(self):
        fixed = Fixed(str)
        assert fixed.__get__(None, None) is fixed


class TestLastUpdatedDict:
    def test_last_updated_dict(self):
        d = LastUpdatedDict()
        d["a"] = 1
        d["b"] = 2
        assert list(d.keys()) == ["a", "b"]
        d["a"] = 3
        assert list(d.keys()) == ["b", "a"]
        assert d == {"a": 3, "b": 2}

    def test_last_updated_dict_first(self):
        d = LastUpdatedDict(last=False)
        d["a"] = 1
        d["b"] = 2
        assert list(d.keys()) == ["b", "a"]
        d["a"] = 3
        assert list(d.keys()) == ["a", "b"]
        assert d == {"a": 3, "b": 2}
