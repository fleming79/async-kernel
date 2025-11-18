from __future__ import annotations

import os
from collections import deque as deque_ref
from typing import TYPE_CHECKING, Any

import pytest

from async_kernel.common import Fixed, import_item

if TYPE_CHECKING:
    from async_kernel.typing import FixedCreate, FixedCreated


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
        with pytest.raises(AttributeError):
            import_item("os.path.nonexistent_function")

    def test_builtin(self):
        abs_fn = import_item("builtins.abs")
        assert abs_fn is abs

    def test_typing(self):
        item = import_item("typing.TypeVar")
        from typing import TypeVar as TypeVarRef  # noqa: PLC0415

        assert item is TypeVarRef


class TestFixed:
    class Owner:
        def __init__(self):
            self.log = None  # For created callback error handling

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
