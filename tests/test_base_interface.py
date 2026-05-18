from __future__ import annotations

import gc
import weakref
from typing import TYPE_CHECKING

import anyio
import pytest
from aiologic import Event
from traitlets.config.configurable import Configurable

from async_kernel.interface import BaseInterface, HasInterface

if TYPE_CHECKING:
    from async_kernel.typing import Backend


class TestBaseInterface:
    async def test_instance_does_not_exist(self, anyio_backend: Backend):

        with pytest.raises(RuntimeError, match="An instance does not exist"):
            BaseInterface.instance()

    async def test_instance_not_a_subclass(self, anyio_backend: Backend):

        class BaseInterfaceSub(BaseInterface):
            pass

        async with BaseInterface():
            with pytest.raises(TypeError, match="An instance exists but it is not an instance of"):
                BaseInterfaceSub.instance()

    async def test_gc(self, anyio_backend: Backend):
        collected = Event()
        async with BaseInterface() as interface:
            weakref.finalize(interface, collected.set)
            del interface

        while not collected:
            gc.collect()
            await anyio.sleep(0)


class TestHasInterface:
    def test_no_global_interface(self):
        with pytest.raises(RuntimeError):
            HasInterface()

    def test_invalidMRO(self):

        with pytest.raises(TypeError, match="Tip: Make `HasInterface` the first inherited class"):

            class InvalidMRO(Configurable, HasInterface):  # pyright: ignore[reportUnusedClass]
                pass

        with pytest.raises(TypeError, match="parameter named 'config' must not be overloaded"):

            class OverwritesProperty(HasInterface):  # pyright: ignore[reportUnusedClass]
                config = {}  # pyright: ignore[reportIncompatibleMethodOverride, reportAssignmentType]  # noqa: RUF012
