from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from traitlets.config.configurable import Configurable

from async_kernel.interface import BaseKernelInterface, HasParentInterface

if TYPE_CHECKING:
    from async_kernel.typing import Backend


class TestBaseKernelInterface:
    async def test_instance_does_not_exist(self, anyio_backend: Backend):

        with pytest.raises(RuntimeError, match="An instance does not exist"):
            BaseKernelInterface.instance()

    async def test_instance_not_a_subclass(self, anyio_backend: Backend):

        class BaseKernelInterfaceSub(BaseKernelInterface):
            pass

        async with BaseKernelInterface():
            with pytest.raises(TypeError, match="An instance exists but it is not an instance of"):
                BaseKernelInterfaceSub.instance()


class TestHasParentInterface:
    def test_no_global_interface(self):
        with pytest.raises(RuntimeError):
            HasParentInterface()

    def test_invalidMRO(self):

        with pytest.raises(TypeError, match="Tip: Make `HasParentInterface` the first inherited class"):

            class InvalidMRO(Configurable, HasParentInterface):  # pyright: ignore[reportUnusedClass]
                pass

        with pytest.raises(TypeError, match="parameter named 'config' must not be overloaded"):

            class OverwritesProperty(HasParentInterface):  # pyright: ignore[reportUnusedClass]
                config = {}  # pyright: ignore[reportIncompatibleMethodOverride, reportAssignmentType]  # noqa: RUF012
