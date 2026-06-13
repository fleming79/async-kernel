from __future__ import annotations

import gc
import weakref
from typing import TYPE_CHECKING

import anyio
import pytest
from aiologic import Event
from traitlets.config.configurable import Configurable

import async_kernel
from async_kernel.interface import BaseInterface, HasInterface
from async_kernel.shell import BaseShell

if TYPE_CHECKING:
    from async_kernel.typing import Backend

# pyright: reportPrivateUsage=false


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
            ref = weakref.ref(interface)
            del interface

        with anyio.move_on_after(2):
            while not collected:
                gc.collect()
                await anyio.sleep(0)
        if obj := ref():
            referrers = gc.get_referrers(obj)
            assert not referrers

    async def test_already_initialized(self, anyio_backend: Backend):
        for _ in range(3):
            async with BaseInterface() as interface:
                with pytest.raises(RuntimeError, match="Already initialized!"):
                    interface.initialize()

    async def test_base_shell(self, anyio_backend: Backend):

        async with BaseInterface(shell_class=BaseShell) as interface:
            assert isinstance(interface.kernel.shell, BaseShell)
            assert "name:" in interface.kernel.shell.banner
            assert isinstance(interface.kernel.shell.user_ns, dict)
            with pytest.raises(NotImplementedError):
                await interface.kernel.do_execute("1+1", True)

            # test subshell
            subshell = interface.kernel.create_subshell(protected=True)
            assert subshell.protected
            assert subshell.subshell_id in interface.kernel.subshells
            subshell.stop()
            assert subshell.subshell_id in interface.kernel.subshells
            subshell.stop(force=True)
            assert subshell.subshell_id not in interface.kernel.subshells
            assert isinstance(subshell.get_ipython(), BaseShell)

    async def test_base_shell_displayhook(self, anyio_backend: Backend, mocker):

        async with BaseInterface(shell_class=BaseShell) as interface:
            iopub_send = mocker.patch.object(interface, "iopub_send")
            with async_kernel.utils.show_result(True):
                interface.kernel.shell.displayhook(123)
            assert iopub_send.called
            expected = "call('execute_result', content={'execution_count': 0, 'data': '123', 'metadata': {}})"
            assert str(iopub_send.call_args) == expected

    async def test_stop_early(self, anyio_backend: Backend):
        app = BaseInterface(shell_class=BaseShell)
        app.stop()
        with pytest.raises(RuntimeError, match="This interface is not the global instance!"):
            app.start()
        with pytest.raises(RuntimeError, match="An instance does not exist"):
            BaseInterface.instance()
        with pytest.raises(RuntimeError, match="Stopped early"):
            async with app:
                pass

    def test_start_bad_settings(self):

        app = BaseInterface(shell_class=BaseShell, backend_options={"loop_factory": "not a factory"})
        assert BaseInterface._instance is app
        with pytest.raises(TypeError, match="object is not callable"):
            app.start()
        assert BaseInterface._instance is None


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
