from __future__ import annotations

import gc
import time
import weakref
from typing import TYPE_CHECKING

import anyio
import anyio.lowlevel
import pytest
from aiologic import Event
from aiologic.lowlevel import async_checkpoint
from traitlets.config.configurable import Configurable

import async_kernel
from async_kernel.comm import Comm
from async_kernel.interface import HasInterface, Interface
from async_kernel.messaging import LocalClient
from async_kernel.shell.base import BaseShell
from async_kernel.typing import Channel, Job, MsgType
from tests import utils

if TYPE_CHECKING:
    from async_kernel.typing import Backend

# pyright: reportPrivateUsage=false


class TestInterface:
    async def test_instance_does_not_exist(self, anyio_backend: Backend):

        with pytest.raises(RuntimeError, match="An instance does not exist"):
            Interface.instance()

    async def test_instance_not_a_subclass(self, anyio_backend: Backend):

        class InterfaceSub(Interface):
            pass

        async with Interface().start():
            with pytest.raises(TypeError, match="An instance exists but it is not an instance of"):
                InterfaceSub.instance()

    async def test_gc(self, anyio_backend: Backend):
        collected = Event()
        async with Interface().start() as interface:
            weakref.finalize(interface, collected.set)
            ref = weakref.ref(interface)
            del interface

        with anyio.move_on_after(2):
            while not collected:
                gc.collect()
                await anyio.lowlevel.checkpoint()
        if obj := ref():
            referrers = gc.get_referrers(obj)
            assert not referrers

    async def test_already_initialized(self, anyio_backend: Backend):
        for _ in range(3):
            async with Interface().start() as interface:
                with pytest.raises(RuntimeError, match="Already initialized!"):
                    interface.initialize()

    async def test_already_exists(self, anyio_backend: Backend):
        interface = Interface()
        try:
            with pytest.raises(RuntimeError, match="An interface already exists!"):
                Interface()
        finally:
            await interface.stop().wait(result=False)

    async def test_early_comm(self, anyio_backend: Backend):
        interface = Interface()
        try:
            comm = Comm()
            comm.open()
            assert comm.comm_id in interface.kernel.comm_manager.comms
            async with interface.start(), LocalClient().start() as client:
                msg = client.msg(MsgType.comm_close, {"comm_id": comm.comm_id}, Channel.shell)
                client.send_message_no_reply(msg)
                with anyio.fail_after(utils.TIMEOUT):
                    while comm.comm_id in interface.kernel.comm_manager.comms:
                        await async_checkpoint(force=True)
        finally:
            await interface.stop()

    async def test_input_request_no_handler(self, anyio_backend: Backend):

        async with Interface(shell_class=BaseShell).start(), LocalClient().start() as client:
            msg = client.msg(MsgType.input_request, None, Channel.stdin)
            job = Job(msg=msg, owner=client.as_owner, ident=[], received_time=time.monotonic())
            with pytest.raises(RuntimeError, match="A handler is not available"):
                await client.input_request(job)

    async def test_stop(self, anyio_backend: Backend) -> None:

        async with Interface(shell_class=BaseShell).start() as interface:
            interface.stop()

    async def test_base_shell(self, anyio_backend: Backend):

        async with Interface(shell_class=BaseShell).start() as interface:
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

        async with Interface(shell_class=BaseShell).start() as interface:
            iopub_send = mocker.patch.object(interface, "iopub_send")
            with async_kernel.utils.show_result(True):
                interface.kernel.shell.displayhook(123)
            assert iopub_send.called
            expected = "{'content': {'execution_count': 0, 'data': '123', 'metadata': {}}}"
            assert str(list(iopub_send.call_args)[1]) == expected

    async def test_stop_early(self, anyio_backend: Backend):
        app = Interface(shell_class=BaseShell)
        app.stop()
        with pytest.raises(RuntimeError, match="This interface is not the global instance!"):
            app.start()
        with pytest.raises(RuntimeError, match="An instance does not exist"):
            Interface.instance()
        with pytest.raises(RuntimeError, match="This interface is not the global instance!"):
            async with app.start():
                pass

    def test_start_bad_settings(self):
        def bad_loop_factory():
            raise RuntimeError

        app = Interface(shell_class=BaseShell, backend_options={"loop_factory": bad_loop_factory})
        assert Interface._instance is app
        with pytest.raises(RuntimeError):
            app.start()
        assert Interface._instance is None


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
