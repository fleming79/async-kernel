import gc
import inspect
import re
import weakref

import anyio
import pytest
from aiologic import Event
from aiologic.meta import await_for

from async_kernel.caller import Caller
from async_kernel.pending import InvalidStateError, Pending, PendingCancelled, PendingGroup, PendingManager
from async_kernel.typing import Backend, PendingTrackerState


@pytest.fixture(params=Backend, scope="module")
def anyio_backend(request):
    return request.param


@pytest.fixture
async def pm(anyio_backend: Backend):
    assert PendingManager.current() is None
    pm = PendingManager()
    pm.activate()
    assert PendingManager.current() is None
    yield pm
    if pm.state is PendingTrackerState.active and (countdown := pm.deactivate()):
        await countdown


@pytest.mark.anyio
class TestPending:
    def test_weakref(self):
        f = Pending()
        assert weakref.ref(f)() is f

    def test_is_slots(self):
        f = Pending()
        with pytest.raises(AttributeError):
            f.not_an_att = None  # pyright: ignore[reportAttributeAccessIssue]

    async def test_set_and_wait_result(self, anyio_backend: Backend):
        pen = Pending[int]()
        assert inspect.isawaitable(pen)
        done_called = False
        after_done = Event()

        def callback(obj):
            nonlocal done_called
            assert obj is pen
            if done_called:
                after_done.set()
            done_called = True

        pen.add_done_callback(callback)
        pen.set_result(42)
        result = await pen
        assert result == 42
        assert done_called
        async with Caller("manual"):
            pen.add_done_callback(callback)
            await after_done

    async def test_set_result_reset(self, caller: Caller):
        pen_resetting = Pending()
        for _ in range(10):
            proceed = Event()
            pen2 = caller.call_soon(lambda: (proceed.set(), pen_resetting.wait())[1])  # noqa: B023
            await proceed
            caller.call_soon(pen_resetting.set_result, 1, reset=True)
            assert await pen2 == 1
            assert not pen_resetting.done()
        pen_resetting.set_exception(RuntimeError("should not continue"))
        with pytest.raises(InvalidStateError):
            pen_resetting.set_result(2)

    async def test_set_and_wait_exception(self, anyio_backend: Backend):
        pen = Pending()
        done_called = False

        def callback(obj):
            nonlocal done_called
            assert obj is pen
            done_called = True

        pen.add_done_callback(callback)
        assert pen.remove_done_callback(callback) == 1
        pen.add_done_callback(callback)
        assert not pen.done()
        exc = ValueError("fail")
        pen.set_exception(exc)
        with pytest.raises(ValueError, match="fail") as e:
            await pen
        assert e.value is exc
        assert pen.done()
        assert done_called

    async def test_set_result_twice_raises(self, anyio_backend: Backend):
        pen = Pending()
        pen.set_result(1)
        with pytest.raises(RuntimeError):
            pen.set_result(2)

    async def test_set_canceller_twice_raises(self, anyio_backend: Backend):
        pen = Pending()
        with anyio.CancelScope() as cancel_scope:
            pen.set_canceller(cancel_scope.cancel)
            with pytest.raises(InvalidStateError):
                pen.set_canceller(cancel_scope.cancel)

    async def test_set_canceller_after_cancelled(self, anyio_backend: Backend):
        pen = Pending()
        pen.cancel()
        with anyio.CancelScope() as cancel_scope:
            pen.set_canceller(cancel_scope.cancel)
            assert cancel_scope.cancel_called

    async def test_set_exception_twice_raises(self, anyio_backend: Backend):
        pen = Pending()
        pen.set_exception(ValueError())
        with pytest.raises(InvalidStateError):
            pen.set_exception(ValueError())

    async def test_set_result_after_exception_raises(self, anyio_backend: Backend):
        pen = Pending()
        pen.set_exception(ValueError())
        assert isinstance(pen.exception(), ValueError)
        with pytest.raises(RuntimeError):
            pen.set_result(1)

    async def test_set_exception_after_result_raises(self, anyio_backend: Backend):
        pen = Pending()
        pen.set_result(1)
        with pytest.raises(RuntimeError):
            pen.set_exception(ValueError())

    def test_result(self):
        pen = Pending()
        with pytest.raises(InvalidStateError):
            pen.result()
        pen.set_result(1)
        assert pen.result() == 1

    def test_result_cancelled(self):
        pen = Pending()
        assert pen.cancel()
        with pytest.raises(PendingCancelled):
            pen.result()

    def test_result_exception(self):
        pen = Pending()
        pen.set_exception(TypeError("my exception"))
        with pytest.raises(TypeError, match="my exception"):
            pen.result()

    async def test_cancel(self, anyio_backend: Backend):
        pen = Pending()
        assert pen.cancel("message 1")
        assert pen.cancel("message 2")
        with pytest.raises(PendingCancelled, match="message 1\nmessage 2"):
            pen.exception()

    async def test_set_from_non_thread(self, caller: Caller, anyio_backend: Backend):
        pen = Pending()
        caller.to_thread(pen.set_result, value=123)
        assert (await pen) == 123

    async def test_wait_cancelled_protect(self, caller: Caller, anyio_backend: Backend):
        pen = Pending()
        with pytest.raises(TimeoutError):
            await pen.wait(timeout=0.001, protect=True)
        assert not pen.cancelled()
        with pytest.raises(TimeoutError):
            await pen.wait(timeout=0.001)
        assert pen.cancelled()

    def test_repr(self):
        a = "long string" * 100
        b = {f"name {i}": "long_string" * 100 for i in range(100)}
        pen = Pending({"allow_tracking": False})
        pen.metadata.update(a=a, b=b)
        matches = [
            f"<Pending {indicator} at {id(pen)} options:{{'allow_tracking': False}} metadata:{{'a': 'long stringl…nglong string', 'b': {{…}}}} >"
            for indicator in ("🏃", "⛔ 🏃", "⛔ 🏁")
        ]
        assert re.match(matches[0], repr(pen))
        pen.cancel()
        assert re.match(matches[1], repr(pen))
        pen.set_result(None)
        assert re.match(matches[2], repr(pen))

    async def test_gc(self, caller: Caller, anyio_backend: Backend):
        collected = Event()
        ok = False

        def isolated():
            class Cls:
                def func(self):
                    assert Caller.current_pending()
                    nonlocal ok
                    ok = True

            t = Cls()
            weakref.finalize(t, collected.set)
            pen = caller.call_soon(t.func)
            id_ = id(pen)
            assert hash(pen.metadata["func"]) == hash(t.func)
            r = weakref.ref(pen)
            del pen
            del t
            return r, id_

        r, id_ = isolated()
        assert id_ in Pending._metadata_mappings  # pyright: ignore[reportPrivateUsage]
        while not collected:
            gc.collect()
            await anyio.sleep(0)
        assert r() is None, f"References found {gc.get_referrers(r())}"
        assert ok
        assert id_ not in Pending._metadata_mappings  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.parametrize("result", [True, False])
    async def test_wait_sync(self, caller: Caller, result: bool, anyio_backend: Backend):
        pen = caller.to_thread(lambda: 1 + 1)
        assert pen.wait_sync(result=result) == (2 if result else None)
        assert pen.wait_sync(result=result) == (2 if result else None)

    async def test_wait_sync_timeout(self, caller: Caller, anyio_backend: Backend):
        pen = caller.call_soon(anyio.sleep_forever)
        with pytest.raises(TimeoutError):
            pen.wait_sync(timeout=0.01)


class TestPendingManager:
    async def test_context(self, pm: PendingManager, caller: Caller) -> None:
        token = pm.start_tracking()
        assert pm is PendingManager.current()
        pm.stop_tracking(token)
        assert PendingManager.current() is None

    async def test_pool_deactivate(self, pm: PendingManager, caller: Caller) -> None:
        token = pm.start_tracking()
        pen = caller.call_soon(anyio.sleep_forever)
        for _ in range(5):
            caller.call_soon(anyio.sleep_forever)
        assert pen in pm.pending
        pm.stop_tracking(token)
        pending = pm.pending
        assert pm.deactivate(cancel_pending=False) is None
        assert pm.state is PendingTrackerState.stopped
        token = pm.activate()
        for pen in pending:
            assert not pen.done()
            pm.track(pen)
        done_event = pm.deactivate(cancel_pending=True)
        assert done_event is not None
        assert done_event.value == 6
        assert pm.state is PendingTrackerState.exiting
        await done_event
        assert pm.state is PendingTrackerState.stopped
        assert pen.done()
        assert not pm.pending

    async def test_pool_ignores_pending_errors(self, pm: PendingManager, caller: Caller) -> None:
        token = pm.start_tracking()
        proceed = Event()
        pen1 = caller.call_soon(await_for, proceed)  # pyright: ignore[reportArgumentType]
        pen2 = caller.call_soon(lambda: 1 / 0)
        await pen2.wait(result=False)
        proceed.set()
        await pen1
        pen2 = caller.call_soon(anyio.sleep_forever)
        assert pm.pending == {pen2}
        with pytest.raises(TimeoutError), anyio.fail_after(0.01):
            await pen2
        await pen2.wait(result=False)
        assert not pm.pending
        assert pm.state is PendingTrackerState.active
        pm.stop_tracking(token)

    async def test_state(self, pm: PendingManager, caller: Caller):
        count = 0
        n = 2

        async def recursive():
            nonlocal count
            count += 1
            if count < n:
                pc = PendingManager.current()
                assert pc
                return caller.call_soon(recursive)
            return count

        with pytest.raises(InvalidStateError):
            pm.activate()

        pm.start_tracking()
        pm.track(pen := caller.call_soon(recursive))
        while isinstance(pen, Pending):
            pen = await pen
        assert pen == n
        assert count == n
        assert not pm.pending

        caller.call_soon(lambda: 1)
        count_event = pm.deactivate(cancel_pending=True)
        count_event = pm.deactivate(cancel_pending=True)
        assert count_event is not None
        assert count_event.value == 1

        with pytest.raises(InvalidStateError):
            pm.track(Pending())

        await count_event

        assert pm.deactivate() is None

        with pytest.raises(InvalidStateError):
            pm.start_tracking()

    async def test_discard(self, pm: PendingManager, caller: Caller):
        pm.track(pen1 := caller.call_soon(lambda: 1 + 1))
        pm.track(pen2 := Pending())
        assert await pen1 == 2
        pm.discard(pen2)

    async def test_subclass(self, pm: PendingManager, caller: Caller):
        pm_token = pm.start_tracking()
        assert PendingManager.current() is pm

        class PendingManagerSubclass(PendingManager):
            pass

        pm2 = PendingManagerSubclass().activate()
        assert PendingManagerSubclass.current() is None
        pm2_token = pm2.start_tracking()

        assert PendingManagerSubclass.current() is pm2
        assert PendingManager.current() is pm

        pm.stop_tracking(pm_token)
        assert PendingManager.current() is None

        with pytest.raises(ValueError, match="was created by a different ContextVar"):
            pm.stop_tracking(pm2_token)

        pm2.stop_tracking(pm2_token)
        assert PendingManagerSubclass.current() is None


class TestPendingGroup:
    async def test_pending_manager_async(self, caller: Caller) -> None:
        assert PendingGroup.current() is None
        async with PendingGroup() as pm:
            assert pm is PendingGroup.current()
            pen1 = caller.call_soon(lambda: 1)
            assert pen1 in pm.pending
            assert pm._count_event.value == 1  # pyright: ignore[reportPrivateUsage]
        assert pen1 not in pm.pending
        assert pm._count_event.value == 0  # pyright: ignore[reportPrivateUsage]
        assert pm.state is PendingTrackerState.stopped

    async def test_async_reenter(self, caller: Caller) -> None:
        assert PendingGroup.current() is None
        async with PendingGroup() as pm:
            pen1 = caller.call_soon(lambda: 1)
            assert pen1 in pm.pending
        assert pen1.result() == 1
        async with pm:
            pen2 = caller.call_soon(lambda: 2)
            with pytest.raises(RuntimeError, match="this PendingGroup has already been entered"):
                async with pm:
                    pass
        assert pen2.result() == 2
        assert not pm.pending
        async with pm:
            pen3 = caller.call_soon(lambda: 3)
            assert pen3 in pm.pending
            assert await pen3 == 3
            assert not pm.pending

    async def test_state(self, caller: Caller):
        count = 0
        n = 2

        async def recursive():
            nonlocal count
            count += 1
            if count < n:
                return caller.call_soon(recursive)
            return count

        async with PendingGroup():
            pen = caller.call_soon(recursive)
        assert count == n
        while isinstance(pen, Pending):
            pen = pen.result()
        assert pen == n

    async def test_cancellation(self, caller: Caller):
        with anyio.move_on_after(0.1):
            async with PendingGroup() as pm:
                pen = caller.call_soon(anyio.sleep_forever)
        assert pen.cancelled()  # pyright: ignore[reportPossiblyUnboundVariable]
        assert not pm.pending  # pyright: ignore[reportPossiblyUnboundVariable]

    async def test_invalid_state(self, caller: Caller):
        pm = PendingGroup()
        ok = False

        async def add_when_cancelled():
            nonlocal ok
            try:
                await anyio.sleep_forever()
            except anyio.get_cancelled_exc_class():
                with pytest.raises(InvalidStateError):
                    caller.call_soon(anyio.sleep_forever)
                ok = True
                raise

        with anyio.move_on_after(0.1):
            async with pm:
                caller.call_soon(add_when_cancelled)
        assert ok
        assert pm.state is PendingTrackerState.stopped

    async def test_wait_exception(self, caller: Caller):
        with pytest.raises(PendingCancelled):  # noqa: PT012
            async with PendingGroup():
                pen = caller.call_soon(anyio.sleep_forever)
                Pending().set_exception(RuntimeError("stop"))
        assert pen.cancelled()  # pyright: ignore[reportPossiblyUnboundVariable]

    async def test_cancelled_by_pending(self, caller: Caller):
        with pytest.raises(PendingCancelled):  # noqa: PT012
            async with PendingGroup() as pg:
                assert caller.call_soon(lambda: 1 / 0) in pg.pending
                await anyio.sleep_forever()
        assert pg.cancelled()  # pyright: ignore[reportPossiblyUnboundVariable]

    async def test_discard(self, caller: Caller):
        async with PendingGroup() as pg:
            pen = Pending()
            pg.discard(pen)
            assert pen not in pg.pending

    async def test_subclass(self, caller: Caller):
        class PendingManagerSubclass(PendingGroup):
            pass

        async with PendingManagerSubclass() as pcsub:
            assert PendingManagerSubclass.current() is pcsub
            async with PendingGroup() as pm:
                assert PendingGroup.current() is pm
                assert PendingManagerSubclass.current() is pcsub

    async def test_shield(self, caller: Caller):
        ok = False
        async with caller.create_pending_group():
            with anyio.move_on_after(0.1):
                try:
                    await anyio.sleep_forever()
                except anyio.get_cancelled_exc_class():
                    async with caller.create_pending_group(shield=True) as pg:
                        assert await caller.call_soon(lambda: 1) == 1
                        ok = True
                    assert not pg.cancelled()
        assert ok
