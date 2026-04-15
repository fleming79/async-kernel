import gc
import inspect
import random
import re
import weakref
from concurrent.futures import ThreadPoolExecutor

import anyio
import pytest
from aiologic import Event, Latch
from aiologic.meta import await_for

from async_kernel.caller import Caller
from async_kernel.pending import (
    Pending,
    PendingCancelled,
    PendingGroup,
    PendingManager,
    PendingNotDone,
    PendingTracker,
)
from async_kernel.typing import Backend


@pytest.fixture(params=Backend, scope="module")
def anyio_backend(request):
    return request.param


class PendingManagerTest(PendingManager):
    "A pending manager for these tests"


@pytest.fixture
async def pm(anyio_backend: Backend):
    assert PendingManagerTest.current() is None
    pm = PendingManagerTest()
    assert PendingManagerTest.current() is None
    yield pm
    for pen in pm.pending:
        pen.cancel()
    if pm.pending:
        with anyio.CancelScope(shield=True):
            await Caller().wait(pm.pending)


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

    async def test_set_already_result(self, anyio_backend: Backend):
        pen = Pending()
        pen.set_result(1)
        pen.set_result(2)
        pen.set_exception(RuntimeError())
        assert pen.result() == 1

    async def test_set_already_exception(self, anyio_backend: Backend):
        pen = Pending()
        pen.set_exception(RuntimeError())
        pen.set_result(2)
        with pytest.raises(RuntimeError):
            pen.result()

    def test_not_done(self):
        pen = Pending()
        with pytest.raises(PendingNotDone):
            pen.result()
        with pytest.raises(PendingNotDone):
            pen.exception()

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

    def test_set_done_base_exception(self):
        event_after = Event()

        def raise_keyboard_interrupt(pen):
            raise KeyboardInterrupt

        pen = Pending()
        pen.add_done_callback(raise_keyboard_interrupt)
        pen.add_done_callback(lambda _: event_after.set())
        with pytest.raises(KeyboardInterrupt):
            pen._set_done(False, None)  # pyright: ignore[reportPrivateUsage]
        assert event_after

    async def test_wait_shield(self, caller: Caller):
        async def f():
            await anyio.sleep(0.001)
            return 2

        with anyio.move_on_after(0):
            result = await caller.call_soon(f).wait(shield=True)
        assert result == 2  # pyright: ignore[reportPossiblyUnboundVariable]

    async def test_cancel(self, anyio_backend: Backend):
        pen = Pending()
        pen.set_canceller(lambda _: None)
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

    async def test_cancel_wait_done(self, caller: Caller, anyio_backend: Backend):
        started = Event()

        async def f():
            started.set()
            await anyio.sleep_forever()

        pen = caller.call_soon(f)
        await started
        await pen.cancel_wait("test cancel")
        with pytest.raises(PendingCancelled, match="test cancel"):
            pen.result()

    async def test_set_done_stress_test(self):
        # Inspiration: https://github.com/python/cpython/issues/146270
        exc = RuntimeError()
        with ThreadPoolExecutor(3) as executor:
            for _ in range(1000):
                pen = Pending()

                f1 = executor.submit(pen.set_result, 1)
                f2 = executor.submit(pen.set_exception, exc)
                f3 = executor.submit(pen.cancel)

                f1.result()
                f2.result()
                f3.result()
                assert pen.done()

    def test_repr(self):
        a = "long string" * 100
        b = {f"name {i}": "long_string" * 100 for i in range(100)}
        pen = Pending()
        pen.set_canceller(lambda _: None)
        pen.metadata.update(a=a, b=b)
        matches = [
            f"<Pending {indicator} at {id(pen)} metadata:{{'a': 'long stringl…nglong string', 'b': {{…}}}} >"
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

    async def test_wait_sync_timeout(self, anyio_backend: Backend):
        async with Caller("manual") as caller:
            pen = caller.call_soon(lambda: 0 / 1)  # should never get called
            with pytest.raises(TimeoutError):
                pen.wait_sync(timeout=0.001)
            assert pen.cancelled()
            with pytest.raises(PendingCancelled):
                await pen

    async def test_many_waiters(self, caller: Caller):
        N = 100
        n_threads = 20
        barrier = Latch(N // 2 - 10)

        async def f_set():
            await barrier

        pen = caller.call_soon(f_set)

        async def f(i, wait_sync=False):
            if i % 2:
                await barrier
            if wait_sync:
                pen.wait_sync()
            await pen

        to_thread = random.sample(range(N), n_threads)
        wait_sync = True
        async with caller.create_pending_group():
            for i in range(N):
                if i in to_thread:
                    caller.to_thread(f, i, wait_sync=wait_sync)
                    wait_sync = not wait_sync
                else:
                    caller.call_soon(f, i)


class TestPendingManagerTest:
    async def test_context(self, pm: PendingManagerTest, caller: Caller) -> None:
        token = pm.activate()
        assert pm is PendingManagerTest.current()
        pm.deactivate(token)
        assert PendingManagerTest.current() is None

    async def test_pool_ignores_pending_errors(self, pm: PendingManagerTest, caller: Caller) -> None:
        token = pm.activate()
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
        pm.deactivate(token)

    async def test_state(self, pm: PendingManagerTest, caller: Caller):
        count = 0
        n = 2

        async def recursive():
            nonlocal count
            count += 1
            if count < n:
                pc = PendingManagerTest.current()
                assert pc
                return caller.call_soon(recursive)
            return count

        token = pm.activate()

        pm.add(pen := caller.call_soon(recursive))
        while isinstance(pen, Pending):
            pen = await pen
        assert pen == n
        assert count == n
        assert not pm.pending

        caller.call_soon(lambda: 1)
        assert pm.pending

        pen = Pending()
        pm.add(pen)
        assert pen in pm.pending
        pm.remove(pen)
        assert pen not in pm.pending
        pm.deactivate(token)
        pen.set_result(None)
        await caller.wait(pm.pending)
        assert not pm.pending

    async def test_subclass(self, pm: PendingManagerTest, caller: Caller):
        pm_token = pm.activate()
        assert PendingManagerTest.current() is pm

        class PendingManagerTestSubclass(PendingManagerTest):
            pass

        assert PendingManagerTestSubclass in PendingTracker._subclasses  # pyright: ignore[reportPrivateUsage]
        pm2 = PendingManagerTestSubclass()
        assert PendingManagerTestSubclass.current() is None
        pm2_token = pm2.activate()

        assert PendingManagerTestSubclass.current() is pm2
        assert PendingManagerTest.current() is pm

        pm.deactivate(pm_token)
        assert PendingManagerTest.current() is None

        with pytest.raises(ValueError, match="was created by a different ContextVar"):
            pm.deactivate(pm2_token)

        pm2.deactivate(pm2_token)
        assert PendingManagerTestSubclass.current() is None

    async def test_nested_isolated(self, pm: PendingManagerTest, caller: Caller):
        async def func() -> None:
            assert PendingManagerTest.current() is None, "A current PendingManagerTest should not exist in this context"

        token = pm.activate()
        await caller.schedule_call(func, (), {}, None, ())
        pm.deactivate(token)


class TestPendingGroup:
    async def test_basic(self, caller: Caller) -> None:
        assert PendingGroup.current() is None
        async with PendingGroup() as pm:
            assert pm is PendingGroup.current()
            pen1 = caller.call_soon(lambda: 1)
            assert pen1 in pm.pending
            assert not pm._all_done  # pyright: ignore[reportPrivateUsage]
        assert pen1 not in pm.pending
        assert pm._all_done  # pyright: ignore[reportPrivateUsage]

    async def test_async_reenter(self, caller: Caller) -> None:
        assert PendingGroup.current() is None
        async with PendingGroup() as pm:
            pen1 = caller.call_soon(lambda: 1)
            assert pen1 in pm.pending
        assert pen1.result() == 1
        with pytest.raises(RuntimeError):
            async with pm:
                pass

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

    async def test_external_cancellation(self, caller: Caller):
        with anyio.move_on_after(0.1):
            async with PendingGroup() as pm:
                pen = caller.call_soon(anyio.sleep_forever)
        assert pen.cancelled()  # pyright: ignore[reportPossiblyUnboundVariable]
        assert not pm.pending  # pyright: ignore[reportPossiblyUnboundVariable]

    async def test_cancellation(self, caller: Caller):
        with pytest.raises(PendingCancelled):  # noqa: PT012
            async with PendingGroup(mode=1) as pm:
                pen = caller.call_soon(anyio.sleep_forever)
                pm.cancel("Stop")
        assert pen.cancelled()  # pyright: ignore[reportPossiblyUnboundVariable]
        assert not pm.pending  # pyright: ignore[reportPossiblyUnboundVariable]

    async def test_invalid_state(self, caller: Caller):
        pm = PendingGroup()
        ok = False

        async def add_when_cancelled():
            assert PendingGroup.current() is pm
            nonlocal ok
            try:
                await anyio.sleep_forever()
            except anyio.get_cancelled_exc_class():
                ok = True
                raise

        with anyio.move_on_after(0.1):
            async with pm:
                caller.call_soon(add_when_cancelled)
        assert ok

    async def test_wait_exception(self, caller: Caller):
        with pytest.raises(ExceptionGroup):  # noqa: PT012
            async with PendingGroup():
                pen = caller.call_soon(anyio.sleep_forever)
                Pending(None, PendingGroup).set_exception(RuntimeError("stop"))
        assert pen.cancelled()  # pyright: ignore[reportPossiblyUnboundVariable]

    async def test_cancelled_by_pending(self, caller: Caller):
        with pytest.raises(ExceptionGroup):  # noqa: PT012
            async with PendingGroup() as pg:
                assert caller.call_soon(lambda: 1 / 0) in pg.pending
                await anyio.sleep_forever()
        assert pg.cancelled()  # pyright: ignore[reportPossiblyUnboundVariable]

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

    async def test_nested(self, caller: Caller):
        with anyio.move_on_after(0.1):
            async with caller.create_pending_group() as pg1, pg1.caller.create_pending_group() as pg2:
                pen = pg2.caller.call_soon(anyio.sleep_forever)
                assert pen in pg2.pending
                assert pen in pg1.pending
        assert pen.cancelled()  # pyright: ignore[reportPossiblyUnboundVariable]

    async def test_nested_raises(self, caller: Caller):
        with pytest.raises(ExceptionGroup):
            async with caller.create_pending_group() as pg1, pg1.caller.create_pending_group() as pg2:
                pen = pg2.caller.call_soon(lambda: 1 / 0)
        assert pen.exception()  # pyright: ignore[reportPossiblyUnboundVariable]

    async def test_tracking(self, caller: Caller):
        pm = PendingManagerTest()
        token = pm.activate()
        try:
            async with caller.create_pending_group() as pg:
                pen = Pending(None, PendingTracker)
                assert pen in pg.pending
                assert pen in pm.pending

                pen_no_track = Pending()
                assert pen_no_track not in pm.pending
                assert pen_no_track not in pg.pending

                pen_pg = Pending(None, PendingGroup)
                assert pen_pg in pg.pending
                assert pen_pg not in pm.pending

                pen_pm = Pending(None, PendingManagerTest)
                assert pen_pm in pm.pending
                assert pen_pm not in pg.pending
                pg._pending.clear()  # pyright: ignore[reportPrivateUsage]

        finally:
            pm._pending.clear()  # pyright: ignore[reportPrivateUsage]
            pm.deactivate(token)

    async def test_propagation(self, caller: Caller):
        async def func() -> None:
            assert PendingGroup.current() is None, "A current PendingGroup should not exist in this context"

        async with caller.create_pending_group():
            await caller.schedule_call(func, (), {}, None, ())

    async def test_mode(self, caller: Caller):

        # mode 0
        async with caller.create_pending_group(mode=0) as pg:
            pen = pg.caller.call_soon(anyio.sleep_forever)
            pen.cancel("stop now")
            await pg.caller.call_soon(lambda: 1 + 1)
            assert not pg.cancelled()
        # mode 1
        with pytest.raises(PendingCancelled):  # noqa: PT012
            async with caller.create_pending_group(mode=1) as pg:
                pen = pg.caller.call_soon(anyio.sleep_forever)
                pen.cancel("stop now")
                assert pg.cancelled()
        # mode 2
        async with caller.create_pending_group(mode=2) as pg:
            pen = pg.caller.call_soon(anyio.sleep_forever)
            pen.cancel("stop now")
            assert pg.cancelled()
        # mode 3
        async with caller.create_pending_group(mode=3) as pg:
            pen = pg.caller.call_soon(anyio.sleep_forever)
            pen.cancel("stop now")
            await pg.caller.call_soon(lambda: 1 + 1)
            assert not pg.cancelled()

    async def test_repr(self, caller: Caller):

        async with caller.create_pending_group() as pg:
            pg.caller.call_soon(anyio.sleep, 0)
            match = f"<PendingGroup at {id(pg)} | 1 pending | mode:0>"
            assert repr(pg) == match
