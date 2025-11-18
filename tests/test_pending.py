import gc
import inspect
import re
import weakref

import anyio
import pytest
from aiologic import Event

from async_kernel.caller import Caller
from async_kernel.kernelspec import Backend
from async_kernel.pending import InvalidStateError, Pending, PendingCancelled


@pytest.fixture(params=Backend, scope="module")
def anyio_backend(request):
    return request.param


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
        async with Caller("new"):
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
        with pytest.raises(InvalidStateError):
            pen.exception()
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
        assert pen.cancel()
        with pytest.raises(PendingCancelled):
            pen.exception()

    async def test_set_from_non_thread(self, caller: Caller, anyio_backend: Backend):
        pen = Pending()
        caller.to_thread(pen.set_result, value=123)
        assert (await pen) == 123

    async def test_wait_cancelled_shield(self, caller: Caller, anyio_backend: Backend):
        pen = Pending()
        with pytest.raises(TimeoutError):
            await pen.wait(timeout=0.001, shield=True)
        assert not pen.cancelled()
        with pytest.raises(TimeoutError):
            await pen.wait(timeout=0.001)
        assert pen.cancelled()

    def test_repr(self):
        a = "long string" * 100
        b = {f"name {i}": "long_string" * 100 for i in range(100)}
        pen = Pending()
        pen.metadata.update(a=a, b=b)
        matches = [
            f"<Pending {indicator} at {id(pen)} {{'a': 'long stringl…nglong string', 'b': {{…}}}} >"
            for indicator in ("🏃", "⛔ 🏃", "⛔ 🏁")
        ]
        assert re.match(matches[0], repr(pen))
        pen.cancel()
        assert re.match(matches[1], repr(pen))
        pen.set_result(None)
        assert re.match(matches[2], repr(pen))

    async def test_gc(self, caller: Caller, anyio_backend: Backend):
        finalized = Event()
        ok = False

        def isolated():
            class Cls:
                def func(self):
                    assert Caller.current_pending()
                    nonlocal ok
                    ok = True

            t = Cls()
            weakref.finalize(t, finalized.set)
            pen = caller.call_soon(t.func)
            id_ = id(pen)
            assert hash(pen.metadata["func"]) == hash(t.func)
            r = weakref.ref(pen)
            del pen
            del t
            return r, id_

        r, id_ = isolated()
        assert id_ in Pending._metadata_mappings  # pyright: ignore[reportPrivateUsage]
        with anyio.move_on_after(1):
            await finalized
        assert r() is None, f"References found {gc.get_referrers(r())}"
        assert ok
        assert id_ not in Pending._metadata_mappings  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.parametrize("result", [True, False])
    async def test_wait_sync(self, caller: Caller, result: bool, anyio_backend: Backend):
        pen = caller.to_thread(lambda: 1 + 1)
        assert pen.wait_sync(result=result) == (2 if result else None)

    async def test_wait_sync_timeout(self, caller: Caller, anyio_backend: Backend):
        pen = caller.call_soon(anyio.sleep_forever)
        with pytest.raises(TimeoutError):
            pen.wait_sync(timeout=0.01)
