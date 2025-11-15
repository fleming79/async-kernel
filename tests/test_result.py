import gc
import importlib.util
import inspect
import re
import weakref

import anyio
import pytest
from aiologic import Event

from async_kernel.caller import Caller
from async_kernel.kernelspec import Backend
from async_kernel.result import InvalidStateError, Result, ResultCancelledError


@pytest.fixture(params=list(Backend) if importlib.util.find_spec("trio") else [Backend.asyncio])
def anyio_backend(request):
    return request.param


@pytest.fixture
async def caller(anyio_backend: Backend):
    async with Caller(create=True) as caller:
        yield caller
    Caller.stop_all()


@pytest.mark.anyio
class TestResult:
    def test_weakref(self):
        f = Result()
        assert weakref.ref(f)() is f

    def test_is_slots(self):
        f = Result()
        with pytest.raises(AttributeError):
            f.not_an_att = None  # pyright: ignore[reportAttributeAccessIssue]

    async def test_set_and_wait_result(self):
        fut = Result[int]()
        assert inspect.isawaitable(fut)
        done_called = False
        after_done = Event()

        def callback(obj):
            nonlocal done_called
            assert obj is fut
            if done_called:
                after_done.set()
            done_called = True

        fut.add_done_callback(callback)
        fut.set_result(42)
        result = await fut
        assert result == 42
        assert done_called
        async with Caller(create=True):
            fut.add_done_callback(callback)
            await after_done

    async def test_set_and_wait_exception(self):
        fut = Result()
        done_called = False

        def callback(obj):
            nonlocal done_called
            assert obj is fut
            done_called = True

        fut.add_done_callback(callback)
        assert fut.remove_done_callback(callback) == 1
        fut.add_done_callback(callback)
        assert not fut.done()
        exc = ValueError("fail")
        fut.set_exception(exc)
        with pytest.raises(ValueError, match="fail") as e:
            await fut
        assert e.value is exc
        assert fut.done()
        assert done_called

    async def test_set_result_twice_raises(self):
        fut = Result()
        fut.set_result(1)
        with pytest.raises(RuntimeError):
            fut.set_result(2)

    async def test_set_canceller_twice_raises(self):
        fut = Result()
        with anyio.CancelScope() as cancel_scope:
            fut.set_canceller(cancel_scope.cancel)
            with pytest.raises(InvalidStateError):
                fut.set_canceller(cancel_scope.cancel)

    async def test_set_canceller_after_cancelled(self):
        fut = Result()
        fut.cancel()
        with anyio.CancelScope() as cancel_scope:
            fut.set_canceller(cancel_scope.cancel)
            assert cancel_scope.cancel_called

    async def test_set_exception_twice_raises(self):
        fut = Result()
        fut.set_exception(ValueError())
        with pytest.raises(InvalidStateError):
            fut.set_exception(ValueError())

    async def test_set_result_after_exception_raises(self):
        fut = Result()
        with pytest.raises(InvalidStateError):
            fut.exception()
        fut.set_exception(ValueError())
        assert isinstance(fut.exception(), ValueError)
        with pytest.raises(RuntimeError):
            fut.set_result(1)

    async def test_set_exception_after_result_raises(self):
        fut = Result()
        fut.set_result(1)
        with pytest.raises(RuntimeError):
            fut.set_exception(ValueError())

    def test_result(self):
        fut = Result()
        with pytest.raises(InvalidStateError):
            fut.result()
        fut.set_result(1)
        assert fut.result() == 1

    def test_result_cancelled(self):
        fut = Result()
        assert fut.cancel()
        with pytest.raises(ResultCancelledError):
            fut.result()

    def test_result_exception(self):
        fut = Result()
        fut.set_exception(TypeError("my exception"))
        with pytest.raises(TypeError, match="my exception"):
            fut.result()

    async def test_cancel(self):
        fut = Result()
        assert fut.cancel()
        with pytest.raises(ResultCancelledError):
            fut.exception()

    async def test_set_from_non_thread(self, caller: Caller):
        fut = Result()
        caller.to_thread(fut.set_result, value=123)
        assert (await fut) == 123

    async def test_wait_cancelled_shield(self, caller: Caller):
        fut = Result()
        with pytest.raises(TimeoutError):
            await fut.wait(timeout=0.001, shield=True)
        assert not fut.cancelled()
        with pytest.raises(TimeoutError):
            await fut.wait(timeout=0.001)
        assert fut.cancelled()

    def test_repr(self):
        a = "long string" * 100
        b = {f"name {i}": "long_string" * 100 for i in range(100)}
        fut = Result()
        fut.metadata.update(a=a, b=b)
        matches = [
            f"<Result {indicator} at {id(fut)} {{'a': 'long stringl…nglong string', 'b': {{…}}}} >"
            for indicator in ("🏃", "⛔ 🏃", "⛔ 🏁")
        ]
        assert re.match(matches[0], repr(fut))
        fut.cancel()
        assert re.match(matches[1], repr(fut))
        fut.set_result(None)
        assert re.match(matches[2], repr(fut))

    async def test_gc(self, caller: Caller):
        finalized = Event()
        ok = False

        def isolated():
            class Cls:
                def func(self):
                    assert Caller.current_result()
                    nonlocal ok
                    ok = True

            t = Cls()
            weakref.finalize(t, finalized.set)
            fut = caller.call_soon(t.func)
            id_ = id(fut)
            assert hash(fut.metadata["func"]) == hash(t.func)
            r = weakref.ref(fut)
            del fut
            del t
            return r, id_

        r, id_ = isolated()
        assert id_ in Result._metadata_mappings  # pyright: ignore[reportPrivateUsage]
        with anyio.move_on_after(1):
            await finalized
        assert r() is None, f"References found {gc.get_referrers(r())}"
        assert ok
        assert id_ not in Result._metadata_mappings  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.parametrize("result", [True, False])
    async def test_wait_sync(self, caller: Caller, result: bool):
        fut = caller.to_thread(lambda: 1 + 1)
        assert fut.wait_sync(result=result) == (2 if result else None)

    async def test_wait_sync_timeout(self, caller: Caller):
        fut = caller.call_soon(anyio.sleep_forever)
        with pytest.raises(TimeoutError):
            fut.wait_sync(timeout=0.01)
