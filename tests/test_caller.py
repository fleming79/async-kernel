import contextlib
import importlib.util
import inspect
import threading
import time
import weakref
from random import random
from typing import Literal, cast

import anyio
import anyio.to_thread
import pytest
import sniffio

from async_kernel.caller import (
    AsyncEvent,
    AsyncLock,
    Caller,
    Future,
    FutureCancelledError,
    InvalidStateError,
    ReentrantAsyncLock,
)
from async_kernel.kernelspec import Backend


@pytest.fixture(params=list(Backend) if importlib.util.find_spec("trio") else [Backend.asyncio])
def anyio_backend(request):
    return request.param


@pytest.fixture
async def caller(anyio_backend: Backend):
    try:
        async with Caller(create=True) as caller:
            yield caller
    finally:
        Caller.stop_all()


@pytest.mark.anyio
class TestFuture:
    def test_weakref(self):
        f = Future()
        assert weakref.ref(f)() is f

    async def test_set_and_wait_result(self):
        fut = Future[int]()
        assert inspect.isawaitable(fut)
        done_called = False
        after_done = AsyncEvent()

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
            await after_done.wait()

    async def test_set_and_wait_exception(self):
        fut = Future()
        done_called = False

        def callback(obj):
            nonlocal done_called
            assert obj is fut
            done_called = True

        fut.add_done_callback(callback)
        assert not fut.done()
        exc = ValueError("fail")
        fut.set_exception(exc)
        with pytest.raises(ValueError, match="fail") as e:
            await fut
        assert e.value is exc
        assert fut.done()
        assert done_called
        assert fut.remove_done_callback(callback) == 1

    async def test_set_result_twice_raises(self):
        fut = Future()
        fut.set_result(1)
        with pytest.raises(RuntimeError):
            fut.set_result(2)

    async def test_set_canceller_twice_raises(self):
        fut = Future()
        with anyio.CancelScope() as cancel_scope:
            fut.set_canceller(cancel_scope.cancel)
            with pytest.raises(InvalidStateError):
                fut.set_canceller(cancel_scope.cancel)

    async def test_set_canceller_after_cancelled(self):
        fut = Future()
        fut.cancel()
        with anyio.CancelScope() as cancel_scope:
            fut.set_canceller(cancel_scope.cancel)
            assert cancel_scope.cancel_called

    async def test_set_exception_twice_raises(self):
        fut = Future()
        fut.set_exception(ValueError())
        with pytest.raises(InvalidStateError):
            fut.set_exception(ValueError())

    async def test_set_result_after_exception_raises(self):
        fut = Future()
        with pytest.raises(InvalidStateError):
            fut.exception()
        fut.set_exception(ValueError())
        assert isinstance(fut.exception(), ValueError)
        with pytest.raises(RuntimeError):
            fut.set_result(1)

    async def test_set_exception_after_result_raises(self):
        fut = Future()
        fut.set_result(1)
        with pytest.raises(RuntimeError):
            fut.set_exception(ValueError())

    def test_result(self):
        fut = Future()
        with pytest.raises(InvalidStateError):
            fut.result()
        fut.set_result(1)
        assert fut.result() == 1

    def test_result_cancelled(self):
        fut = Future()
        assert fut.cancel()
        with pytest.raises(FutureCancelledError):
            fut.result()

    def test_result_exception(self):
        fut = Future()
        fut.set_exception(TypeError("my exception"))
        with pytest.raises(TypeError, match="my exception"):
            fut.result()

    async def test_cancel(self):
        fut = Future()
        assert fut.cancel()
        with pytest.raises(FutureCancelledError):
            fut.exception()

    def test_error_from_non_thread(self):
        fut = Future(threading.Thread())
        with pytest.raises(RuntimeError):
            fut.set_result(None)

    async def test_set_from_non_thread(self, anyio_backend):
        caller = Caller.start_new(backend=anyio_backend)
        try:
            fut = Future(caller.thread)
            assert fut.thread is not threading.current_thread()
            fut.set_result(value=123)
            assert (await fut) == 123
            with pytest.raises(RuntimeError, match="already exists"):
                Caller.start_new(name=caller.thread.name)
        finally:
            caller.stop()

    async def test_start_new_should_fail(self, caller: Caller):
        await anyio.to_thread.run_sync(print, "")
        name = next(iter(t.name for t in threading.enumerate() if t.name != "MainThread"))
        with pytest.raises(RuntimeError, match="already exists"):
            Caller.start_new(name=name)

    async def test_wait_cancelled_shield(self, caller: Caller):
        fut = Future()
        with pytest.raises(TimeoutError):
            await fut.wait(timeout=0.001, shield=True)
        assert not fut.cancelled()
        with pytest.raises(TimeoutError):
            await fut.wait(timeout=0.001)
        assert fut.cancelled()

    def test_repr(self):
        fut = Future(name="test", mydict={"test": "a long string" * 100})
        assert repr(fut) == "<Future MainThread 🏃 {'mydict': {…}, 'name': 'test'} >"

    async def test_gc(self, caller: Caller):
        finalized = anyio.Event()
        ok = False

        class Cls:
            def func(self):
                assert Caller.current_future()
                nonlocal ok
                ok = True

        t = Cls()
        weakref.finalize(t, finalized.set)
        fut = caller.call_soon(t.func)
        assert hash(fut.metadata["func"]) == hash(t.func)
        del fut
        del t
        await finalized.wait()
        assert ok


@pytest.mark.anyio
class TestCaller:
    def setup_method(self, test_method):
        Caller.stop_all()

    def teardown_method(self, test_method):
        Caller.stop_all()

    async def test_sync(self):
        async with Caller(create=True) as caller:
            is_called = AsyncEvent()
            caller.call_later(0.01, is_called.set)
            await is_called.wait()

    async def test_call_returns_future(self, caller: Caller):
        fut = Future()
        caller.call_direct(lambda: fut)
        assert await caller.call_soon(lambda: fut) is fut

    async def test_repr(self, caller):
        async def test_func(a, b, c):
            pass

        a = "long string" * 100
        b = {f"name {i}": "long_string" * 100 for i in range(100)}
        c = Future()
        c.metadata.update(a=a, b=b)
        assert repr(c) == "<Future MainThread 🏃 {'a': 'long stringl…nglong string', 'b': {…}} >"
        fut = caller.call_soon(test_func, a, b, c)
        assert repr(fut).startswith("<Future MainThread 🏃 | <function")
        await fut
        assert repr(fut) == "<Future MainThread 🏁 >"
        c.cancel()
        assert repr(c) == "<Future MainThread ⛔ 🏃 {'a': 'long stringl…nglong string', 'b': {…}} >"
        c.set_result(None)
        assert repr(c) == "<Future MainThread ⛔ 🏁 {'a': 'long stringl…nglong string', 'b': {…}} >"

    def test_no_thread(self):
        with pytest.raises(RuntimeError):
            Caller()

    async def test_protected(self, anyio_backend):
        caller = Caller(create=True, protected=True)
        caller.stop()
        assert not caller.stopped
        caller.stop(force=True)

    @pytest.mark.parametrize("args_kwargs", argvalues=[((), {}), ((1, 2, 3), {"a": 10})])
    async def test_async(self, args_kwargs: tuple[tuple, dict]):
        val = None

        async def my_func(is_called: AsyncEvent, *args, **kwargs):
            nonlocal val
            val = args, kwargs
            is_called.set()
            return args, kwargs

        async with Caller(create=True) as caller:
            is_called = AsyncEvent()
            fut = caller.call_later(0.1, my_func, is_called, *args_kwargs[0], **args_kwargs[1])
            await is_called.wait()
            assert val == args_kwargs
            assert (await fut) == args_kwargs

    async def test_anyio_to_thread(self, anyio_backend):
        # Test the call works from an anyio thread
        async with Caller(create=True) as caller:
            assert caller.running
            assert caller in Caller.all_callers()

            def _in_thread():
                def my_func(*args, **kwargs):
                    return args, kwargs

                async def runner():
                    fut = caller.call_soon(my_func, 1, 2, 3, a=10)
                    result = await fut
                    assert result == ((1, 2, 3), {"a": 10})

                anyio.run(runner)

            await anyio.to_thread.run_sync(_in_thread)
        assert caller not in Caller.all_callers()

    async def test_call_soon_cancelled_early(self, caller: Caller):
        fut = caller.call_soon(anyio.sleep_forever)
        fut.cancel()
        await fut.wait(result=False)

    async def test_direct_async(self, caller: Caller):
        event: AsyncEvent = AsyncEvent()

        async def set_event():
            event.set()

        caller.call_direct(set_event)
        with anyio.fail_after(1):
            await event.wait()

    async def test_cancels_on_exit(self):
        is_cancelled = False
        async with Caller(create=True) as caller:

            async def my_test():
                nonlocal is_cancelled
                started.set()
                exception_ = anyio.get_cancelled_exc_class()
                try:
                    await anyio.sleep_forever()
                except exception_:
                    is_cancelled = True

            started = AsyncEvent()
            caller.call_later(0.01, my_test)
            await started.wait()
        assert is_cancelled

    @pytest.mark.parametrize("check_result", ["result", "exception"])
    @pytest.mark.parametrize("check_mode", ["main", "local", "asyncio", "trio"])
    async def test_wait_from_threads(self, anyio_backend, check_mode: str, check_result: str):
        finished_event = cast("AsyncEvent", object)
        ready = threading.Event()

        def _thread_task():
            nonlocal the_thread
            nonlocal finished_event
            finished_event = AsyncEvent()

            async def _run():
                async with Caller(create=True) as caller:
                    assert caller.backend == anyio_backend
                    ready.set()
                    await finished_event.wait()

            anyio.run(_run, backend=anyio_backend)

        the_thread = threading.Thread(target=_thread_task, daemon=True)
        the_thread.start()
        ready.wait()
        assert isinstance(finished_event, AsyncEvent)
        caller = Caller.get_instance(the_thread.name)
        if check_result == "result":
            expr = "10"
            context = contextlib.nullcontext()
        else:
            expr = "invalid call"
            context = pytest.raises(SyntaxError)
        fut = caller.call_later(0.01, eval, expr)
        with context:
            match check_mode:
                case "main":
                    assert (await fut) == 10
                case "local":
                    fut_local = caller.call_soon(fut.wait)
                    result = await fut_local
                    assert result == 10
                case "asyncio" | "trio":

                    def another_thread():
                        async def waiter():
                            result = await fut
                            assert result == 10
                            return result

                        return anyio.run(waiter, backend=check_mode)

                    result = await anyio.to_thread.run_sync(another_thread)
                    assert result == 10
                case _:
                    raise NotImplementedError

        caller.call_soon(finished_event.set)
        the_thread.join()

    async def test_get_instance_no_instance(self, anyio_backend):
        with pytest.raises(RuntimeError):
            Caller.get_instance(None, create=False)

    async def test_get_instance_get_runner(self, anyio_backend):
        if anyio_backend == Backend.trio:
            with pytest.raises(RuntimeError):
                Caller.get_instance()
            return
        caller = Caller.get_instance()
        try:
            await caller.call_soon(anyio.sleep, 0.01)
        finally:
            caller.stop()

    async def test_get_runner_error(self):
        caller = Caller(create=True)
        caller.stop()
        with pytest.raises(RuntimeError):
            caller.get_runner()

    @pytest.mark.parametrize("mode", ["restricted", "surge"])
    async def test_as_completed(self, anyio_backend, mode: Literal["restricted", "surge"], mocker):
        mocker.patch.object(Caller, "MAX_IDLE_POOL_INSTANCES", new=2)

        async def func():
            assert sniffio.current_async_library() == anyio_backend
            n = random()
            if n < 0.2:
                time.sleep(n / 10)
            elif n < 0.6:
                await anyio.sleep(n / 10)
            return threading.current_thread()

        threads = set[threading.Thread]()
        n = 40
        fut = Caller.to_thread(time.sleep, 0)
        await fut
        async with Caller(create=True):
            # check can handle completed future okay first
            async for fut_ in Caller.as_completed([fut]):
                assert fut_.done()
            # work directly with iterator
            n_ = 0
            max_concurrent = Caller.MAX_IDLE_POOL_INSTANCES if mode == "restricted" else n / 2
            async for fut in Caller.as_completed(
                (Caller.to_thread(func) for _ in range(n)), max_concurrent=max_concurrent
            ):
                assert fut.done()
                n_ += 1
                thread = await fut
                threads.add(thread)
            assert n_ == n
            if mode == "restricted":
                assert len(threads) == 2
            else:
                assert len(threads) > 2
            assert len(Caller._to_thread_pool) == 2  # pyright: ignore[reportPrivateUsage]

    async def test_as_completed_error(self, caller: Caller):
        def func():
            raise RuntimeError()

        async for fut in Caller.as_completed((Caller.to_thread(func) for _ in range(6)), max_concurrent=4):
            with pytest.raises(RuntimeError):
                await fut

    async def test_as_completed_cancelled(self, caller):
        items = {Caller.to_thread(anyio.sleep, 100) for _ in range(4)}
        with anyio.move_on_after(0.1):
            with pytest.raises(anyio.get_cancelled_exc_class()):
                async for _ in Caller.as_completed(items):
                    pass
        for item in items:
            assert item.cancelled()
            with pytest.raises(FutureCancelledError):
                await item

    async def test__check_in_thread(self, anyio_backend):
        Caller.to_thread(anyio.sleep, 0.1)
        worker = next(iter(Caller.all_callers()))
        assert not worker.protected
        with pytest.raises(RuntimeError):
            worker._check_in_thread()  # pyright: ignore[reportPrivateUsage]

    async def test_execution_queue(self, caller: Caller):
        N = 10

        pool = list(range(N))
        for _ in range(2):
            firstcall = AsyncEvent()

            async def func(a, b, /, *, results, firstcall=firstcall):
                firstcall.set()
                if b:
                    await anyio.sleep_forever()
                results.append(b)

            results = []
            for j in pool:
                caller.queue_call(func, 0, j, results=results)
            fut = caller.queue_get(func)
            assert fut
            assert results != pool
            await firstcall.wait()
            assert results == [0]
            caller.queue_close(func)
            assert not caller.queue_get(func)

    async def test_execution_queue_from_thread(self, caller: Caller):
        event = AsyncEvent()
        caller.to_thread(caller.queue_call, event.set)
        await event.wait()

    async def test_gc(self, anyio_backend):
        event_finalize_called = anyio.Event()
        async with Caller(create=True) as caller:
            weakref.finalize(caller, event_finalize_called.set)
            del caller
        await anyio.sleep(0.1)
        await event_finalize_called.wait()

    async def test_queue_cancel(self, caller: Caller):
        started = AsyncEvent()

        async def test_func():
            started.set()
            await anyio.sleep_forever()

        caller.queue_call(test_func)
        fut = caller.queue_get(test_func)
        assert fut
        await started.wait()
        fut.cancel()
        await fut.wait(result=False)

    async def test_execution_queue_gc(self, caller: Caller):
        class MyObj:
            async def method(self):
                method_called.set()

        obj_finalized = AsyncEvent()
        method_called = AsyncEvent()
        obj = MyObj()
        weakref.finalize(obj, obj_finalized.set)
        caller.queue_call(obj.method)
        await method_called.wait()
        assert caller.queue_get(obj.method), "A ref should be retained unless it is explicitly removed"
        del obj

        await obj_finalized.wait()
        assert not any(caller._queue_map)  # pyright: ignore[reportPrivateUsage]

    async def test_call_early(self, anyio_backend) -> None:
        caller = Caller(create=True)
        assert not caller.running
        fut = caller.call_soon(time.sleep, 0.1)
        await anyio.sleep(delay=0.1)
        assert not fut.done()
        async with caller:
            await fut

    async def test_current_future(self, anyio_backend):
        async with Caller(create=True) as caller:
            fut = caller.call_soon(Caller.current_future)
            res = await fut
            assert res is fut

    async def test_closed_in_call_soon(self, caller: Caller):
        ready = threading.Event()
        proceed = threading.Event()

        async def close_tsc():
            caller = Caller(create=True)
            ready.set()
            proceed.wait()
            caller.stop()
            await anyio.sleep_forever()

        fut = Caller.to_thread(close_tsc)
        caller = Caller.get_instance(fut.thread.name)
        ready.wait()
        never_called_future = caller.call_later(10, str)
        proceed.set()
        with pytest.raises(FutureCancelledError):
            await fut
        assert fut.done()
        assert caller.stopped
        with pytest.raises(anyio.ClosedResourceError):
            caller.call_soon(time.sleep, 0)
        with pytest.raises(FutureCancelledError):
            await never_called_future

    @pytest.mark.parametrize("mode", ["async", "blocking"])
    @pytest.mark.parametrize("cancel_mode", ["local", "thread"])
    @pytest.mark.parametrize("msg", ["msg", None, "twice"])
    async def test_cancel(
        self, caller: Caller, mode: Literal["async", "blocking"], cancel_mode: Literal["local", "thread"], msg
    ):
        ready = AsyncEvent()
        proceed = AsyncEvent()

        async def blocking_func():
            ready.set()
            await proceed.wait()
            time.sleep(0.1)

        async def non_blocking_func():
            ready.set()
            await anyio.sleep_forever()

        my_func = blocking_func if mode == "blocking" else non_blocking_func

        fut = caller.call_soon(my_func)
        await ready.wait()
        proceed.set()
        if cancel_mode == "local":
            fut.cancel(msg)
            if msg == "twice":
                fut.cancel(msg)
                msg = f"{msg}(?s:.){msg}"
        else:

            def in_thread():
                proceed.set()
                time.sleep(0.01)
                fut.cancel(msg)

            caller.to_thread(in_thread)

        with pytest.raises(FutureCancelledError, match=msg):
            await fut

    async def test_cancelled_waiter(self, caller: Caller):
        # Cancelling the waiter should also cancel call soon operation.
        async def async_func():
            await anyio.sleep(10)
            raise RuntimeError

        fut = caller.call_soon(async_func)
        with anyio.move_on_after(0.1):
            await fut
        with pytest.raises(FutureCancelledError):
            fut.exception()

    @pytest.mark.parametrize("return_when", ["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"])
    async def test_wait(self, caller: Caller, return_when):
        waiters = [AsyncEvent() for _ in range(4)]
        waiters[0].set()

        async def f(i: int):
            await waiters[i].wait()
            try:
                if i == 1:
                    raise RuntimeError
            finally:
                waiters[i + 1].set()

        items = [caller.call_later(i * 0.01, f, i) for i in range(3)]
        done, pending = await Caller.wait(items, return_when=return_when)
        match return_when:
            case "FIRST_COMPLETED":
                assert items[0] in done
            case "FIRST_EXCEPTION":
                assert items[1] in done
            case _:
                assert done == set(items)
                assert not pending

    async def test_cancelled_future(self, caller: Caller):
        fut = caller.call_soon(anyio.sleep_forever)
        await anyio.sleep(0.1)
        a = AsyncEvent()
        weakref.finalize(a, fut.cancel)
        del a
        await fut.wait(result=False)


class TestLock:
    async def test_basic(self, caller: Caller):
        lock = AsyncLock()
        count = 0

        async def get_lock():
            nonlocal count
            count += 1
            while count < 3:
                await anyio.sleep(0.01)
            for _ in range(2):
                assert not lock.is_in_context()
                async with lock:
                    await anyio.sleep(0.02)
                    assert lock.is_in_context()
                    assert lock.count == 1
                    with pytest.raises(RuntimeError, match="Already locked and not reentrant!"):
                        async with lock:
                            pass
            count += 1

        async for fut in Caller.as_completed([caller.call_soon(get_lock) for _ in range(3)]):
            await fut
        assert lock.count == 0
        assert count == 6

    async def test_pops_on_error(self, caller: Caller):
        lock = AsyncLock()
        locked = AsyncEvent()
        unlock = AsyncEvent()

        async def _locked():
            async with lock:
                locked.set()
                await unlock.wait()

        caller.call_soon(_locked)
        await locked.wait()
        with anyio.move_on_after(0):
            await lock.acquire()
            raise RuntimeError
        unlock.set()
        assert not lock._queue  # pyright: ignore[reportPrivateUsage]

    async def test_invald_release(self, caller):
        lock = AsyncLock()
        with pytest.raises(InvalidStateError):
            await lock.release()

    async def test_reentrant(self, caller: Caller):
        lock = ReentrantAsyncLock()

        async def func():
            assert lock.count == 2
            async with lock:
                assert lock.is_in_context()
                assert lock.count == 3
            return True

        async with lock:
            assert lock.is_in_context()
            assert lock.count == 1
            async with lock:
                await caller.call_soon(func)
        assert lock.count == 0
        assert not lock.is_in_context()

    async def test_reentrant_outside(self, caller: Caller):
        # We need to test the case where a lock is released with a common context
        # It would be better practice maintain the lock, but it shows the lock can be reacquired.
        lock = ReentrantAsyncLock()
        begin = AsyncEvent()
        n = 10
        ctx_ids = set()

        async def do_lock():
            futures = set()
            async with lock:
                for _ in range(n):
                    ready = AsyncEvent()
                    futures.add(caller.call_later(0.1, isolated_lock, ready))
                    await ready.wait()
            lock._count = 1  # pyright: ignore[reportPrivateUsage]
            return futures

        async def isolated_lock(ready: AsyncEvent):
            if ready:
                ready.set()
                await begin.wait()
            assert lock.count == 1
            async with lock:
                ctx_ids.add(lock._ctx_var.get())  # pyright: ignore[reportPrivateUsage]

        futures = await caller.call_soon(do_lock)
        begin.set()
        while len(lock._queue) < n:  # pyright: ignore[reportPrivateUsage]
            await anyio.sleep(0.01)
        # Release the lock
        lock._count = 0  # pyright: ignore[reportPrivateUsage]
        async with lock:
            await anyio.sleep(0.01)
        # Wait for restoration
        async for fut in Caller.as_completed(futures):
            assert fut
        assert len(ctx_ids) == 1

    async def test_reentrant_thread(self, caller: Caller) -> None:
        lock = ReentrantAsyncLock()
        count = 0

        async def tester_async():
            nonlocal count
            async with lock:
                count += 1

        async with lock:
            futures = [caller.call_soon(tester_async), caller.to_thread(tester_async)]
            await tester_async()
            await Caller.wait(futures)
            assert count == 3

    async def test_nested_reentrant(self, caller: Caller):
        count = 0
        lock = ReentrantAsyncLock()
        n = 3

        async def using_lock():
            nonlocal count
            count += 1
            async with lock:
                while count < n:
                    await anyio.sleep(0.01)
                assert lock.count == 1
                await caller.call_soon(nested)

        async def nested():
            nonlocal count
            await anyio.sleep(0.01)
            assert lock.count == 1
            async with lock:
                count += 1
                assert lock.count == 2
            assert lock.count == 1

        async for fut in Caller.as_completed([caller.call_soon(using_lock) for _ in range(n)]):
            await fut
        assert not lock.count
        assert not lock._queue  # pyright: ignore[reportPrivateUsage]


class TestAsyncEvent:
    async def test_current_thread(self, caller):
        event = AsyncEvent()
        futures = {Caller.to_thread(event.wait), caller.call_soon(event.wait)}
        caller.call_later(0.1, event.set)
        await Caller.wait(futures)
        # Test again for  an already set event
        futures = {Caller.to_thread(event.wait), caller.call_soon(event.wait)}
        await Caller.wait(futures)

    async def test_another_thread(self, caller):
        ct = Caller.start_new()
        event = AsyncEvent(ct.thread)
        caller.call_later(0.1, event.set)
        await event.wait()

    def test_repr(self):
        event = AsyncEvent()
        assert repr(event) == "<AsyncEvent 🏃>"
        event.set()
        assert repr(event) == "<AsyncEvent 🏁>"
