import contextlib
import gc
import importlib.util
import inspect
import re
import threading
import time
import weakref
from random import random
from typing import Literal, cast

import anyio
import anyio.to_thread
import pytest
from aiologic import Event
from aiologic.lowlevel import create_async_event, current_async_library

from async_kernel.caller import Caller, Future, FutureCancelledError, InvalidStateError
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

    def test_is_slots(self):
        f = Future()
        with pytest.raises(AttributeError):
            f.not_an_att = None  # pyright: ignore[reportAttributeAccessIssue]

    async def test_set_and_wait_result(self):
        fut = Future[int]()
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
        fut = Future()
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

    async def test_set_from_non_thread(self, caller: Caller):
        fut = Future()
        caller.to_thread(fut.set_result, value=123)
        assert (await fut) == 123

    async def test_already_exists(self, caller: Caller):
        with pytest.raises(RuntimeError, match="already exists"):
            Caller.start_new(name=caller.thread.name)

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
        a = "long string" * 100
        b = {f"name {i}": "long_string" * 100 for i in range(100)}
        fut = Future()
        fut.metadata.update(a=a, b=b)
        matches = [
            f"<Future {indicator} at {id(fut)} {{'a': 'long stringl…nglong string', 'b': {{…}}}} >"
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
                    assert Caller.current_future()
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
        assert id_ in Future._metadata_mappings  # pyright: ignore[reportPrivateUsage]
        with anyio.move_on_after(1):
            await finalized
        assert r() is None, f"References found {gc.get_referrers(r())}"
        assert ok
        assert id_ not in Future._metadata_mappings  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.parametrize("result", [True, False])
    async def test_wait_sync(self, caller: Caller, result: bool):
        fut = caller.to_thread(lambda: 1 + 1)
        assert fut.wait_sync(result=result) == (2 if result else None)

    async def test_wait_sync_timeout(self, caller: Caller):
        fut = caller.call_soon(anyio.sleep_forever)
        with pytest.raises(TimeoutError):
            fut.wait_sync(timeout=0.01)


@pytest.mark.anyio
class TestCaller:
    def setup_method(self, test_method):
        Caller.stop_all()

    def teardown_method(self, test_method):
        Caller.stop_all()

    async def test_sync(self):
        async with Caller(create=True) as caller:
            is_called = Event()
            caller.call_later(0.01, is_called.set)
            await is_called

    async def test_call_returns_future(self, caller: Caller):
        fut = Future()
        caller.call_direct(lambda: fut)
        assert await caller.call_soon(lambda: fut) is fut

    async def test_repr_caller_future(self, caller):
        async def test_func(a, b, c):
            pass

        fut = caller.call_soon(test_func, 1, "ABC", {"a": 10})
        matches = [
            f"<Future {indicator} at {id(fut)} | <function TestCaller.test_repr.<locals>.test_func at {id(test_func)}> caller=Caller<MainThread 🏃> >"
            for indicator in ("🏃", "🏁")
        ]
        assert re.match(matches[0], repr(fut))
        await fut
        assert re.match(matches[1], repr(fut))

    def test_no_thread(self):
        with pytest.raises(RuntimeError):
            Caller()

    async def test_protected(self, anyio_backend: Backend):
        caller = Caller(create=True, protected=True)
        caller.stop()
        assert not caller.stopped
        caller.stop(force=True)

    @pytest.mark.parametrize("args_kwargs", argvalues=[((), {}), ((1, 2, 3), {"a": 10})])
    async def test_async(self, args_kwargs: tuple[tuple, dict]):
        val = None

        async def my_func(is_called: Event, *args, **kwargs):
            nonlocal val
            val = args, kwargs
            is_called.set()
            return args, kwargs

        async with Caller(create=True) as caller:
            is_called = Event()
            fut = caller.call_later(0.1, my_func, is_called, *args_kwargs[0], **args_kwargs[1])
            await is_called
            assert val == args_kwargs
            assert (await fut) == args_kwargs

    async def test_anyio_to_thread(self, anyio_backend: Backend):
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
        event: Event = Event()

        async def set_event():
            event.set()

        caller.call_direct(set_event)
        with anyio.fail_after(1):
            await event

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

            started = Event()
            caller.call_later(0.01, my_test)
            await started
        assert is_cancelled

    @pytest.mark.parametrize("check_result", ["result", "exception"])
    @pytest.mark.parametrize("check_mode", ["main", "local", "asyncio", "trio"])
    async def test_wait_from_threads(self, anyio_backend, check_mode: str, check_result: str):
        finished_event = cast("Event", object)
        ready = threading.Event()

        def _thread_task():
            nonlocal the_thread
            nonlocal finished_event
            finished_event = Event()

            async def _run():
                async with Caller(create=True) as caller:
                    assert caller.backend == anyio_backend
                    ready.set()
                    await finished_event

            anyio.run(_run, backend=anyio_backend)

        the_thread = threading.Thread(target=_thread_task, daemon=True)
        the_thread.start()
        ready.wait()
        assert isinstance(finished_event, Event)
        caller = Caller.get_instance(name=the_thread.name)
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

    async def test_to_thread_advanced_no_name(self):
        with pytest.raises(ValueError, match="A name was not provided"):
            Caller.to_thread_advanced({}, lambda: None)

    async def test_get_instance_main_thread(self, anyio_backend: Backend):
        name = "MainThread"
        # Check a caller can be started in asyncio using create_task.
        assert not Caller._instances  # pyright: ignore[reportPrivateUsage]
        if anyio_backend == Backend.trio:
            with pytest.raises(RuntimeError):
                Caller.get_instance(name=name)
            return
        if anyio_backend == Backend.asyncio:
            caller = Caller.get_instance(name=name)
        assert await caller.call_soon(lambda: 1 + 1) == 2

    async def test_get_instance_no_instance(self, anyio_backend: Backend):
        with pytest.raises(RuntimeError):
            Caller.get_instance(name=None, create=False)

    async def test_get_instance_get_runner(self, anyio_backend: Backend):
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
    async def test_as_completed(self, anyio_backend: Backend, mode: Literal["restricted", "surge"], mocker):
        mocker.patch.object(Caller, "MAX_IDLE_POOL_INSTANCES", new=2)

        async def func():
            assert current_async_library() == anyio_backend
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

    async def test_as_completed_cancelled(self, caller: Caller):
        items = {Caller.to_thread(anyio.sleep, 100) for _ in range(4)}
        with anyio.move_on_after(0.1):
            with pytest.raises(anyio.get_cancelled_exc_class()):
                async for _ in Caller.as_completed(items):
                    pass
        for item in items:
            assert item.cancelled()
            with pytest.raises(FutureCancelledError):
                await item

    async def test_execution_queue(self, caller: Caller):
        N = 10

        pool = list(range(N))
        for _ in range(2):
            firstcall = Event()

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
            await firstcall
            assert results == [0]
            caller.queue_close(func)
            assert not caller.queue_get(func)

    async def test_execution_queue_from_thread(self, caller: Caller):
        event = Event()
        caller.to_thread(caller.queue_call, event.set)
        await event

    async def test_gc(self, anyio_backend: Backend):
        event_finalize_called = Event()
        async with Caller(create=True) as caller:
            weakref.finalize(caller, event_finalize_called.set)
            del caller
        await anyio.sleep(0.1)
        await event_finalize_called

    async def test_queue_cancel(self, caller: Caller):
        started = Event()

        async def test_func():
            started.set()
            await anyio.sleep_forever()

        caller.queue_call(test_func)
        fut = caller.queue_get(test_func)
        assert fut
        await started
        fut.cancel()
        await fut.wait(result=False)

    async def test_execution_queue_gc(self, caller: Caller):
        class MyObj:
            async def method(self):
                method_called.set()

        obj_finalized = Event()
        method_called = Event()
        obj = MyObj()
        weakref.finalize(obj, obj_finalized.set)
        caller.queue_call(obj.method)
        await method_called
        assert caller.queue_get(obj.method), "A ref should be retained unless it is explicitly removed"
        del obj

        await obj_finalized
        assert not any(caller._queue_map)  # pyright: ignore[reportPrivateUsage]

    async def test_call_early(self, anyio_backend: Backend) -> None:
        caller = Caller(create=True)
        assert not caller.running
        fut = caller.call_soon(time.sleep, 0.1)
        await anyio.sleep(delay=0.1)
        assert not fut.done()
        async with caller:
            await fut

    async def test_prevent_multi_entry(self, anyio_backend: Backend):
        async with Caller(create=True) as caller:
            assert caller is Caller()
            with pytest.raises(RuntimeError):
                async with caller:
                    pass
        assert caller.stopped
        await caller._stopped_event  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(RuntimeError):
            async with caller:
                pass

    async def test_current_future(self, anyio_backend: Backend):
        async with Caller(create=True) as caller:
            fut = caller.call_soon(Caller.current_future)
            res = await fut
            assert res is fut

    async def test_closed_in_call_soon(self):
        ready = threading.Event()
        proceed = threading.Event()

        async def close_tsc():
            ready.set()
            proceed.wait()
            caller.stop()
            await anyio.sleep_forever()

        caller = Caller.get_instance(create=True, name=None)
        fut = caller.call_soon(close_tsc)
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
        ready = Event()
        proceed = Event()

        async def blocking_func():
            ready.set()
            await proceed
            time.sleep(0.1)

        async def non_blocking_func():
            ready.set()
            await anyio.sleep_forever()

        my_func = blocking_func if mode == "blocking" else non_blocking_func

        fut = caller.call_soon(my_func)
        await ready
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

    async def test_cancelled_while_waiting(self, caller: Caller):
        async def async_func():
            with anyio.fail_after(0.01):
                await anyio.sleep_forever()

        fut = caller.call_soon(async_func)
        with pytest.raises(TimeoutError):
            await fut

    @pytest.mark.parametrize("return_when", ["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"])
    async def test_wait(
        self, caller: Caller, return_when: Literal["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"]
    ):
        waiters = [create_async_event() for _ in range(4)]
        waiters[0].set()

        async def f(i: int):
            await waiters[i]
            try:
                if i == 1:
                    raise RuntimeError
            finally:
                caller.call_soon(waiters[i + 1].set)

        items = [caller.call_soon(f, i) for i in range(3)]
        done, pending = await Caller.wait(items, return_when=return_when)
        match return_when:
            case "FIRST_COMPLETED":
                assert {items[0]} == done
            case "FIRST_EXCEPTION":
                assert {*items[0:2]} == done
            case _:
                assert {*items} == done
                assert not pending

    async def test_cancelled_future(self, caller: Caller):
        fut = caller.call_soon(anyio.sleep_forever)
        await anyio.sleep(0.1)
        a = Event()
        weakref.finalize(a, fut.cancel)
        del a
        await fut.wait(result=False)
