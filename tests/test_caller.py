import contextlib
import gc
import re
import threading
import time
import weakref
from random import random
from typing import Literal

import anyio
import anyio.to_thread
import pytest
from aiologic import CountdownEvent, Event
from aiologic.lowlevel import create_async_event, current_async_library

from async_kernel.caller import Caller
from async_kernel.pending import Pending, PendingCancelled
from async_kernel.typing import Backend


@pytest.fixture(params=Backend, scope="module")
def anyio_backend(request):
    return request.param


@pytest.mark.anyio
class TestCaller:
    def test_no_thread(self, anyio_backend: Backend):
        with pytest.raises(RuntimeError, match="unknown async library, or not in async context"):
            Caller()

    async def test_worker_lifecycle(self, anyio_backend: Backend):
        async with Caller("manual") as caller:
            assert not caller.protected
            # worker thread
            assert await caller.to_thread(lambda: 2 + 1) == 3
            assert len(caller.children) == 1
            worker = next(iter(caller.children))
            assert worker.thread.name == "async_kernel_caller"
            # Child thread
            c1 = caller.get(name="c1", protected=True)
            assert c1 in caller.children
            assert len(caller.children) == 2
            assert caller.get(name="c1") is c1
            wrong_backend = next(b for b in Backend if b != anyio_backend)
            with pytest.raises(RuntimeError, match="Backend mismatch!"):
                caller.get(name="c1", backend=wrong_backend)
            # A child's child
            c2 = c1.get(name="c2")
            assert c2 in c1.children
            assert c2 not in caller.children
            assert c1.get(name="c2") is c2
            assert Caller("MainThread") is caller

        assert len(caller.children) == 0
        assert c1.stopped
        assert c2.stopped
        c3 = Caller("manual")
        c3.stop()
        assert c3.stopped

    async def test_already_exists(self, caller: Caller):
        assert Caller.get_current(caller.thread)
        assert Caller("MainThread") is caller
        with pytest.raises(RuntimeError, match="An instance already exists for thread="):
            Caller("manual")

    async def test_start_after(self, anyio_backend: Backend):
        caller = Caller("manual")
        assert not caller.running
        pen = caller.call_soon(lambda: 2 + 3)
        async with caller:
            assert caller.running
            assert await pen == 5

    async def test_get_non_main_thread(self, anyio_backend: Backend):
        async def get_caller():
            thread = threading.current_thread()
            assert thread is not threading.main_thread()
            caller = Caller()
            assert caller.thread is thread
            assert (await caller.call_soon(lambda: 1 + 1)) == 2

        thread = threading.Thread(target=anyio.run, args=[get_caller])
        thread.start()
        thread.join()

    def test_no_event_loop(self, anyio_backend: Backend):
        assert current_async_library(failsafe=True) is None
        caller = Caller("NewThread", backend=anyio_backend)
        assert caller.thread is not threading.current_thread()
        assert caller.call_soon(lambda: 2 + 2).wait_sync() == 4
        caller.stop()

    async def test_sync(self):
        async with Caller("manual") as caller:
            is_called = Event()
            caller.call_later(0.01, is_called.set)
            await is_called

    async def test_call_returns_result(self, caller: Caller) -> None:
        pen = Pending()
        caller.call_direct(lambda: pen)
        assert await caller.call_soon(lambda: pen) is pen

    async def test_zmq_context(self, caller: Caller):
        assert caller.zmq_context is None

    async def test_repr_caller_result(self, caller):
        async def test_func(a, b, c):
            pass

        pen = caller.call_soon(test_func, 1, "ABC", {"a": 10})
        matches = [
            f"<Pending {indicator} at {id(pen)} | <function TestCaller.test_repr.<locals>.test_func at {id(test_func)}> caller=Caller<MainThread 🏃> >"
            for indicator in ("🏃", "🏁")
        ]
        assert re.match(matches[0], repr(pen))
        await pen
        assert re.match(matches[1], repr(pen))

    async def test_protected(self, anyio_backend: Backend):
        async with Caller("manual", protected=True) as caller:
            caller.stop()
            assert not caller.stopped
        assert caller.stopped

    @pytest.mark.parametrize("args_kwargs", argvalues=[((), {}), ((1, 2, 3), {"a": 10})])
    async def test_async(self, args_kwargs: tuple[tuple, dict]):
        val = None

        async def my_func(is_called: Event, *args, **kwargs):
            nonlocal val
            val = args, kwargs
            is_called.set()
            return args, kwargs

        async with Caller("manual") as caller:
            is_called = Event()
            pen = caller.call_later(0.1, my_func, is_called, *args_kwargs[0], **args_kwargs[1])
            await is_called
            assert val == args_kwargs
            assert (await pen) == args_kwargs

    async def test_anyio_to_thread(self, anyio_backend: Backend):
        # Test the call works from an anyio thread
        async with Caller("manual") as caller:
            assert caller.running
            assert caller in Caller.all_callers()

            def _in_thread():
                def my_func(*args, **kwargs):
                    return args, kwargs

                async def runner():
                    pen = caller.call_soon(my_func, 1, 2, 3, a=10)
                    result = await pen
                    assert result == ((1, 2, 3), {"a": 10})

                anyio.run(runner)

            await anyio.to_thread.run_sync(_in_thread)
        assert caller not in Caller.all_callers()

    async def test_usage_example(self, anyio_backend: Backend):
        async with Caller("manual") as caller:
            child_1 = caller.get()
            child_2 = caller.get(name="asyncio backend", backend="asyncio")
            child_3 = caller.get(name="trio backend", backend="trio")
            assert caller.children == {child_1, child_2, child_3}
        assert not caller.children

    async def test_async_enter_missing_modifier(self, anyio_backend: Backend):
        with pytest.raises(RuntimeError, match="Already starting! Did you mean to use"):
            async with Caller():
                pass
        Caller().stop()

    async def test_call_soon_cancelled_early(self, caller: Caller):
        pen = caller.call_soon(anyio.sleep_forever)
        pen.cancel()
        await pen.wait(result=False)

    async def test_direct_async(self, caller: Caller):
        event: Event = Event()

        async def set_event():
            event.set()

        def fail():
            raise RuntimeError

        caller.call_direct(fail)
        caller.call_direct(set_event)
        with anyio.fail_after(1):
            await event

    async def test_cancels_on_exit(self):
        is_cancelled = False
        async with Caller("manual") as caller:

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
        ready, finished = Event(), Event()

        def _thread_task():
            async def _run():
                async with Caller("manual") as caller:
                    assert caller.backend == anyio_backend
                    ready.set()
                    await finished

            anyio.run(_run, backend=anyio_backend)

        thread = threading.Thread(target=_thread_task)
        thread.start()
        await ready
        assert isinstance(finished, Event)
        caller = Caller.get_current(thread)
        assert caller
        if check_result == "result":
            expr = "10"
            context = contextlib.nullcontext()
        else:
            expr = "invalid call"
            context = pytest.raises(SyntaxError)
        pen = caller.call_later(0.01, eval, expr)
        with context:
            match check_mode:
                case "main":
                    assert (await pen) == 10
                case "local":
                    pen_local = caller.call_soon(pen.wait)
                    result = await pen_local
                    assert result == 10
                case "asyncio" | "trio":

                    def another_thread():
                        async def waiter():
                            result = await pen
                            assert result == 10
                            return result

                        return anyio.run(waiter, backend=check_mode)

                    result = await anyio.to_thread.run_sync(another_thread)
                    assert result == 10
                case _:
                    raise NotImplementedError

        caller.call_soon(finished.set)
        thread.join()

    async def test_get_start_main_thread(self, anyio_backend: Backend):
        # Check a caller can be started in the main thread synchronously.
        caller = Caller()
        assert await caller.call_soon(lambda: 1 + 1) == 2

    async def test_get_current_thread(self, anyio_backend: Backend):
        # Test starting in the async event loop of a non-main-thread
        pen = Pending[Caller]()
        done = Event()

        def caller_not_already_running():
            async def async_loop_before_caller_started():
                caller = Caller()
                pen.set_result(caller)
                await done

            anyio.run(async_loop_before_caller_started, backend=anyio_backend)

        thread = threading.Thread(target=caller_not_already_running)
        thread.start()
        caller = await pen
        assert (await caller.call_soon(lambda: 2 + 2)) == 4
        done.set()

    async def test_stop_early(self, anyio_backend: Backend):
        caller = Caller()
        caller.stop()
        await caller.stopped
        with pytest.raises(RuntimeError, match="is stopped!"):
            caller.call_soon(lambda: None)

    async def test_await_stopped(self, anyio_backend: Backend):
        caller = Caller()
        caller.call_soon(anyio.sleep_forever)
        assert await caller.call_soon(lambda: 1 + 1) == 2
        caller.stop()
        try:
            assert await caller.stopped
        except anyio.get_cancelled_exc_class():
            raise RuntimeError from None

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
            pen = caller.queue_get(func)
            assert pen
            assert results != pool
            await firstcall
            assert results == [0]
            caller.queue_close(func)
            assert not caller.queue_get(func)

    async def test_queue_call_result(self, caller: Caller):
        def pass_through(n):
            return n

        pen = Pending()
        for i in range(10):
            pen = caller.queue_call(pass_through, i)
        assert await pen == 9
        del pass_through
        while not pen.cancelled():
            gc.collect()
            await anyio.sleep(0)

    @pytest.mark.parametrize("anyio_backend", [Backend.asyncio])
    async def test_asyncio_queue_call_cancelled(self, caller: Caller):
        # Test queue_call can catch a CancelledError raised by the user
        from asyncio import CancelledError  # noqa: PLC0415

        def func(obj):
            if obj == "CancelledError":
                raise CancelledError
            obj()

        caller.queue_call(func, "CancelledError")
        okay = Event()
        await caller.queue_call(func, okay.set)
        assert okay

    async def test_execution_queue_from_thread(self, caller: Caller):
        event = Event()
        caller.to_thread(caller.queue_call, event.set)
        await event

    async def test_gc(self, anyio_backend: Backend):
        collected = Event()
        async with Caller("manual") as caller:
            assert await caller.call_soon(lambda: 1 + 1) == 2
            weakref.finalize(caller, collected.set)
            del caller

        while not collected:
            gc.collect()
            await anyio.sleep(0)

    async def test_queue_cancel(self, caller: Caller):
        started = Event()

        async def test_func():
            started.set()
            await anyio.sleep_forever()

        caller.queue_call(test_func)
        pen = caller.queue_get(test_func)
        assert pen
        await started
        pen.cancel()
        await pen.wait(result=False)

    async def test_execution_queue_gc(self, caller: Caller):
        class MyObj:
            async def method(self):
                method_called.set()

        collected = Event()
        method_called = Event()
        obj = MyObj()
        weakref.finalize(obj, collected.set)
        caller.queue_call(obj.method)
        await method_called
        assert caller.queue_get(obj.method), "A ref should be retained unless it is explicitly removed"
        del obj
        while not collected:
            gc.collect()
            await anyio.sleep(0)
        assert not any(caller._queue_map)  # pyright: ignore[reportPrivateUsage]

    async def test_call_early(self, anyio_backend: Backend) -> None:
        caller = Caller("manual")
        assert not caller.running
        pen = caller.call_soon(lambda: 3 + 3)
        await anyio.sleep(delay=0.1)
        assert not pen.done()
        async with caller:
            assert await pen == 6

    async def test_name_mismatch(self, caller: Caller):
        with pytest.raises(ValueError, match="The thread and caller's name do not match!"):
            Caller(name="wrong name")

    async def test_backend_mismatch(self, caller: Caller):
        wrong_backend = next(b for b in Backend if b != caller.backend)
        with pytest.raises(ValueError, match="The backend does not match!"):
            Caller(backend=wrong_backend)

    async def test_prevent_multi_entry(self, anyio_backend: Backend):
        async with Caller("manual") as caller:
            assert caller is Caller()
            with pytest.raises(RuntimeError):
                async with caller:
                    pass
        assert caller.stopped
        await caller.stopped
        with pytest.raises(RuntimeError):
            async with caller:
                pass

    async def test_current_pending(self, anyio_backend: Backend):
        async with Caller("manual") as caller:
            pen = caller.call_soon(Caller.current_pending)
            res = await pen
            assert res is pen

    async def test_closed_in_call_soon(self):
        async with Caller("manual") as caller:
            never_called_result = caller.call_later(10, anyio.sleep_forever)

        with pytest.raises(PendingCancelled):
            await never_called_result

    @pytest.mark.parametrize("mode", ["async", "direct"])
    @pytest.mark.parametrize("cancel_mode", ["local", "thread"])
    @pytest.mark.parametrize("msg", ["msg", None, "twice"])
    async def test_cancel(
        self, caller: Caller, mode: Literal["async", "direct"], cancel_mode: Literal["local", "thread"], msg
    ):
        ready = Event()
        proceed = Event()

        async def direct_func():
            ready.set()
            await proceed
            time.sleep(0.1)

        async def non_direct_func():
            ready.set()
            await anyio.sleep_forever()

        my_func = direct_func if mode == "direct" else non_direct_func

        pen = caller.call_soon(my_func)
        await ready
        proceed.set()
        if cancel_mode == "local":
            pen.cancel(msg)
            if msg == "twice":
                pen.cancel(msg)
                msg = f"{msg}(?s:.){msg}"
        else:

            def in_thread():
                proceed.set()
                time.sleep(0.01)
                pen.cancel(msg)

            caller.to_thread(in_thread)

        with pytest.raises(PendingCancelled, match=msg):
            await pen

    async def test_cancelled_waiter(self, caller: Caller):
        # Cancelling the waiter should also cancel call soon operation.
        pen = caller.call_soon(anyio.sleep_forever)
        with anyio.move_on_after(0.1):
            await pen
        with pytest.raises(PendingCancelled):
            pen.exception()

    async def test_cancelled_while_waiting(self, caller: Caller):
        async def async_func():
            with anyio.fail_after(0.01):
                await anyio.sleep_forever()

        pen = caller.call_soon(async_func)
        with pytest.raises(TimeoutError):
            await pen

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
        done, pending = await caller.wait(items, return_when=return_when)
        match return_when:
            case "FIRST_COMPLETED":
                assert {items[0]} == done
            case "FIRST_EXCEPTION":
                assert {*items[0:2]} == done
            case _:
                assert {*items} == done
                assert not pending

    async def test_cancelled_result(self, caller: Caller):
        pen = caller.call_soon(anyio.sleep_forever)
        pen_was_cancelled = caller.call_soon(pen.wait, result=False)
        await anyio.sleep(0.1)
        a = Event()
        weakref.finalize(a, pen.cancel)
        del a
        while not pen.done():
            gc.collect()
            await anyio.sleep(0)
        await pen_was_cancelled

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
        async with Caller("manual") as caller:
            # check can handle completed result okay first
            pen = caller.call_soon(lambda: 1 + 2)
            assert await pen.wait() == 3
            async for pen_ in caller.as_completed([pen]):
                assert pen_ is pen
            # work directly with iterator
            n_ = 0
            max_concurrent = caller.MAX_IDLE_POOL_INSTANCES if mode == "restricted" else n / 2
            async for pen in caller.as_completed(
                (caller.to_thread(func) for _ in range(n)), max_concurrent=max_concurrent
            ):
                assert pen.done()
                n_ += 1
                thread = await pen
                threads.add(thread)
            assert n_ == n
            if mode == "restricted":
                assert len(threads) == 2
            else:
                assert len(threads) > 2
            assert len(caller._worker_pool) == 2  # pyright: ignore[reportPrivateUsage]

    async def test_as_completed_error(self, caller: Caller):
        def func():
            raise RuntimeError()

        async for pen in caller.as_completed((caller.to_thread(func) for _ in range(6)), max_concurrent=4):
            with pytest.raises(RuntimeError):
                await pen

    async def test_as_completed_cancelled(self, anyio_backend: Backend):
        async with Caller("manual") as caller:
            n = 20
            ready = CountdownEvent(n)

            async def test_func():
                ready.down()
                if ready.value:
                    await anyio.sleep_forever()
                return ready

            items = {caller.to_thread(test_func) for _ in range(n)}
            with anyio.CancelScope() as scope:
                async for _ in caller.as_completed(items):
                    await ready
                    scope.cancel()
            for item in items:
                if not item.cancelled():
                    assert item.result() is ready
                else:
                    with pytest.raises(PendingCancelled):
                        await item
            await caller.wait(items, return_when="ALL_COMPLETED")

    async def test_as_completed_awaitables(self, caller: Caller):
        async def f(i: int):
            await anyio.sleep(i * 0.001)
            return i

        results = set()
        async for pen in caller.as_completed(f(i) for i in range(2)):
            results.add(await pen)
        assert results == {0, 1}

    async def test_wait_awaitables(self, caller: Caller):
        async def f(i: int):
            await anyio.sleep(i * 0.001)
            return i

        done, pending = await caller.wait(f(i) for i in range(2))
        assert not pending
        assert {pen.result() for pen in done} == {0, 1}

    async def test_worker_in_pool_shutdown(self, caller: Caller, mocker):
        pen1 = caller.to_thread(threading.current_thread)
        w1 = Caller.get_current(await pen1)
        assert w1
        assert w1 in caller._worker_pool  # pyright: ignore[reportPrivateUsage]
        w1.stop()
        pen2 = caller.to_thread(threading.current_thread)
        await w1.stopped
        assert w1 not in caller._worker_pool  # pyright: ignore[reportPrivateUsage]
        w2 = Caller.get_current(await pen2)
        assert w2
        assert not w2.stopped
        w2.stop()
        await w2.stopped
        assert not caller._worker_pool  # pyright: ignore[reportPrivateUsage]

    async def test_idle_worker_shutdown(self, caller: Caller, mocker):
        mocker.patch.object(Caller, "IDLE_WORKER_SHUTDOWN_DURATION", new=0.1)
        pen1 = caller.to_thread(threading.current_thread)
        w1 = Caller.get_current(await pen1)
        pen2 = caller.to_thread(threading.current_thread)
        w2 = Caller.get_current(await pen2)
        assert w1
        assert w2
        await w1.stopped
        await w2.stopped

    async def test_pending_group(self, caller: Caller):
        async with caller.create_pending_group() as pg:
            assert pg.caller.call_soon(lambda: None) in pg.pending
        assert not pg.pending
