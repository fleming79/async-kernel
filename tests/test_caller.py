import asyncio
import gc
import re
import sys
import threading
import time
import weakref
from random import random
from typing import Literal

import anyio
import anyio.lowlevel
import anyio.to_thread
import pytest
import trio
from aiologic import CountdownEvent, Event, Latch
from aiologic.lowlevel import create_async_event, create_async_waiter, current_async_library

from async_kernel.caller import Caller, StartStopTask
from async_kernel.pending import Pending, PendingCancelled
from async_kernel.typing import Backend, CallerState, Hosts

# pyright: reportPrivateUsage=false


@pytest.mark.anyio
class TestCaller:
    def test_thread_no_event_loop(self, anyio_backend: Backend):
        okay = False

        def thread_no_event_loop():
            nonlocal okay
            with pytest.raises(RuntimeError, match="unknown async library, or not in async context"):
                Caller()
            okay = True

        thread = threading.Thread(target=thread_no_event_loop)
        thread.start()
        thread.join()
        assert okay

    async def test_caller_main_thread_stopped(self, anyio_backend: Backend):
        ready = create_async_waiter()

        async def acquire_main_thread():
            async with caller:
                ready.wake()
                await caller.stopped

        caller = Caller()
        caller.call_soon(acquire_main_thread)
        await ready
        caller.stop(force=True)
        await caller.stopped

    async def test_child_lifecycle(self, anyio_backend: Backend):
        async with Caller() as caller:
            # worker thread
            assert caller.IDLE_WORKER_SHUTDOWN_DURATION == 0
            worker = await caller.to_thread(caller.get_existing)
            assert worker in caller.children
            # Child thread
            async with caller.get(name="c1") as c1:
                assert c1 in caller.children
                assert worker._state is CallerState.running
                assert caller._children == {worker, c1}
                assert caller.get(name="c1") is c1
                # A child's child
                c2 = c1.get(name="c2")
                assert c2 in c1.children
                assert c2 not in caller.children
                assert c1.get(name="c2") is c2
                assert Caller("MainThread") is caller
            assert c1.stopped.done()
            assert c2.stopped.done()
            assert not c1._children
            assert worker._state is CallerState.running
        assert worker.stopped.done()

    async def test_already_exists(self, caller: Caller):
        assert Caller.get_existing(caller.id)
        assert Caller("MainThread") is caller
        assert caller.thread is threading.main_thread()

    async def test_caller_autostop(self, anyio_backend: Backend):
        async def f():
            async with caller:
                await caller.call_soon(lambda: 1 + 2)
                async with caller:
                    pass

        async def ff():
            async with caller:
                await create_async_waiter()

        caller = Caller()
        pen = caller.call_soon(f)
        pen2 = caller.call_soon(ff)
        await pen
        assert caller.stopping.done()
        assert pen2.cancelled()
        await caller.stopped

    async def test_start_after(self, anyio_backend: Backend):
        caller = Caller()
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
            assert caller.id != Caller.CALLER_MAIN_THREAD_ID
            assert (await caller.call_soon(lambda: 1 + 1)) == 2

        thread = threading.Thread(target=anyio.run, args=[get_caller])
        thread.start()
        thread.join()

    def test_no_event_loop(self, anyio_backend: Backend):
        caller = Caller("NewThread", backend=anyio_backend, no_debug=True)
        assert caller.id != threading.get_ident()
        assert caller.call_soon(lambda: 2 + 2).wait_sync() == 4
        assert caller.thread.pydev_do_not_trace  # pyright: ignore[reportAttributeAccessIssue]
        caller.stop()

    async def test_call_later(self, anyio_backend: Backend):
        async with Caller() as caller:
            # We have retries because sleeping can be a bit flaky on CI
            for _ in range(10):
                start_time = time.monotonic()
                dt = await caller.call_later(0.1, time.monotonic) - start_time
                if dt >= 0.1:
                    return
            assert dt >= 0.1  # pyright: ignore[reportPossiblyUnboundVariable]

    async def test_wrong_backend(self, anyio_backend: Backend):
        wrong_backend = next(b for b in Backend if b != anyio_backend)
        async with Caller() as caller:
            caller.get(name="c1")
            with pytest.raises(RuntimeError, match="Backend mismatch!"):
                caller.get(name="c1", backend=wrong_backend)
            with pytest.raises(RuntimeError, match="Host mismatch!"):
                caller.get(name="c1", host=Hosts.tk)

    async def test_manual_stop(self):
        async with Caller() as caller:
            caller.stop()
        assert caller.stopped.done()

    async def test_call_returns_result(self, caller: Caller) -> None:
        pen = Pending()
        caller.call_direct(lambda: pen)
        assert await caller.call_soon(lambda: pen) is pen

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

    async def test_stopping(self, anyio_backend: Backend):
        caller = Caller("NewThread")
        caller.stop()
        with pytest.raises(RuntimeError):
            async with caller:
                pass
        assert caller.stopped.done()

    async def test_cancelled_async_context(self, anyio_backend: Backend):
        caller = None
        with anyio.CancelScope() as scope:
            async with Caller("NewThread") as caller:
                scope.cancel("Force cancellation")
                await anyio.sleep_forever()
        assert caller
        assert caller.started.done()
        assert caller.stopping.done()
        assert caller.stopped.done()

    async def test_protected(self, anyio_backend: Backend):
        async with Caller() as caller:
            assert caller.protected
            caller.stop()
            assert not caller.stopped.done()
        assert caller.stopped.done()

    @pytest.mark.parametrize("args_kwargs", argvalues=[((), {}), ((1, 2, 3), {"a": 10})])
    async def test_async(self, args_kwargs: tuple[tuple, dict]):
        val = None

        async def my_func(is_called: Event, *args, **kwargs):
            nonlocal val
            val = args, kwargs
            is_called.set()
            return args, kwargs

        async with Caller() as caller:
            is_called = Event()
            pen = caller.call_later(0.1, my_func, is_called, *args_kwargs[0], **args_kwargs[1])
            await is_called
            assert val == args_kwargs
            assert (await pen) == args_kwargs

    async def test_anyio_to_thread(self, anyio_backend: Backend):
        # Test the call works from an anyio thread
        async with Caller() as caller:
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
        async with Caller() as caller:
            child_1 = caller.get()
            child_2 = caller.get(name="asyncio backend", backend="asyncio")
            child_3 = caller.get(name="trio backend", backend="trio")
            assert caller.children == {child_1, child_2, child_3}
        assert not caller.children
        with pytest.raises(RuntimeError):
            caller.get()

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
        async with Caller() as caller:

            async def f():
                nonlocal is_cancelled
                started.set()
                try:
                    await anyio.sleep_forever()
                except anyio.get_cancelled_exc_class():
                    is_cancelled = True
                    raise

            started = Event()
            caller.call_soon(f)
            await started
        assert is_cancelled

    async def test_get_start_main_thread(self, anyio_backend: Backend):
        # Check a caller can be started in the main thread synchronously.
        caller = Caller()
        assert caller._state.value < CallerState.running.value, "needed for __repr__ early check"
        assert str(caller)
        assert await caller.call_soon(lambda: 1 + 1) == 2

    async def test_get_current_thread(self, anyio_backend: Backend):
        # Test starting in the async event loop of a non-main-thread
        ready, done = Event(), Event()
        caller: Caller = None  # pyright: ignore[reportAssignmentType]

        def caller_not_already_running():
            async def async_loop_before_caller_started():
                nonlocal caller
                caller = Caller()
                ready.set()
                await done
                caller.stop()
                await caller.stopped

            anyio.run(async_loop_before_caller_started, backend=anyio_backend)

        (t := threading.Thread(target=caller_not_already_running)).start()
        await ready
        assert caller
        assert (await caller.call_soon(lambda: 2 + 2)) == 4
        done.set()
        t.join()

    async def test_stop_early(self, anyio_backend: Backend):
        caller = Caller()
        caller.stop()
        await caller.stopped
        pen = caller.call_soon(lambda: None)
        assert pen.cancelled()
        assert pen.done()

    async def test_await_stopped(self, anyio_backend: Backend):
        caller = Caller()
        caller.call_soon(anyio.sleep_forever)
        assert await caller.call_soon(lambda: 1 + 1) == 2
        caller.stop()
        await caller.stopped

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
        caller.queue_call(func, okay.set)
        await okay

    async def test_execution_queue_from_thread(self, caller: Caller):
        event = Event()
        caller.to_thread(caller.queue_call, event.set)
        await event

    async def test_gc(self, anyio_backend: Backend):
        collected = Event()
        async with Caller() as caller:
            assert await caller.call_soon(lambda: 1 + 1) == 2
            weakref.finalize(caller, collected.set)
            del caller

        while not collected:
            gc.collect()
            await anyio.lowlevel.checkpoint()

    async def test_queue_cancel(self, caller: Caller):
        started = Event()

        async def test_func():
            started.set()
            await anyio.sleep_forever()

        caller.queue_call(test_func)
        pen = caller.queue_get(test_func)
        assert pen is not None
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
            await anyio.lowlevel.checkpoint()
        assert not any(caller._queue_map)

    async def test_call_early(self, anyio_backend: Backend) -> None:
        caller = Caller()
        pen = caller.call_soon(lambda: 3 + 3)
        assert not caller.running
        assert not pen.done()
        assert await pen == 6

    async def test_name_mismatch(self, caller: Caller):
        with pytest.raises(ValueError, match="The thread and caller's name do not match!"):
            Caller(name="wrong name")

    async def test_backend_mismatch(self, caller: Caller):
        wrong_backend = next(b for b in Backend if b != caller.backend)
        with pytest.raises(ValueError, match="The backend does not match!"):
            Caller(backend=wrong_backend)

    async def test_multi_entry(self, anyio_backend: Backend):
        ready, resume = create_async_waiter(), create_async_waiter()

        async def f():
            async with caller:
                ready.wake()
                await resume
                assert "waiting context 1 remain" in repr(caller)

        async with Caller() as caller:
            assert caller is Caller()
            async with caller:
                pass
            pen = caller.call_soon(f)
            await ready
            resume.wake()
        assert caller.stopped.done()
        assert pen.done()
        await pen
        await caller.stopped
        with pytest.raises(RuntimeError):
            async with caller:
                pass

    async def test_current_pending(self, anyio_backend: Backend):
        async with Caller() as caller:
            pen = caller.call_soon(Caller.current_pending)
            res = await pen
            assert res is pen

    async def test_closed_in_call_soon(self):
        async with Caller() as caller:
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
    @pytest.mark.parametrize("when", ["done", "soon"])
    async def test_wait(
        self,
        caller: Caller,
        return_when: Literal["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"],
        when: Literal["done", "soon"],
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

        if when == "soon":
            items = [caller.call_soon(f, i) for i in range(3)]
        else:
            items = [Pending(), Pending(), Pending()]
            items[0].set_result(None)
            if return_when in ("FIRST_EXCEPTION", "ALL_COMPLETED"):
                items[1].set_exception(RuntimeError())
            if return_when == "ALL_COMPLETED":
                items[2].set_result(None)

        done, pending = await caller.wait(items, return_when=return_when)
        match return_when:
            case "FIRST_COMPLETED":
                assert {items[0]} == done
            case "FIRST_EXCEPTION":
                assert {*items[0:2]} == done
            case _:
                assert {*items} == done
                assert not pending

    async def test_wait_awaitable(self, caller):
        done, pending = await caller.wait((anyio.lowlevel.checkpoint(),))
        assert not pending
        assert len(done) == 1
        assert isinstance(next(iter(done)), Pending)

    async def test_cancelled_result(self, caller: Caller):
        pen = caller.call_soon(anyio.sleep_forever)
        pen_was_cancelled = caller.call_soon(pen.wait, result=False)
        await anyio.sleep(0.1)
        a = Event()
        weakref.finalize(a, pen.cancel)
        del a
        while not pen.done():
            gc.collect()
            await anyio.lowlevel.checkpoint()
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
        async with Caller() as caller:
            # check can handle completed result okay first
            pen = caller.call_soon(lambda: 1 + 2)
            assert await pen.wait() == 3
            async for pen_ in caller.as_completed([pen]):
                assert pen_ is pen
            # work directly with iterator
            n_ = 0
            max_concurrent = caller.MAX_IDLE_POOL_INSTANCES if mode == "restricted" else n // 2
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
            assert len(caller._worker_pool) in [2, 3], "The pool should roughly adhere to max_concurrent restriction"

    async def test_as_completed_error(self, caller: Caller):
        def func():
            raise RuntimeError()

        async for pen in caller.as_completed((caller.to_thread(func) for _ in range(6)), max_concurrent=4):
            with pytest.raises(RuntimeError):
                await pen

    async def test_as_completed_cancelled(self, anyio_backend: Backend):
        async with Caller() as caller:
            n = 6
            barrier = Latch(n + 1)

            async def f(i):
                await barrier
                if i > n - 2:
                    await create_async_waiter()
                return "ok"

            items = {caller.to_thread(f, i) for i in range(n)}
            with anyio.CancelScope() as scope:
                await barrier
                async for _ in caller.as_completed(items):
                    scope.cancel()
            for item in items:
                if not item.cancelled():
                    assert item.result() == "ok"
                else:
                    assert item.cancelled()

    async def test_as_completed_awaitables(self, caller: Caller):
        async def f(i: int):
            await anyio.sleep(i * 0.001)
            return i

        results = set()
        async for pen in caller.as_completed(f(i) for i in range(2)):
            results.add(await pen)
        assert results == {0, 1}

    async def test_as_completed_current_pending_deadlock(self, caller: Caller):
        async def f():
            if pen := caller.current_pending():
                async for _ in caller.as_completed((pen,)):
                    pass

        with pytest.raises(RuntimeError, match="deadlock"):
            await caller.call_soon(f)

    async def test_as_completed_empty_iterator(self, caller: Caller) -> None:
        async for _ in caller.as_completed(iter(())):
            pass

    async def test_wait_awaitables(self, caller: Caller) -> None:
        async def f(i: int):
            await anyio.sleep(i * 0.001)
            return i

        done, pending = await caller.wait((caller.call_soon(f, 1), caller.to_thread(f, 2)))
        assert not pending
        assert {pen.result() for pen in done} == {1, 2}

    async def test_worker_in_pool_shutdown(self, caller: Caller):
        pen1 = caller.to_thread(threading.get_ident)
        w1 = Caller.get_existing(await pen1)
        assert w1
        assert w1 in caller._worker_pool
        w1.stop()
        await w1.stopped
        assert w1 not in caller._worker_pool

    async def test_worker_returned_to_pool(self, caller: Caller):
        caller_id = await caller.to_thread(threading.get_ident)
        w2 = Caller.get_existing(caller_id)
        assert w2
        assert not w2.stopped.done()
        assert w2 in caller._worker_pool
        w2.stop()
        await w2.stopped
        assert not caller._worker_pool

    async def test_idle_worker_shutdown(self, caller: Caller, mocker):
        resume = create_async_event()

        async def controlled_sleep(*args, sleep=anyio.sleep):
            await resume
            await sleep(*args)

        mocker.patch.object(anyio, "sleep", new=controlled_sleep)
        mocker.patch.object(Caller, "IDLE_WORKER_SHUTDOWN_DURATION", new=0.001)
        pen1 = caller.to_thread(Caller.get_existing)
        w1 = await pen1
        assert w1 in caller._worker_pool
        resume.set()
        await w1.stopped

    async def test_worker_in_pool_stopping(self, caller: Caller):
        worker = await caller.to_thread(caller.get_existing)
        assert worker
        worker.stopping.set_result(None)
        assert await caller.to_thread(lambda: 1 + 1) == 2

    async def test_pending_group(self, caller: Caller):
        async with caller.create_pending_group() as pg:
            assert pg.caller.call_soon(lambda: None) in pg.pending
        assert not pg.pending

    async def test_to_thread_emscripten(self, caller: Caller, mocker):
        mocker.patch.object(sys, "platform", new="emscripten")
        caller2 = await caller.to_thread(Caller)
        assert caller2 is not caller
        assert caller2.id != caller.id
        caller2.stop()
        await caller2.stopped

    @pytest.mark.parametrize("mode", ["sync", "async"])
    async def test_balanced(self, caller: Caller, mode: Literal["sync", "async"], anyio_backend):
        def sync_func(pen: Pending, value):
            pen.set_result(value)

        async def async_func(pen: Pending, value):
            await anyio.lowlevel.checkpoint()
            pen.set_result(value)

        func = sync_func if mode == "sync" else async_func

        n = 1000
        all_pending = []
        for _ in range(n):
            for method in (caller.call_direct, caller.queue_call, caller.call_soon):
                pen = Pending()
                method(func, pen, method.__name__)
                all_pending.append(pen)
        results = [pen.result() async for pen in caller.as_completed(all_pending)]

        assert results == ["call_direct", "queue_call", "call_soon"] * n

    async def test_call_soon_with_backend(self):
        async with Caller() as caller:
            opposite = next(b for b in Backend if b is not caller.backend)

            async def check_backend(backend: Backend, fail=False):
                assert current_async_library() == backend
                if backend is Backend.asyncio:
                    await asyncio.sleep(0.01)
                else:
                    await trio.sleep(0.01)
                if fail:
                    raise RuntimeError

            await check_backend(caller.backend)
            await caller.call_using_backend(caller.backend, check_backend, caller.backend)
            await caller.call_using_backend(opposite, check_backend, opposite)
            with pytest.raises(RuntimeError):
                await caller.call_using_backend(opposite, check_backend, opposite, fail=True)

    async def test_call_soon_with_backend_pending_group(self, caller: Caller):
        opposite = next(b for b in Backend if b is not caller.backend)
        assert caller.backend is not opposite
        async with caller.create_pending_group() as pg:
            for _ in range(3):
                pen = caller.call_using_backend(opposite, current_async_library)
                assert pen in pg.pending
            pending = pg.pending
        for pen in pending:
            assert pen.result() == opposite

    async def test_call_soon_with_backend_cancel(self, anyio_backend):
        async with Caller() as caller:
            opposite = next(b for b in Backend if b is not caller.backend)
            assert await caller.call_using_backend(opposite, lambda: 1 + 1) == 2
            pen = caller.call_using_backend(opposite, lambda: 1 + 1)
            caller.stop()
        assert pen.cancelled()

    async def test_caller_with_host(self, anyio_backend: Backend):

        from .test_event_loop import AsyncioHost, TrioHost  # noqa: PLC0415

        cls = AsyncioHost if anyio_backend == Backend.trio else TrioHost
        caller = Caller("NewThread", host=Hosts.custom, backend=anyio_backend, host_options={"host_class": cls})
        assert caller.host is Hosts.custom
        assert await caller.call_soon(lambda: 1 + 1) == 2
        caller.stop()
        await caller.stopped

    async def test_loop_factory(self, anyio_backend: Backend):
        caller = Caller("NewThread", backend="asyncio", backend_options={"loop_factory": "asyncio.new_event_loop"})
        assert await caller.call_soon(lambda: 1 + 1) == 2
        caller.stop()
        await caller.stopped


@pytest.mark.parametrize("backend", Backend)
def test_unmanged_shutdown(backend: Backend):
    assert not Caller._instances

    async def f():
        await Caller().to_thread(lambda: 1 + 1)
        Caller().to_thread(lambda: 1 + 1)

    anyio.run(f, backend=str(backend))
    assert not list(Caller._instances)


@pytest.mark.parametrize("backend", Backend)
def test_guest_non_protected(backend: Backend):
    opposite = next(b for b in Backend if b is not backend)

    async def f():
        with pytest.raises(RuntimeError, match="Async context must be acquired prior to using a guest backend!"):
            await Caller().call_using_backend(opposite, lambda: 1 + 1)

    anyio.run(f, backend=str(backend))


class TestStartStopTask:
    async def test_NotSet(self, anyio_backend: Backend):
        task = StartStopTask()
        with pytest.raises(RuntimeError, match="`start` must be called before entering the context!"):
            async with task:
                pass
        with pytest.raises(RuntimeError, match="`start` must be called before entering the context!"):
            await task

    async def test_sync(self, anyio_backend: Backend) -> None:

        caller = Caller()
        resume = create_async_event()

        async def f(started, stop):
            started()
            await stop
            await resume
            return "ok"

        task = caller.create_start_stop_task(f).start()
        await task.started
        caller.stop(force=True)
        assert task.stopping.done()
        resume.set()
        assert await task.stopped == "ok"
        assert await task == "ok"

        with pytest.raises(PendingCancelled, match="Stopped early!"):
            await caller.create_start_stop_task(f).stop()
        with pytest.raises(RuntimeError, match="can only be set once"):
            task.set_task_function(f)

    async def test_asyncontext(self, caller: Caller) -> None:

        async def f(started, stop):
            started()
            await stop
            await anyio.sleep(0)
            return "ok"

        task = caller.create_start_stop_task(f)
        with pytest.raises(RuntimeError, match="must be called before entering the context"):
            async with task:
                pass
        assert not task.started.done()
        async with task.start() as task:
            assert task.started.done()
            # Exiting the context should initial shutdown.
        assert task.stopped.result() == "ok"
        assert await task == "ok"
        with pytest.raises(RuntimeError, match="The async context can only be used once"):
            async with task:
                await create_async_event()

    async def test_asyncontext_stop_early(self, caller: Caller) -> None:

        async def f(started, stop):
            started()

        task = caller.create_start_stop_task(f)
        with pytest.raises(RuntimeError, match="stopped early"):
            async with task.start():
                await create_async_waiter()

    async def test_gc(self, caller: Caller):

        class MyClass:
            async def f(self, started, stop):
                started()
                await stop

        cleaned = CountdownEvent(2)
        c = MyClass()
        task = caller.create_start_stop_task(c.f)
        ref = weakref.ref(c)
        ref2 = weakref.ref(task)
        weakref.finalize(c, cleaned.down)
        weakref.finalize(task, cleaned.down)
        async with task.start():
            pass
        del c, task
        with anyio.move_on_after(2):
            await cleaned

        if ref():
            referrers = gc.get_referrers(ref())
            assert not referrers
        if ref2():
            referrers = gc.get_referrers(ref2())
            assert not referrers

    async def test_task_cancelled(self, caller: Caller):
        async def f(started, stop):
            pen = Caller.current_pending()
            assert pen
            pen.cancel("Stop")

        task = caller.create_start_stop_task(f)
        with pytest.raises(PendingCancelled, match="cancelled"):
            await task.start()

    async def test_task_error(self, caller: Caller):
        async def f(started, stop):
            msg = "This failed"
            raise RuntimeError(msg)

        task = caller.create_start_stop_task(f)
        with pytest.raises(RuntimeError, match="This failed"):
            await task.start()

        async def check_non_caller():
            with pytest.raises(RuntimeError, match="can only be used by the same caller"):
                async with task:
                    raise RuntimeError

        await caller.to_thread(check_non_caller)

    def test_no_func(self):
        with pytest.raises(RuntimeError, match="task function has not been set"):
            StartStopTask().start()
