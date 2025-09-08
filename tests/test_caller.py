import contextlib
import importlib.util
import inspect
import threading
import time
import weakref
from contextvars import ContextVar
from random import random
from typing import Literal, cast

import anyio
import anyio.to_thread
import pytest
import sniffio
from anyio.abc import TaskStatus

from async_kernel.caller import Caller, Future, FutureCancelledError, InvalidStateError, Lock
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
        after_done = anyio.Event()

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

    async def test_set_cancel_scope_twice_raises(self):
        fut = Future()
        with anyio.CancelScope() as cancel_scope:
            fut.set_cancel_scope(cancel_scope)
            with pytest.raises(InvalidStateError):
                fut.set_cancel_scope(cancel_scope)

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
        fut = Future(thread=threading.Thread())
        with pytest.raises(RuntimeError):
            fut.set_result(None)

    async def test_set_from_non_thread(self, anyio_backend):
        caller = Caller.start_new(backend=anyio_backend)
        try:
            fut = Future(thread=caller.thread)
            assert fut.thread is not threading.current_thread()
            fut.set_result(value=123)
            assert (await fut) == 123
        finally:
            caller.stop()

    async def test_wait_cancelled_shield(self, anyio_backend):
        fut = Future()
        with pytest.raises(TimeoutError):
            await fut.wait(timeout=0.001, shield=True)
        assert not fut.cancelled()
        with pytest.raises(TimeoutError):
            await fut.wait(timeout=0.001)
        assert fut.cancelled()

    async def test_wait_sync_cancelled(self, anyio_backend):
        fut = Caller.to_thread(anyio.sleep_forever)
        with pytest.raises(TimeoutError):
            fut.wait_sync(timeout=0.001, shield=True)
        assert not fut.cancelled()
        with pytest.raises(TimeoutError):
            fut.wait_sync(timeout=0.001)
        assert fut.cancelled()


@pytest.mark.anyio
class TestCaller:
    def setup_method(self, test_method):
        Caller.stop_all()

    def teardown_method(self, test_method):
        Caller.stop_all()

    async def test_sync(self):
        async with Caller(create=True) as caller:
            is_called = anyio.Event()
            caller.call_later(0.01, is_called.set)
            await is_called.wait()

    def test_no_thread(self):
        with pytest.raises(RuntimeError):
            Caller()

    async def test_protected(self, anyio_backend):
        caller = Caller(create=True, protected=True)
        caller.stop()
        assert not caller.stopped
        caller.stop(force=True)

    def test_no_backend_error(self, anyio_backend):
        with pytest.raises(RuntimeError):
            Caller(create=True)

    @pytest.mark.parametrize("args_kwargs", [((), {}), ((1, 2, 3), {"a": 10})])
    async def test_async(self, args_kwargs: tuple[tuple, dict]):
        val = None

        async def my_func(is_called: anyio.Event, *args, **kwargs):
            nonlocal val
            val = args, kwargs
            is_called.set()
            return args, kwargs

        async with Caller(create=True) as caller:
            is_called = anyio.Event()
            fut = caller.call_later(0.1, my_func, is_called, *args_kwargs[0], **args_kwargs[1])
            await is_called.wait()
            assert val == args_kwargs
            assert (await fut) == args_kwargs

    async def test_anyio_to_thread(self):
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

            started = anyio.Event()
            caller.call_later(0.01, my_test)
            await started.wait()
        assert is_cancelled

    @pytest.mark.parametrize("check_result", ["result", "exception"])
    @pytest.mark.parametrize("check_mode", ["main", "local", "asyncio", "trio", "wait_sync"])
    async def test_wait_from_threads(self, anyio_backend, check_mode: str, check_result: str):
        finished_event = cast("anyio.Event", object)
        ready = threading.Event()

        def _thread_task():
            nonlocal the_thread
            nonlocal finished_event
            finished_event = anyio.Event()

            async def _run():
                async with Caller(create=True):
                    ready.set()
                    await finished_event.wait()

            anyio.run(_run, backend=anyio_backend)

        the_thread = threading.Thread(target=_thread_task, daemon=True)
        the_thread.start()
        ready.wait()
        assert isinstance(finished_event, anyio.Event)
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
                case "wait_sync":
                    assert fut.wait_sync() == 10
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

    async def test_wait_sync_error(self, caller: Caller):
        fut = caller.call_later(0, anyio.sleep, 0.1)
        with pytest.raises(RuntimeError):
            fut.wait_sync()

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

    async def test_as_completed_cancelled(self, anyio_backend):
        items = {Caller.to_thread(anyio.sleep, 100) for _ in range(4)}
        async with Caller(create=True):

            async def cancelled(task_status: TaskStatus[None]):
                with pytest.raises(anyio.get_cancelled_exc_class()):  # noqa: PT012
                    task_status.started()
                    async for _ in Caller.as_completed(items):
                        pass

            async with anyio.create_task_group() as tg:
                await tg.start(cancelled)
                tg.cancel_scope.cancel()
            for item in items:
                with pytest.raises(FutureCancelledError):
                    await item

    async def test__check_in_thread(self, anyio_backend):
        Caller.to_thread(anyio.sleep, 0.1)
        worker = next(iter(Caller.all_callers()))
        assert not worker.protected
        with pytest.raises(RuntimeError):
            worker._check_in_thread()  # pyright: ignore[reportPrivateUsage]

    async def test_execution_queue(self, caller: Caller):
        delay = 0.01
        N = 10

        pool = list(range(N))
        results = []

        async def func(a, b, results=results):
            await anyio.sleep(delay)
            results.append(b)

        for i in range(3):
            for j in pool:
                buff = i * N + 1
                if waiter := caller.queue_call(func, 0, j, wait=j >= buff, max_buffer_size=buff):
                    await waiter  # pyright: ignore[reportGeneralTypeIssues]
            assert caller.queue_exists(func)
            assert results != pool
            caller.queue_close(func)
            assert not caller.queue_exists(func)

    async def test_gc(self, anyio_backend):
        event_finalize_called = anyio.Event()
        async with Caller(create=True) as caller:
            weakref.finalize(caller, event_finalize_called.set)
            del caller
        await event_finalize_called.wait()

    async def test_execution_queue_gc(self, anyio_backend):
        class MyObj:
            async def method(self):
                method_called.set()

        obj_finalized = anyio.Event()
        method_called = anyio.Event()
        async with Caller(create=True) as caller:
            obj = MyObj()
            weakref.finalize(obj, obj_finalized.set)
            caller.queue_call(obj.method)
            await method_called.wait()
            assert caller.queue_exists(obj.method), "A ref should be retained unless it is explicitly removed"
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

    async def test_call_coroutine(self, caller: Caller):
        # Test we can await a coroutine, note that it is not permitted with the type hints,
        # but should probably be discouraged anyway since there is no way of knowing
        # (with type hints) if a coroutine has already been awaited.
        my_contextvar = ContextVar[int]("my_contextvar")
        my_contextvar.set(1)

        async def my_func():
            await anyio.sleep(0)
            assert my_contextvar.get() == 1
            return True

        # Discouraged
        fut = Caller.to_thread(my_func())  # pyright: ignore[reportCallIssue, reportArgumentType]
        val = await fut
        assert val is True
        # This the preferred way of calling.
        fut = Caller.to_thread(my_func)
        val = await fut
        assert val is True

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
        def blocking_func():
            import time  # noqa: PLC0415

            time.sleep(0.1)

        my_func = blocking_func
        match mode:
            case "async":
                my_func = anyio.sleep_forever
            case "blocking":
                my_func = blocking_func

        fut = caller.call_soon(my_func)
        if cancel_mode == "local":
            fut.cancel(msg)
            if msg == "twice":
                fut.cancel(msg)
                msg = f"{msg}(?s:.){msg}"
        else:
            caller.to_thread(fut.cancel, msg)

        with pytest.raises(FutureCancelledError, match=msg):
            await fut

    async def test_cancelled_waiter(self, caller: Caller):
        # Cancelling the waiter should also cancel call soon operation.
        async def async_func():
            await anyio.sleep(10)
            raise RuntimeError

        async with anyio.create_task_group() as tg:
            fut = caller.call_soon(async_func)
            tg.start_soon(fut.wait)
            await anyio.sleep(0)
            tg.cancel_scope.cancel()
        await anyio.sleep(0)
        with pytest.raises(FutureCancelledError):
            fut.exception()  # pyright: ignore[reportPossiblyUnboundVariable]

    @pytest.mark.parametrize("return_when", ["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"])
    async def test_wait(self, caller: Caller, return_when):
        waiters = [anyio.Event() for _ in range(4)]
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


class TestLock:
    async def test_basic_non_reentrant(self, caller: Caller):
        lock = Lock()

        async def func():
            assert lock.locked()
            async with lock:
                assert lock._count == 1  # pyright: ignore[reportPrivateUsage]
                return True

        async with lock:
            assert lock.locked()
            fut = caller.call_soon(func)
            with anyio.move_on_after(0.01):
                async with lock:
                    raise RuntimeError
        assert await fut
        assert not lock.locked()

    async def test_basic_reentrant(self, caller: Caller):
        async with Lock(reentrant=True) as lock:
            assert lock.locked()
            async with lock:
                assert lock.locked()
                assert lock._count == 2  # pyright: ignore[reportPrivateUsage]
        assert lock._count == 0  # pyright: ignore[reportPrivateUsage]
        assert not lock.locked()

    async def test_reentrant_thread(self, caller: Caller):
        lock = Lock(reentrant=True)
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

    async def test_basic_reentran_release(self, caller: Caller):
        lock = Lock(reentrant=True)
        futures = set()

        async def using_lock(haslock: anyio.Event, release: anyio.Event):
            async with lock:
                if len(futures) < 10:
                    futures.add(caller.call_soon(using_lock, haslock, release))
                await anyio.sleep(0.1)
                haslock.set()
                await release.wait()

        l1, r1 = anyio.Event(), anyio.Event()
        l2, r2 = anyio.Event(), anyio.Event()

        caller.call_soon(using_lock, l1, r1)
        caller.call_soon(using_lock, l2, r2)
        await l1.wait()
        assert not l2.is_set()
        r1.set()
        await l2.wait()
        r2.set()
        _, pend = await Caller.wait(futures, timeout=0.1)
        assert len(pend) == 0