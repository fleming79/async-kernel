# from https://gist.github.com/x42005e1f/857dcc8b6865a11f1ffc7767bb602779 (56th revision)

# SPDX-FileCopyrightText: 2026 Ilya Egorov <0x42005e1f@gmail.com>
# SPDX-License-Identifier: ISC

# requires-python = ">=3.8"
# dependencies = [
#     "outcome>=1.0.0",
#     "sniffio>=1.3.0",
#     "wrapt>=2.0.0",
# ]

import asyncio
import sys

from concurrent.futures import ThreadPoolExecutor

import outcome

from sniffio import thread_local as current_async_library_tlocal
from wrapt import AutoObjectProxy

_MAXIMUM_SELECT_TIMEOUT = 24 * 60 * 60
_THREAD_JOIN_TIMEOUT = 5 * 60


class _GuestSelector(AutoObjectProxy):
    __slots__ = "_guest_selector_future"

    def select(self, /, timeout=None):
        if self._guest_selector_future is not None:
            try:
                return self._guest_selector_future.result()
            finally:
                self._guest_selector_future = None  # break reference cycles

        return self.__wrapped__.select(timeout)


def _patch_loop(loop, /):
    loop_selector = loop._selector = _GuestSelector(loop._selector)
    loop_selector._guest_selector_future = None
    if getattr(loop, "_proactor", None) is loop_selector.__wrapped__:
        loop._proactor = loop_selector  # Windows

    return loop


# see asyncio.base_events.BaseEventLoop._run_once
def _compute_nearest_timeout(loop, /):
    if loop._ready:
        return 0
    if not loop._scheduled:
        return None

    handle = loop._scheduled[0]
    if handle.cancelled():
        return 0

    timeout = handle.when() - loop.time()
    if timeout < 0:
        return 0
    if timeout > _MAXIMUM_SELECT_TIMEOUT:
        return _MAXIMUM_SELECT_TIMEOUT

    return timeout


def _run_until_complete(loop, executor, future, /):
    selector = loop._selector
    select = selector.__wrapped__.select
    future = asyncio.ensure_future(future, loop=loop)

    while True:
        host_library = current_async_library_tlocal.name
        host_loop = asyncio._get_running_loop()
        try:
            asyncio.set_event_loop(None)
            asyncio._set_running_loop(None)
            current_async_library_tlocal.name = "asyncio"

            done = future.done()
            loop.stop()
            loop.run_forever()
            if done:
                break
            if future.done():
                timeout = 0
            else:
                timeout = _compute_nearest_timeout(loop)
        finally:
            current_async_library_tlocal.name = host_library
            asyncio._set_running_loop(host_loop)
            asyncio.set_event_loop(host_loop)

        if timeout is None or timeout:  # timeout is non-zero
            selector._guest_selector_future = executor.submit(select, timeout)

            yield selector._guest_selector_future

            assert selector._guest_selector_future.done()
        else:  # timeout is zero
            yield None


# see asyncio.runners._cancel_all_tasks
def _cancel_all_tasks(loop, executor, /):
    to_cancel = asyncio.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    # see python/cpython#124640
    async def wait_for_cancelled():
        return await asyncio.gather(*to_cancel, return_exceptions=True)

    yield from _run_until_complete(loop, executor, wait_for_cancelled())

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is None:
            continue

        loop.call_exception_handler({
            "message": "unhandled exception during start_guest_run() shutdown",
            "exception": task.exception(),
            "task": task,
        })


def _run(loop, executor, future, /):
    try:
        yield from _run_until_complete(loop, executor, future)
    finally:  # see asyncio.runners.Runner.close
        try:
            yield from _cancel_all_tasks(loop, executor)
            yield from _run_until_complete(
                loop,
                executor,
                loop.shutdown_asyncgens(),
            )
            if sys.version_info >= (3, 12):
                yield from _run_until_complete(
                    loop,
                    executor,
                    loop.shutdown_default_executor(_THREAD_JOIN_TIMEOUT),
                )
            elif sys.version_info >= (3, 9):
                yield from _run_until_complete(
                    loop,
                    executor,
                    loop.shutdown_default_executor(),
                )
        finally:
            try:
                executor.shutdown(wait=False)
            finally:
                loop.close()


def start_guest_run(
    async_fn,
    /,
    *args,
    done_callback,
    run_sync_soon_threadsafe,
    run_sync_soon_not_threadsafe=None,
    host_uses_signal_set_wakeup_fd=False,  # ignored
    loop_factory=None,
    task_factory=None,
    context=None,
    debug=None,
):
    async def wrapper(*args):
        return await async_fn(*args)

    if run_sync_soon_not_threadsafe is None:
        run_sync_soon_not_threadsafe = run_sync_soon_threadsafe

    if loop_factory is None:
        if sys.version_info >= (3, 13):
            loop_factory = asyncio.EventLoop
        elif sys.platform == "win32":
            loop_factory = asyncio.ProactorEventLoop
        else:
            loop_factory = asyncio.SelectorEventLoop

    guest_loop = _patch_loop(loop_factory())
    if task_factory is not None:
        guest_loop.set_task_factory(task_factory)
    if debug is not None:
        guest_loop.set_debug(debug)

    guest_executor = ThreadPoolExecutor(1, thread_name_prefix="asyncio-guest")

    if context is None:
        guest_task = guest_loop.create_task(wrapper(*args))
    elif sys.version_info >= (3, 11):
        guest_task = guest_loop.create_task(wrapper(*args), context=context)
    else:
        guest_task = context.run(guest_loop.create_task, wrapper(*args))

    guest_run = _run(guest_loop, guest_executor, guest_task)

    def guest_callback(future):
        run_sync_soon_threadsafe(host_callback)

    def host_callback():
        try:
            future = next(guest_run)
        except StopIteration:  # completed
            def guest_shutdown_callback():
                done_callback(outcome.capture(guest_task.result))

            run_sync_soon_not_threadsafe(guest_shutdown_callback)
        except BaseException as exc:  # failed
            message = "The event loop has been closed prematurely"
            exception = RuntimeError(message)
            exception.__cause__ = exc
            if guest_task.done() and not guest_task.cancelled():
                exception.__context__ = guest_task.exception()

            def guest_shutdown_callback():
                nonlocal exception
                try:
                    done_callback(outcome.Error(exception))
                finally:
                    del exception  # break reference cycles

            run_sync_soon_not_threadsafe(guest_shutdown_callback)
        else:  # running
            if future is None:  # timeout is zero
                run_sync_soon_not_threadsafe(host_callback)
            else:  # timeout is non-zero
                future.add_done_callback(guest_callback)

    run_sync_soon_threadsafe(host_callback)

    return guest_loop
