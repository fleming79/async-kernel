# from https://gist.github.com/x42005e1f/857dcc8b6865a11f1ffc7767bb602779/62da6a864aeab89cfbf1756005880c7a06d78abb (51st revision)

# SPDX-FileCopyrightText: 2026 Ilya Egorov <0x42005e1f@gmail.com>
# SPDX-License-Identifier: ISC

# requires-python = ">=3.8"
# dependencies = [
#     "outcome>=1.0.0",
#     "sniffio>=1.3.0",
#     "taskgroup>=0.2.2; python_version<'3.11'",
#     "wrapt>=2.0.0",
# ]

import asyncio
import sys

from concurrent.futures import ThreadPoolExecutor

import outcome

from sniffio import thread_local as current_async_library_tlocal
from wrapt import AutoObjectProxy

if sys.version_info >= (3, 11):
    from asyncio import Runner
else:
    from taskgroup import Runner

MAXIMUM_SELECT_TIMEOUT = 24 * 60 * 60  # one day


class GuestSelector(AutoObjectProxy):
    __slots__ = "_guest_selector_future"

    def select(self, /, timeout=None):
        if self._guest_selector_future is not None:
            try:
                assert self._guest_selector_future.done()
                return self._guest_selector_future.result()
            finally:
                self._guest_selector_future = None  # break reference cycles

        return self.__wrapped__.select(timeout)


def _compute_nearest_timeout(loop):
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

    if timeout > MAXIMUM_SELECT_TIMEOUT:
        return MAXIMUM_SELECT_TIMEOUT

    return timeout


def start_guest_run(
    async_fn,
    /,
    *args,  # to `async_fn()`
    done_callback,
    run_sync_soon_threadsafe,
    run_sync_soon_not_threadsafe=None,
    host_uses_signal_set_wakeup_fd=False,  # ignored
    **kwargs,  # to `asyncio.Runner`
):
    if sys.version_info >= (3, 13):
        kwargs.setdefault("loop_factory", asyncio.EventLoop)
    elif sys.platform == "win32":
        kwargs.setdefault("loop_factory", asyncio.ProactorEventLoop)
    else:
        kwargs.setdefault("loop_factory", asyncio.SelectorEventLoop)

    if run_sync_soon_not_threadsafe is None:
        run_sync_soon_not_threadsafe = run_sync_soon_threadsafe

    async def wrapper(*args):
        return await async_fn(*args)

    guest_runner = Runner(**kwargs)
    guest_loop = guest_runner.get_loop()
    guest_selector = guest_loop._selector = GuestSelector(guest_loop._selector)
    guest_selector._guest_selector_future = None
    guest_executor = ThreadPoolExecutor(1, thread_name_prefix="asyncio-guest")
    guest_task = guest_loop.create_task(wrapper(*args))

    def guest_callback(future=None):
        run_sync_soon_threadsafe(host_callback)

    def host_callback():
        host_library = current_async_library_tlocal.name
        host_loop = asyncio._get_running_loop()

        try:
            asyncio.set_event_loop(None)
            asyncio._set_running_loop(None)
            current_async_library_tlocal.name = "asyncio"

            try:
                guest_done = guest_task.done()
                guest_loop.stop()
                guest_loop.run_forever()
                if guest_task.done():
                    timeout = 0
                else:
                    timeout = _compute_nearest_timeout(guest_loop)
            except BaseException:
                guest_done = True
                raise
            finally:
                if guest_done:
                    try:
                        guest_executor.shutdown(wait=False)
                    finally:
                        guest_runner.close()
        except BaseException as exc:
            message = "the event loop has been closed prematurely"
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
            return
        finally:
            current_async_library_tlocal.name = host_library
            asyncio._set_running_loop(host_loop)
            asyncio.set_event_loop(host_loop)

        if guest_done:
            def guest_shutdown_callback():
                done_callback(outcome.capture(guest_task.result))

            run_sync_soon_not_threadsafe(guest_shutdown_callback)
            return

        if timeout == 0:
            guest_callback()
            return

        guest_selector._guest_selector_future = guest_executor.submit(
            guest_selector.__wrapped__.select,
            timeout,
        )
        guest_selector._guest_selector_future.add_done_callback(guest_callback)

    run_sync_soon_threadsafe(host_callback)

    return guest_loop
