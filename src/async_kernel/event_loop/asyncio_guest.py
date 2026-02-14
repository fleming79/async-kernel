# from https://gist.github.com/x42005e1f/857dcc8b6865a11f1ffc7767bb602779/ef332565c9322623cc5bc7b62af9e84f98e4c374 (46th revision)

#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Ilya Egorov <0x42005e1f@gmail.com>
# SPDX-License-Identifier: ISC

# requires-python = ">=3.8"
# dependencies = [
#     "greenlet>=1.0.0",
#     "outcome>=1.0.0",
#     "sniffio>=1.3.0",
#     "wrapt>=2.0.0",
# ]

import asyncio
import sys

from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context

import outcome

from greenlet import getcurrent as current_greenlet, greenlet
from sniffio import thread_local as current_async_library_tlocal
from wrapt import AutoObjectProxy

# The greenlet may be an instance of a subclass that overrides the `switch()`
# method in an incompatible way (an example of this is the gevent hub), so we
# have to use the original one.
gr_switch = greenlet.switch


class ThreadedSelector(AutoObjectProxy):
    __slots__ = "__executor"

    def select(self, /, timeout=None):
        if timeout is None or timeout:  # timeout is non-zero
            try:
                executor = self.__executor
            except AttributeError:
                self.__executor = executor = ThreadPoolExecutor(1)

            future = executor.submit(self.__wrapped__.select, timeout)

            if (parent := current_greenlet().parent) is not None:
                gr_switch(parent, future)

            return future.result()

        result = self.__wrapped__.select(timeout)

        if (parent := current_greenlet().parent) is not None:
            gr_switch(parent, None)

        return result

    def close(self, /):
        try:
            self.__wrapped__.close()
        finally:
            try:
                executor = self.__executor
            except AttributeError:
                pass
            else:
                executor.shutdown()


def patch_loop(loop, /, *, host_uses_signal_set_wakeup_fd=False):
    loop_selector = loop._selector = ThreadedSelector(loop._selector)
    if getattr(loop, "_proactor", None) is loop_selector.__wrapped__:
        loop._proactor = loop_selector  # Windows

    if host_uses_signal_set_wakeup_fd:
        # The event loop methods related to Unix signals use
        # `signal.set_wakeup_fd()` to wake up the event loop when any signal is
        # received. However, the host event loop can also do this, which can
        # lead to hard-to-detect problems (hangs). Therefore, we prohibit these
        # methods when we know this is the case.

        loop_close = loop.close

        def add_signal_handler(self, /, sig, callback, *args):
            msg = "signal handlers cannot be set in guest mode"
            raise RuntimeError(msg)

        def remove_signal_handler(self, /, sig):
            msg = "signal handlers cannot be set in guest mode"
            raise RuntimeError(msg)

        def close(self, /):
            # to break reference cycles
            del self.add_signal_handler
            del self.remove_signal_handler
            del self.close

            loop_close()

        loop.add_signal_handler = add_signal_handler.__get__(loop)
        loop.remove_signal_handler = remove_signal_handler.__get__(loop)
        loop.close = close.__get__(loop)

    return loop


def start_guest_run(
    async_fn,
    /,
    *args,  # to `async_fn()`
    done_callback,
    run_sync_soon_threadsafe,
    run_sync_soon_not_threadsafe=None,
    host_uses_signal_set_wakeup_fd=False,
    **kwargs,  # to `asyncio.run()`
):
    if sys.version_info >= (3, 13):
        kwargs.setdefault("loop_factory", asyncio.EventLoop)
    elif sys.version_info >= (3, 11):
        if sys.platform == "win32":
            kwargs.setdefault("loop_factory", asyncio.ProactorEventLoop)
        else:
            kwargs.setdefault("loop_factory", asyncio.SelectorEventLoop)
    elif "trio_asyncio" in sys.modules:
        # When trio-asyncio is imported, the default event loop will
        # potentially be `trio_asyncio._sync.SyncTrioEventLoop`, which is not
        # compatible with our implementation. Therefore, in this case, we run
        # Trio in guest mode with trio-asyncio on top of it. But note that this
        # is not compatible with already scheduled Trio runs in the current
        # thread!

        import trio
        import trio_asyncio

        async def wrapper(*args):
            async with trio_asyncio.open_loop():
                return await trio_asyncio.aio_as_trio(async_fn(*args))

        # `kwargs` are ignored
        trio.lowlevel.start_guest_run(
            wrapper,
            *args,
            done_callback=done_callback,
            run_sync_soon_threadsafe=run_sync_soon_threadsafe,
            run_sync_soon_not_threadsafe=run_sync_soon_not_threadsafe,
            host_uses_signal_set_wakeup_fd=host_uses_signal_set_wakeup_fd,
        )
        return

    if run_sync_soon_not_threadsafe is None:
        run_sync_soon_not_threadsafe = run_sync_soon_threadsafe

    if sys.version_info >= (3, 11):
        def run(main, **kwargs):
            with asyncio.Runner(**kwargs) as runner:
                patch_loop(
                    runner.get_loop(),
                    host_uses_signal_set_wakeup_fd=(
                        host_uses_signal_set_wakeup_fd
                    ),
                )

                return runner.run(main)

        wrapper = async_fn
    else:
        run = asyncio.run

        async def wrapper(*args):
            patch_loop(
                asyncio.get_running_loop(),
                host_uses_signal_set_wakeup_fd=(
                    host_uses_signal_set_wakeup_fd
                ),
            )

            return await async_fn(*args)

    gr = greenlet(outcome.capture)
    gr.gr_context = copy_context()
    gr_library = "asyncio"
    gr_loop = None

    def executor_callback(future):
        run_sync_soon_threadsafe(host_callback)

    def host_callback():
        nonlocal gr_library
        nonlocal gr_loop

        library = current_async_library_tlocal.name
        loop = asyncio._get_running_loop()
        try:
            asyncio.set_event_loop(gr_loop)
            asyncio._set_running_loop(gr_loop)
            current_async_library_tlocal.name = gr_library

            if gr:  # running
                gr.parent = current_greenlet()

                obj = gr.switch()
            else:  # not yet started
                obj = gr.switch(run, wrapper(*args), **kwargs)

            gr_library = current_async_library_tlocal.name
            gr_loop = asyncio._get_running_loop()
        finally:
            current_async_library_tlocal.name = library
            asyncio._set_running_loop(loop)
            asyncio.set_event_loop(loop)

        if gr.dead:  # completed
            done_callback(obj)
            return

        if obj is not None:  # timeout is non-zero
            obj.add_done_callback(executor_callback)
            return

        run_sync_soon_not_threadsafe(host_callback)

    host_callback()  # run in guest mode
