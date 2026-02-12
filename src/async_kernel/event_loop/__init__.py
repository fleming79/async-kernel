from __future__ import annotations

from typing import TYPE_CHECKING

import anyio

from async_kernel.typing import Loop

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CoroutineType


def run(func: Callable[[], CoroutineType], loop: Loop, loop_options: dict, /) -> None:
    """
    Run func to completion using specified event loop.

    Args:
        func: A coroutine function.
        loop: The name of the event loop in which to run func to completion.
        loop_options: Options to use when creating the loop.
    """
    loop = Loop(loop)
    match loop:
        case Loop.asyncio:
            anyio.run(func, backend="asyncio", backend_options=loop_options)

        case Loop.trio:
            anyio.run(func, backend="trio", backend_options=loop_options)

        case Loop.tk_trio:
            import async_kernel.event_loop.tk_trio_guest  # noqa: PLC0415

            async_kernel.event_loop.tk_trio_guest.run(func, **loop_options)
        case Loop.qt_trio:
            import async_kernel.event_loop.qt_trio_guest  # noqa: PLC0415

            async_kernel.event_loop.qt_trio_guest.run(func, **loop_options)
