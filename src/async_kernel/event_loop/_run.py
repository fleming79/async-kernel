from __future__ import annotations

from typing import TYPE_CHECKING

import anyio

from async_kernel.common import import_item
from async_kernel.typing import Backend, Host, RunSettings

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import CoroutineType


def run(func: Callable[[], CoroutineType], args: tuple, settings: RunSettings, /) -> None:
    """
    Run func to completion using specified backend / event loop.

    The backend defaults to asyncio if it is not specified.

    Args:
        func: A coroutine function.
        args: Args to use when calling func.
        settings: Specifics on the backend and optional gui loop with the settings.
    """
    backend = Backend(settings.get("backend", "asyncio"))
    if loop := settings.get("loop"):
        # A loop with the backend running as a guest
        get_host: Callable[..., Host] = import_item(f"async_kernel.event_loop.{loop}.get_host")
        host: Host = get_host(**settings.get("loop_options") or {})
        return run_with_host(host, func, args, backend, settings.get("backend_options") or {})
    # backend only
    return anyio.run(func, *args, backend=str(backend), backend_options=settings.get("backend_options"))


def run_with_host(
    host: Host,
    async_fn: Callable[[], Awaitable],
    args: tuple,
    backend: Backend,
    backend_options: dict,
) -> None:
    "Run async_fn in the host loop and the backend running as guest."

    match backend:
        case Backend.asyncio:
            from .asyncio_guest import start_guest_run  # noqa: PLC0415

            start_guest_run(
                async_fn,
                *args,
                run_sync_soon_threadsafe=host.run_sync_soon_threadsafe,
                run_sync_soon_not_threadsafe=host.run_sync_soon_not_threadsafe,
                done_callback=host.done_callback,
                **backend_options,
            )
        case Backend.trio:
            import trio.lowlevel  # noqa: PLC0415

            trio.lowlevel.start_guest_run(
                async_fn,
                *args,
                run_sync_soon_threadsafe=host.run_sync_soon_threadsafe,
                run_sync_soon_not_threadsafe=host.run_sync_soon_not_threadsafe,
                done_callback=host.done_callback,
                **backend_options,
            )
    host.mainloop()
