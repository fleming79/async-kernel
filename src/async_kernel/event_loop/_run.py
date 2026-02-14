from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

import anyio

from async_kernel.common import import_item
from async_kernel.typing import Backend, RunSettings, T

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CoroutineType

    from outcome import Outcome


def run(func: Callable[..., CoroutineType[Any, Any, T]], args: tuple, settings: RunSettings, /) -> T:
    """
    Run func to completion using specified backend / event loop.

    The backend defaults to asyncio if it is not specified.

    Args:
        func: A coroutine function.
        args: Args to use when calling func.
        settings: Specifics on the backend and optional gui loop with the settings.
    """
    backend = Backend(settings.get("backend", "asyncio"))
    backend_options = settings.get("backend_options") or {}
    if loop := settings.get("loop"):
        # A loop with the backend running as a guest.
        get_host: Callable[..., Host] = import_item(f"async_kernel.event_loop.{loop}.get_host")
        host: Host = get_host(**settings.get("loop_options") or {})
        if backend is Backend.asyncio:
            from .asyncio_guest import start_guest_run as sgr  # noqa: PLC0415
        else:
            from trio.lowlevel import start_guest_run as sgr  # noqa: PLC0415
        sgr(
            func,
            *args,
            run_sync_soon_threadsafe=host.run_sync_soon_threadsafe,
            run_sync_soon_not_threadsafe=host.run_sync_soon_not_threadsafe,
            done_callback=host.done_callback,
            **backend_options,
        )
        return host.mainloop()
    # backend only
    return anyio.run(func, *args, backend=str(backend), backend_options=backend_options)


class Host(Generic[T]):
    """
    A template non-async event loops to work with [run][].
    """

    _outcome: Outcome[T] | None = None

    def run_sync_soon_threadsafe(self, fn: Callable[[], Any]) -> None: ...
    def run_sync_soon_not_threadsafe(self, fn: Callable[[], Any]) -> None: ...
    def done_callback(self, outcome: Outcome) -> None:
        self._outcome = outcome

    def mainloop(self) -> T:
        if not self._outcome:
            msg = "The mainloop should only exit once done_callback has been called!"
            raise RuntimeError(msg)
        return self._outcome.unwrap()  # pragma: no cover
