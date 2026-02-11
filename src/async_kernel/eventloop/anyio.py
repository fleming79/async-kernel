from __future__ import annotations

from typing import TYPE_CHECKING, Any

import anyio

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


def run_asyncio(async_fn: Callable[[], Awaitable], **kwargs: Any) -> None:
    anyio.run(async_fn, backend="asyncio", backend_options=kwargs)


def run_trio(async_fn: Callable[[], Awaitable], **kwargs: Any) -> None:
    anyio.run(async_fn, backend="trio", backend_options=kwargs)
