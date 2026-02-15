from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import anyio
from aiologic import Event
from typing_extensions import override

from async_kernel.common import Fixed
from async_kernel.event_loop._run import Host
from async_kernel.typing import Loop

if TYPE_CHECKING:
    from outcome import Outcome


class AsyncioHost(Host):
    LOOP = Loop.asyncio
    done_event = Fixed(Event)

    def __init__(self, **backend_options) -> None:
        self.backend_options = backend_options

    async def _start(self):
        loop = asyncio.get_running_loop()
        self.run_sync_soon_not_threadsafe = loop.call_soon  # pyright: ignore[reportAttributeAccessIssue]
        self.run_sync_soon_threadsafe = loop.call_soon_threadsafe  # pyright: ignore[reportAttributeAccessIssue]
        self.start_guest()
        await self.done_event

    @override
    def done_callback(self, outcome: Outcome) -> None:
        super().done_callback(outcome)
        self.done_event.set()

    @override
    def mainloop(self):
        anyio.run(self._start, backend="asyncio", backend_options=self.backend_options)
        return super().mainloop()
