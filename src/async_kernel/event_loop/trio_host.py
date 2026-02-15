from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
from typing_extensions import override

from async_kernel.event_loop._run import Host
from async_kernel.typing import Loop

if TYPE_CHECKING:
    from outcome import Outcome


class TrioHost(Host):
    LOOP = Loop.trio
    _done = False

    def __init__(self, **backend_options) -> None:
        self.backend_options = backend_options

    async def _start(self):
        from aiologic import Queue  # noqa: PLC0415

        queue = Queue()

        self.run_sync_soon_not_threadsafe = lambda fn: queue.green_put(fn, blocking=False)
        self.run_sync_soon_threadsafe = lambda fn: queue.green_put(fn, blocking=False)
        self.done = lambda: queue.green_put(lambda: None, blocking=False)
        self.start_guest()
        while not self._outcome:
            fn = await queue.async_get()
            try:
                fn()
            except Exception:
                continue

    @override
    def done_callback(self, outcome: Outcome) -> None:
        super().done_callback(outcome)
        self.done()

    @override
    def mainloop(self):
        anyio.run(self._start, backend="trio", backend_options=self.backend_options)
        return super().mainloop()
