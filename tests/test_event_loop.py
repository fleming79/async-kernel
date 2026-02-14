import asyncio

import aiologic
import anyio
import outcome
import pytest
import trio.lowlevel
from aiologic.lowlevel import current_async_library

import async_kernel.event_loop
import async_kernel.event_loop.asyncio_guest
from async_kernel.event_loop._run import Host
from async_kernel.pending import Pending
from async_kernel.typing import Backend


class TestHost:
    def test_host(self):
        host = Host()
        with pytest.raises(RuntimeError):
            host.mainloop()
        host.done_callback(outcome.capture(lambda: 1 + 1))
        result = host.mainloop()
        assert result == 2

    @pytest.mark.parametrize("backend", Backend)
    def test_host_import(self, mocker, backend: Backend):
        if backend is Backend.asyncio:
            func = mocker.patch.object(async_kernel.event_loop.asyncio_guest, "start_guest_run")
        else:
            func = mocker.patch.object(trio.lowlevel, "start_guest_run")

        async_kernel.event_loop._run.get_host = Host  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]
        try:
            with pytest.raises(RuntimeError, match="mainloop"):
                async_kernel.event_loop.run(anyio.sleep, (0,), {"loop": "_run", "backend": backend})  # pyright: ignore[reportArgumentType]
            assert func.call_count == 1
        finally:
            del async_kernel.event_loop._run.get_host  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]

    async def test_start_guest_run(self, anyio_backend) -> None:
        lock = aiologic.Lock()
        results = set()

        async def work(flavor, i):
            if i != 0:  # to mix tasks
                await aiologic.lowlevel.async_sleep(1e-1)

            results.add(f"{flavor}={current_async_library()} task={i} start")

            async with lock:
                await aiologic.lowlevel.async_sleep(0.01)

            results.add(f"{flavor}={current_async_library()} task={i} end")

        async def run(flavor):
            async with anyio.create_task_group() as tg:
                tg.start_soon(work, flavor, 0)
                tg.start_soon(work, flavor, 1)

        async def asyncio_run_as_asyncio_guest(func, /, *args):
            pen: Pending[outcome.Outcome] = Pending()

            loop = asyncio.get_running_loop()
            async_kernel.event_loop.asyncio_guest.start_guest_run(
                func,
                *args,
                run_sync_soon_threadsafe=loop.call_soon_threadsafe,
                run_sync_soon_not_threadsafe=loop.call_soon,
                host_uses_signal_set_wakeup_fd=True,
                done_callback=pen.set_result,
            )

            result = (await pen).unwrap()
            results.add(result)

        async with anyio.create_task_group() as tg:
            tg.start_soon(run, "asyncio-host")
            tg.start_soon(asyncio_run_as_asyncio_guest, run, "asyncio-guest1")
            tg.start_soon(asyncio_run_as_asyncio_guest, run, "asyncio-guest2")

        assert len(results) == 13
        summary = " | ".join(sorted(r.split("= ")[-1] for r in results if r))
        assert (
            summary
            == "asyncio-guest1=asyncio task=0 end | asyncio-guest1=asyncio task=0 start | asyncio-guest1=asyncio task=1 end | asyncio-guest1=asyncio task=1 start | asyncio-guest2=asyncio task=0 end | asyncio-guest2=asyncio task=0 start | asyncio-guest2=asyncio task=1 end | asyncio-guest2=asyncio task=1 start | asyncio-host=asyncio task=0 end | asyncio-host=asyncio task=0 start | asyncio-host=asyncio task=1 end | asyncio-host=asyncio task=1 start"
        )
