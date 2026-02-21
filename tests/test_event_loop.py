import asyncio
import importlib.util

import aiologic
import anyio
import outcome
import pytest
from aiologic.lowlevel import current_async_library
from traitlets import import_item
from typing_extensions import override

import async_kernel.event_loop
import async_kernel.event_loop.asyncio_guest
from async_kernel.common import Fixed
from async_kernel.event_loop.run import Host, get_runtime_matplotlib_guis
from async_kernel.pending import Pending
from async_kernel.typing import Backend, Loop, RunSettings


class TestHost:
    def test_host(self):
        host = Host()
        with pytest.raises(RuntimeError):
            host.mainloop()
        host.done_callback(outcome.capture(lambda: 1 + 1))
        result = host.mainloop()
        assert result == 2

    def test_custom_host(self, mocker):
        class MyCustomHost(Host):
            LOOP = Loop.custom
            MATPLOTLIB_GUIS = ("my gui",)
            event_done = Fixed(aiologic.Event)

            @override
            def done_callback(self, outcome: outcome.Outcome) -> None:
                assert get_runtime_matplotlib_guis() == MyCustomHost.MATPLOTLIB_GUIS
                self._outcome = outcome
                self.event_done.set()

            @override
            def mainloop(self):
                with asyncio.Runner() as runner:
                    loop = runner.get_loop()
                    self.run_sync_soon_not_threadsafe = loop.call_soon  # pyright: ignore[reportAttributeAccessIssue]
                    self.run_sync_soon_threadsafe = loop.call_soon_threadsafe  # pyright: ignore[reportAttributeAccessIssue]
                    self.start_guest()
                    loop.run_until_complete(self.event_done)
                return super().mainloop()

        async def test_func(*args):
            assert current_async_library() == "trio"
            return args

        settings = RunSettings(
            backend="trio",
            loop=Loop.custom,
            loop_options={"host_class": MyCustomHost},
        )
        result = async_kernel.event_loop.run(test_func, ("abc",), settings)
        assert result == ("abc",)
        assert get_runtime_matplotlib_guis() == ()

    def test_custom_host_import(self):
        settings = RunSettings(backend="trio", loop=Loop.custom, loop_options={"host_class": "async_kernel.Pending"})
        with pytest.raises(TypeError):
            Host.run(anyio.sleep, (), settings)

    def test_asyncio_host(self):
        async def test_func(val):
            loop = asyncio.get_running_loop()
            if importlib.util.find_spec("uvloop"):
                assert isinstance(loop, import_item("uvloop.Loop"))
            assert current_async_library() == "trio"
            return val

        settings = RunSettings(
            backend="trio",
            loop=Loop.asyncio,
            loop_options={"use_uvloop": True} if importlib.util.find_spec("uvloop") else {},
        )
        result = async_kernel.event_loop.run(test_func, ("abc",), settings)
        assert result == "abc"

    def test_trio_host(self):
        async def test_func(val):
            asyncio.get_running_loop()
            assert current_async_library() == "asyncio"
            with pytest.raises(RuntimeError, match="already running"):
                async_kernel.event_loop.run(test_func, ("abc",), settings)
            return val

        settings = RunSettings(backend=Backend.asyncio, loop=Loop.trio)
        result = async_kernel.event_loop.run(test_func, ("abc",), settings)
        assert result == "abc"

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
