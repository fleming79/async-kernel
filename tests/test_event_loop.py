import anyio
import outcome
import pytest

import async_kernel.event_loop
from async_kernel.event_loop._run import Host


class TestHost:
    def test_host(self):
        host = Host()
        host.done_callback(outcome.capture(lambda: 1 + 1))
        result = host.mainloop()
        assert result == 2

    def test_host_import(self, mocker):
        async_kernel.event_loop._run.get_host = Host  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]
        try:
            with pytest.raises(RuntimeError, match="mainloop"):
                async_kernel.event_loop.run(anyio.sleep, (0,), {"loop": "_run", "backend": "trio"})  # pyright: ignore[reportArgumentType]
        finally:
            del async_kernel.event_loop._run.get_host  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]
