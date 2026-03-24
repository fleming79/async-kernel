from __future__ import annotations

import threading
from importlib import import_module
from typing import TYPE_CHECKING, Any, Generic, Self

import anyio
from wrapt import lazy_import

from async_kernel.common import import_item
from async_kernel.typing import Backend, Hosts, RunSettings, T

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CoroutineType

    from outcome import Outcome


__all__ = ["Host", "run"]


if TYPE_CHECKING:
    from trio.lowlevel import start_guest_run as start_guest_run_asyncio  # noqa: TC004

    from async_kernel.event_loop.asyncio_guest import start_guest_run as start_guest_run_trio  # noqa: TC004

globals()["start_guest_run_asyncio"] = lazy_import("async_kernel.event_loop.asyncio_guest", "start_guest_run")
globals()["start_guest_run_trio"] = lazy_import("trio.lowlevel", "start_guest_run")


def get_start_guest_run(backend: Backend):
    """
    Get the `start_guest_run` function corresponding to the `backend`.
    """
    return start_guest_run_asyncio if Backend(backend) is Backend.asyncio else start_guest_run_trio


def run(func: Callable[..., CoroutineType[Any, Any, T]], args: tuple, settings: RunSettings, /) -> T:
    """
    Run `func` to completion asynchronously in the current thread using a [backend][async_kernel.typing.Backend]
    with an optional host (gui event loop).

    The default backend is ['asyncio'][async_kernel.typing.Backend.asyncio].

    If [host][async_kernel.typing.Hosts] is specified in `settings`. A _host_ (gui) mainloop
    will be started with the `backend` running as a guest (in the same thread). The `backend`
    will execute `func` asynchronously to completion. Once completed the `backend` and `host`
    are stopped and finally the result is returned.

    Args:
        func: A coroutine function.
        args: Args to use when calling func.
        settings: Settings to use when running func.

    Custom host:
        A custom host can be started by subclassing [Host][] and passed as the 'host_class' as the
        class or a dotted path if it is importable.
    """
    if settings.get("host"):
        # A gui with the backend running as a guest.
        return Host.run(func, args, settings)
    # backend only.
    return anyio.run(
        func,
        *args,
        backend=Backend(settings.get("backend", "asyncio")),
        backend_options=settings.get("backend_options"),
    )


def get_runtime_matplotlib_guis(thread: threading.Thread | None = None) -> tuple[str, ...]:
    "A list of runtime guis supported by the host for the associated thread."
    if host := Host.current(thread):
        return host.MATPLOTLIB_GUIS
    return ()


class Host(Generic[T]):
    """
    A class that provides the necessary callbacks to run a gui event loop with a `backend` started using `start_guest_run`.
    """

    HOST: Hosts
    MATPLOTLIB_GUIS = ()
    _subclasses: dict[Hosts, type[Self]] = {}
    _instances: dict[threading.Thread, Host] = {}

    _outcome: Outcome[T] | None = None
    start_guest: Callable[[], Any] = staticmethod(lambda: None)
    "A callback to start the guest. This must be called by a subclass."

    def __init_subclass__(cls) -> None:
        if cls.HOST is not Hosts.custom:
            cls._subclasses[cls.HOST] = cls

    @classmethod
    def current(cls, thread: threading.Thread | None = None) -> Host | None:
        "The host running in the corresponding thread or current thread."
        thread = thread or threading.current_thread()
        return cls._instances.get(thread)

    @classmethod
    def run(cls, func: Callable[..., CoroutineType[Any, Any, T]], args: tuple, settings: RunSettings, /) -> T:
        "Run the loop in the current thread with a backend guest."

        if (thread := threading.current_thread()) in cls._instances:
            msg = "A host is already running in this thread"
            raise RuntimeError(msg)

        loop = Hosts(settings.get("host"))
        backend = Backend(settings.get("backend", "asyncio"))
        backend_options = settings.get("backend_options") or {}
        host_options = settings.get("host_options") or {}

        if "host_class" in host_options:
            host_options = host_options.copy()
            cls_ = host_options.pop("host_class")
            if isinstance(cls_, str):
                cls_ = import_item(cls_)
            if not issubclass(cls_, cls):
                msg = f"{cls_} is not a subclass of {cls}!"
                raise TypeError(msg)
        else:
            assert loop != backend
            if loop not in cls._subclasses:
                import_module(f"async_kernel.event_loop.{loop}_host")
                assert loop in cls._subclasses, f"Host for {loop=} is not implemented correctly!"
            cls_ = cls._subclasses[loop]
        assert cls_.HOST is loop

        host = cls_(**host_options)
        # set the `start_guest` function (runs once).
        backend_options.setdefault("host_uses_signal_set_wakeup_fd", host.host_uses_signal_set_wakeup_fd)
        start_guest_run = get_start_guest_run(backend)
        host.start_guest = lambda: [
            start_guest_run(
                func,
                *args,
                run_sync_soon_threadsafe=host.run_sync_soon_threadsafe,
                run_sync_soon_not_threadsafe=host.run_sync_soon_not_threadsafe,
                done_callback=host.done_callback,
                **backend_options,
            ),
            setattr(host, "start_guest", lambda: None),
        ][1]
        host._instances[thread] = host
        try:
            return host.mainloop()
        finally:
            host._instances.pop(threading.current_thread())

    # Override the methods/attributes below as required.
    host_uses_signal_set_wakeup_fd = False

    def run_sync_soon_threadsafe(self, fn: Callable[[], Any]) -> None: ...
    def run_sync_soon_not_threadsafe(self, fn: Callable[[], Any]) -> None: ...

    def done_callback(self, outcome: Outcome) -> None:
        self._outcome = outcome

    def mainloop(self) -> T:
        "Start the main event loop of the host."
        self.start_guest()  # Call at an appropriate time in the overriding subclass.
        if not self._outcome:
            msg = "The mainloop should only exit once done_callback has been called!"
            raise RuntimeError(msg)
        return self._outcome.unwrap()  # pragma: no cover
