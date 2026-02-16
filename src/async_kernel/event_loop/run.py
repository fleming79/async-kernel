from __future__ import annotations

import threading
from importlib import import_module
from typing import TYPE_CHECKING, Any, Generic, Self

import anyio

from async_kernel.common import import_item
from async_kernel.typing import Backend, Loop, RunSettings, T

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CoroutineType

    from outcome import Outcome


__all__ = ["Host", "run"]


def run(func: Callable[..., CoroutineType[Any, Any, T]], args: tuple, settings: RunSettings, /) -> T:
    """
    Run `func` to completion asynchronously in the current thread using a [backend][async_kernel.typing.Backend]
    with an optional gui event loop (_host_).

    The default backend is ['asyncio'][async_kernel.typing.Backend.asyncio].

    If [loop][async_kernel.typing.Loop] is specified in `settings`. A _host_ (gui) mainloop
    will be started with the `backend` running as a guest (in the same thread). The `backend`
    will execute `func` asynchronously to completion. Once completed the backend and host
    are stopped and finally the result is returned.

    Args:
        func: A coroutine function.
        args: Args to use when calling func.
        settings: Settings to use when running func.

    [async_kernel.Loop.custom][]:
        To define a custom gui event loop do the following:
        1. Create a subclass of Host.
        2. In settings specify: `{'loop':'custom', 'loop_options':{'host_class': MyCustomHost}}`
            Alternatively, the class can be specified as a dotted path if it is importable.

    """
    if settings.get("loop"):
        # A loop with the backend running as a guest.
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
    A class that provides the necessary callbacks for `start_guest_run`.
    """

    LOOP: Loop
    MATPLOTLIB_GUIS = ()
    _subclasses: dict[Loop, type[Self]] = {}
    _instances: dict[threading.Thread, Host] = {}

    _outcome: Outcome[T] | None = None
    start_guest: Callable[[], Any] = staticmethod(lambda: None)
    "A callback to start the guest. This must be called by a subclass."

    def __init_subclass__(cls) -> None:
        if cls.LOOP is not Loop.custom:
            cls._subclasses[cls.LOOP] = cls

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

        loop = Loop(settings.get("loop"))
        backend = Backend(settings.get("backend", "asyncio"))
        backend_options = settings.get("backend_options") or {}
        loop_options = settings.get("loop_options") or {}

        if loop is Loop.custom:
            loop_options = loop_options.copy()
            cls_ = loop_options.pop("host_class")
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
        if backend is Backend.asyncio:
            from .asyncio_guest import start_guest_run as sgr  # noqa: PLC0415
        else:
            from trio.lowlevel import start_guest_run as sgr  # noqa: PLC0415
        host = cls_(**loop_options)
        # Provide the start_guest function that can only be called once.
        host.start_guest = lambda: [
            sgr(
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

    # The methods below here should be overridden by a subclass
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
