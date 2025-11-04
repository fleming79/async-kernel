from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import inspect
import logging
import reprlib
import threading
import time
import weakref
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from contextlib import asynccontextmanager
from types import CoroutineType
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Never, Self, Unpack, overload

import anyio
from aiologic import Event, REvent
from aiologic.lowlevel import current_async_library
from typing_extensions import override
from zmq import Context, Socket, SocketType

import async_kernel
from async_kernel.kernelspec import Backend
from async_kernel.typing import CallerStartNewOptions, NoValue, T

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import CoroutineType

    from anyio.abc import TaskGroup, TaskStatus

    from async_kernel.typing import P

__all__ = ["Caller", "Future", "FutureCancelledError", "InvalidStateError"]

truncated_rep = reprlib.Repr()
truncated_rep.maxlevel = 1
truncated_rep.maxother = 100
truncated_rep.fillvalue = "…"


class FutureCancelledError(anyio.ClosedResourceError):
    "Used to indicate a [Future][async_kernel.caller.Future] is cancelled."


class InvalidStateError(RuntimeError):
    "An invalid state of a [Future][async_kernel.caller.Future]."


class Future(Awaitable[T]):
    """
    A class representing a future result modelled on [asyncio.Future][].

    This class provides an anyio compatible Future primitive. It is designed
    to work with `Caller` to enable thread-safe calling, setting, awaiting and
    cancelling execution results.
    """

    _cancelled = False
    _canceller: Callable[[str | None], Any] | None = None
    _exception = None
    _done = False
    _result: T
    REPR_OMIT: ClassVar[set[str]] = {"func", "args", "kwargs"}

    def __init__(self, **metadata) -> None:
        self._done_callbacks = []
        self._metadata = metadata
        self._done_event = Event()

    @override
    def __repr__(self) -> str:
        rep = "<Future" + (" ⛔" if self.cancelled() else "") + (" 🏁" if self.done() else " 🏃")
        with contextlib.suppress(Exception):
            md = self.metadata
            if "func" in md:
                items = [f"{k}={truncated_rep.repr(v)}" for k, v in md.items() if k not in self.REPR_OMIT]
                rep += f" | {md['func']} {' | '.join(items) if items else ''}"
            else:
                rep += f" {truncated_rep.repr(md)}" if md else ""
        return rep + " >"

    @override
    def __await__(self) -> Generator[Any, None, T]:
        return self.wait().__await__()

    def _set_value(self, mode: Literal["result", "exception"], value) -> None:
        if self._done:
            raise InvalidStateError
        self._done = True
        if self._cancelled:
            mode = "exception"
            value = self._make_cancelled_error()
        if mode == "exception":
            self._exception = value
        else:
            self._result = value  # pyright: ignore[reportAttributeAccessIssue]
        while self._done_callbacks:
            cb = self._done_callbacks.pop()
            try:
                cb(self)
            except Exception:
                pass
        self._done_event.set()

    def _make_cancelled_error(self) -> FutureCancelledError:
        return FutureCancelledError(self._cancelled) if isinstance(self._cancelled, str) else FutureCancelledError()

    @property
    def metadata(self) -> dict[str, Any]:
        """
        A dict provided to store metadata with the Future until it is done.

        !!! info
            The metadata is used when forming the representation of the Future.

        !!! example

            === "At init"

                ```python
                fut = Future(name="My future")
                ```

            === "On the instance"

                ```python
                fut = Caller().call_soon(anyio.sleep, 0)
                fut.metadata.update(name="My future")
                ```

        !!! info

            A `Future` returned by methods of [Caller][async_kernel.caller.Caller] stores the function and call arguments
            in the futures metedata. It also adds a *done callback* using [add_done_callback][async_kernel.caller.Future.add_done_callback]
            which clears the Future's metadata to avoid memory leaks.
        """
        return self._metadata

    if TYPE_CHECKING:

        @overload
        async def wait(
            self, *, timeout: float | None = ..., shield: bool = False | ..., result: Literal[True] = True
        ) -> T: ...

        @overload
        async def wait(self, *, timeout: float | None = ..., shield: bool = ..., result: Literal[False]) -> None: ...

    async def wait(self, *, timeout: float | None = None, shield: bool = False, result: bool = True) -> T | None:
        """
        Wait for Future to be done (thread-safe) returning the result if specified.

        Args:
            timeout: Timeout in seconds.
            shield: Shield the Future from cancellation.
            result: Whether the result should be returned (use `result=False` to avoid exceptions raised by [async_kernel.caller.Future.result][]).
        """
        try:
            if not self._done_event:
                with anyio.fail_after(timeout):
                    await self._done_event
            return self.result() if result else None
        finally:
            if not self.done() and not shield:
                self.cancel("Cancelled with waiter cancellation.")

    def set_result(self, value: T) -> None:
        "Set the result (thread-safe)."
        self._set_value("result", value)

    def set_exception(self, exception: BaseException) -> None:
        "Set the exception (thread-safe)."
        self._set_value("exception", exception)

    def done(self) -> bool:
        """
        Returns True if the Future has a result.

        Done means either that a result / exception is available."""
        return self._done

    def add_done_callback(self, fn: Callable[[Self], Any]) -> None:
        """
        Add a callback for when the Future is done (not thread-safe).

        If the Future is already done it will called immediately.
        """
        if not self.done():
            self._done_callbacks.append(fn)
        else:
            fn(self)

    def cancel(self, msg: str | None = None) -> bool:
        """
        Cancel the Future.

        !!! note

            - Cancellation cannot be undone.
            - The Future will not be *done* until either [async_kernel.caller.Future.set_result][] or [async_kernel.caller.Future.set_exception][] is called.
                In both cases the value is ignore and replaced with a [FutureCancelledError][async_kernel.caller.FutureCancelledError]
                and the result is inaccessible.

        Args:
            msg: The message to use when cancelling.

        Returns if it has been cancelled.
        """
        if not self.done():
            if msg and isinstance(self._cancelled, str):
                msg = f"{self._cancelled}\n{msg}"
            self._cancelled = msg or self._cancelled or True
            if canceller := self._canceller:
                canceller(msg)
        return self.cancelled()

    def cancelled(self) -> bool:
        """Return True if the Future is cancelled."""
        return bool(self._cancelled)

    def result(self) -> T:
        """
        Return the result of the Future.

        If the Future has been cancelled, this method raises a [FutureCancelledError][async_kernel.caller.FutureCancelledError] exception.

        If the Future isn't done yet, this method raises an [InvalidStateError][async_kernel.caller.InvalidStateError] exception.
        """
        if not self.cancelled() and not self.done():
            raise InvalidStateError
        if e := self.exception():
            raise e
        return self._result

    def exception(self) -> BaseException | None:
        """
        Return the exception that was set on the Future.

        If the Future has been cancelled, this method raises a [FutureCancelledError][async_kernel.caller.FutureCancelledError] exception.

        If the Future isn't done yet, this method raises an [InvalidStateError][async_kernel.caller.InvalidStateError] exception.
        """
        if self._cancelled:
            raise self._make_cancelled_error()
        if not self.done():
            raise InvalidStateError
        return self._exception

    def remove_done_callback(self, fn: Callable[[Self], object], /) -> int:
        """
        Remove all instances of a callback from the callbacks list.

        Returns the number of callbacks removed.
        """
        n = 0
        while fn in self._done_callbacks:
            n += 1
            self._done_callbacks.remove(fn)
        return n

    def set_canceller(self, canceller: Callable[[str | None], Any]) -> None:
        """
        Set a callback to handle cancellation.

        Args:
            canceller: A callback that performs the cancellation of the Future.
                - It must accept the cancellation message as the first argument.
                - The cancellation call is not thread-safe.

        !!! note

            `set_result` must still be called to mark the Future as completed. You can pass any
            value as it will be replaced with a [async_kernel.caller.FutureCancelledError][].
        """
        if self.done() or self._canceller:
            raise InvalidStateError
        self._canceller = canceller
        if self.cancelled():
            self.cancel()


class Caller(anyio.AsyncContextManagerMixin):
    """
    A class to enable calling functions and coroutines between anyio event loops.

    The `Caller` class provides a mechanism to execute functions and coroutines
    in a dedicated thread, leveraging AnyIO for asynchronous task management.
    It supports scheduling calls with delays, executing them immediately,
    and running them without a context.  It also provides a means to manage
    a pool of threads for general purpose offloading of tasks.

    The class maintains a registry of instances, associating each with a specific
    thread. It uses a task group to manage the execution of scheduled tasks and
    provides methods to start, stop, and query the status of the caller.
    """

    MAX_IDLE_POOL_INSTANCES = 10
    "The number of `pool` instances to leave idle (See also [to_thread][async_kernel.Caller.to_thread])."

    _instances: ClassVar[dict[threading.Thread, Self]] = {}
    _to_thread_pool: ClassVar[deque[Self]] = deque()
    _backend: Backend
    _queue_map: dict[int, Future]
    _queue: deque[tuple[contextvars.Context, Future] | Callable[[], Any]]
    _thread: threading.Thread
    _queue_added: REvent
    _stopped_event: Event
    _stopped = False
    _protected = False
    _running = False
    _name: str
    _future_var: contextvars.ContextVar[Future | None] = contextvars.ContextVar("_future_var", default=None)

    log: logging.LoggerAdapter[Any]
    ""
    iopub_sockets: ClassVar[weakref.WeakKeyDictionary[threading.Thread, Socket]] = weakref.WeakKeyDictionary()
    ""
    iopub_url: ClassVar = "inproc://iopub"
    ""

    def __new__(
        cls,
        *,
        thread: threading.Thread | None = None,
        log: logging.LoggerAdapter | None = None,
        create: bool = False,
        protected: bool = False,
    ) -> Self:
        """
        Create or retrieve the `Caller` instance for the specified thread.

        Args:
            thread: The thread where the caller is based. There is only one instance per thread.
            log: Logger to use for logging messages.
            create: Whether to create a new instance if one does not exist for the current thread.
            protected: Whether the caller is protected from having its event loop closed.

        Returns:
            Caller: The `Caller` instance for the current thread.

        Raises:
            RuntimeError: If `create` is `False` and a `Caller` instance does not exist.
        """

        thread = thread or threading.current_thread()
        if not (inst := cls._instances.get(thread)):
            if not create:
                msg = f"A caller does not exist for{thread=}. Did you mean use the classmethod `Caller.get_instance()`?"
                raise RuntimeError(msg)
            inst = super().__new__(cls)
            inst._thread = thread
            inst._name = thread.name
            inst.log = log or logging.LoggerAdapter(logging.getLogger())
            inst._queue = deque()
            inst._queue_added = REvent()
            inst._protected = protected
            inst._queue_map = {}
            cls._instances[thread] = inst
        return inst

    @override
    def __repr__(self) -> str:
        return f"Caller<{self.name} {'🏃' if self.running else ('🏁 stopped' if self.stopped else '❗ not running')}>"

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        self._backend = Backend(current_async_library())
        self._running = True
        self._stopped_event = Event()
        async with anyio.create_task_group() as tg:
            try:
                await tg.start(self._server_loop, tg)
                yield self
            finally:
                self.stop(force=True)

    async def _server_loop(self, tg: TaskGroup, task_status: TaskStatus[None]) -> None:
        socket = Context.instance().socket(SocketType.PUB)
        socket.linger = 500
        socket.connect(self.iopub_url)
        try:
            self.iopub_sockets[self.thread] = socket
            task_status.started()
            while not self._stopped:
                if not self._queue:
                    self._queue_added.clear()
                    await self._queue_added
                while self._queue:
                    if self._stopped:
                        return
                    item = self._queue.popleft()
                    if isinstance(item, Callable):
                        try:
                            result = item()
                            if inspect.iscoroutine(result):
                                await result
                        except Exception as e:
                            self.log.exception("Simple call failed", exc_info=e)
                    else:
                        context, fut = item
                        context.run(tg.start_soon, self._wrap_call, fut)
        finally:
            self._running = False
            for item in self._queue:
                if isinstance(item, tuple):
                    item[1].set_exception(FutureCancelledError())
            socket.close()
            self.iopub_sockets.pop(self.thread, None)
            self._stopped_event.set()
            tg.cancel_scope.cancel()

    async def _wrap_call(self, fut: Future) -> None:
        if fut.cancelled():
            if not fut.done():
                fut.set_result(None)  # This will cancel
            return
        md = fut.metadata
        func = md["func"]
        token = self._future_var.set(fut)
        try:
            with anyio.CancelScope() as scope:
                fut.set_canceller(lambda msg: self.call_direct(scope.cancel, msg))
                try:
                    if (delay := md.get("delay")) and ((delay := delay - time.monotonic() + md["start_time"]) > 0):
                        await anyio.sleep(delay)
                    # Evaluate
                    result = func(*md["args"], **md["kwargs"])
                    if inspect.iscoroutine(result):
                        result = await result
                    fut.set_result(result)
                except anyio.get_cancelled_exc_class():
                    if not fut.cancelled():
                        fut.cancel()
                    fut.set_result(None)  # This will cancel
                except Exception as e:
                    fut.set_exception(e)
        except Exception as e:
            self.log.exception("Calling func %s failed", func, exc_info=e)
        finally:
            self._future_var.reset(token)

    @property
    def name(self) -> str:
        "The name of the thread when the caller was created."
        return self._name

    @property
    def thread(self) -> threading.Thread:
        "The thread in which the caller will run."
        return self._thread

    @property
    def backend(self) -> Backend:
        "The `anyio` backend the caller is running in."
        return self._backend

    @property
    def protected(self) -> bool:
        "Returns `True` if the caller is protected from stopping."
        return self._protected

    @property
    def running(self):
        "Returns `True` when the caller is available to run requests."
        return self._running

    @property
    def stopped(self) -> bool:
        "Returns  `True` if the caller is stopped."
        return self._stopped

    def get_runner(self, *, started: Callable[[], None] | None = None):
        """The preferred way to run the caller loop.

        !!! tip

            See [async_kernel.caller.Caller.get_instance][] for a usage example.
        """
        if self.running or self.stopped:
            raise RuntimeError

        async def run_caller_in_context() -> None:
            with contextlib.suppress(anyio.get_cancelled_exc_class()):
                async with self:
                    if started:
                        started()
                    await anyio.sleep_forever()

        return run_caller_in_context

    def stop(self, *, force=False) -> None:
        """
        Stop the caller, cancelling all pending tasks and close the thread.

        If the instance is protected, this is no-op unless force is used.
        """
        if self._protected and not force:
            return
        self._stopped = True
        for func in tuple(self._queue_map):
            self.queue_close(func)
        self._queue_added.set()
        self._instances.pop(self.thread, None)
        if self in self._to_thread_pool:
            self._to_thread_pool.remove(self)
        if self.thread is not threading.current_thread():
            self._stopped_event.wait()

    def schedule_call(
        self,
        func: Callable[..., CoroutineType[Any, Any, T] | T],
        /,
        args: tuple,
        kwargs: dict,
        context: contextvars.Context | None = None,
        **metadata: Any,
    ) -> Future[T]:
        """
        Schedule `func` to be called inside a task running in the callers thread (thread-safe).

        The methods [call_soon][async_kernel.caller.Caller.call_soon] and [call_later][async_kernel.caller.Caller.call_later]
        use this method in the background,  they should be used in preference to this method since they provide type hinting for the arguments.

        Args:
            func: The function to be called. If it returns a coroutine, it will be awaited and its result will be returned.
            args: Arguments corresponding to in the call to  `func`.
            kwargs: Keyword arguments to use with in the call to `func`.
            context: The context to use, if not provided the current context is used.
            metadata: Additional metadata to store in the future.

        !!! note

            All arguments are stored in the future's metadata. When the call is done the metadata is cleared to prevent memory leaks.
        """
        if self._stopped:
            raise anyio.ClosedResourceError
        fut = Future(func=func, args=args, kwargs=kwargs, caller=self, **metadata)
        fut.add_done_callback(self._on_call_done)
        self._queue.append((context or contextvars.copy_context(), fut))
        self._queue_added.set()
        return fut

    @staticmethod
    def _on_call_done(fut: Future):
        #  Avoid memory leaks
        fut.metadata.clear()

    def call_later(
        self,
        delay: float,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        """
        Schedule func to be called in caller's event loop copying the current context.

        Args:
            func: The function.
            delay: The minimum delay to add between submission and execution.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        !!! info

            All call arguments are packed into the Futures metadata. The future metadata
            is cleared when futures result is set.
        """
        return self.schedule_call(func, args, kwargs, delay=delay, start_time=time.monotonic())

    def call_soon(
        self,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        """
        Schedule func to be called in caller's event loop copying the current context.

        Args:
            func: The function.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.
        """
        return self.schedule_call(func, args, kwargs)

    def call_direct(
        self,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """
        Schedule `func` to be called in caller's event loop directly.

        This method is provided to facilitate lightweight *thread-safe* function calls that
        need to be performed from within the callers event loop/taskgroup.

        Args:
            func: The function.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        ??? warning

            **Use this method for lightweight calls only!**

        """
        self._queue.append(functools.partial(func, *args, **kwargs))
        self._queue_added.set()

    def queue_get(self, func: Callable) -> Future[Never] | None:
        """Returns Future for `func` where the queue is running.

        !!! warning

            - This future loops forever until the  loop is closed or func no longer exists.
            - `queue_close` is the preferred means to shutdown the queue.
        """
        return self._queue_map.get(hash(func))

    def queue_call(
        self,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """
        Queue the execution of `func` in a queue unique to it and this caller (thread-safe).

        The queue executor loop will stay open until one of the following occurs:

        1. The method [async_kernel.caller.Caller.queue_close][] is called with `func`.
        2. If `func` is a method is deleted and garbage collected (using [weakref.finalize][]).

        Args:
            func: The function.
            *args: Arguments to use with `func`.
            **kwargs: Keyword arguments to use with `func`.
        """
        key = hash(func)
        if not (fut_ := self._queue_map.get(key)):
            queue = deque()
            event_added = REvent()
            with contextlib.suppress(TypeError):
                weakref.finalize(func.__self__ if inspect.ismethod(func) else func, lambda: self.queue_close(key))

            async def queue_loop(key: int, queue: deque, event_added: REvent) -> None:
                fut = self.current_future()
                assert fut
                try:
                    while True:
                        if queue:
                            context, func_, args, kwargs = queue.popleft()
                            try:
                                result = context.run(func_, *args, **kwargs)
                                if inspect.iscoroutine(object=result):
                                    await result
                            except (anyio.get_cancelled_exc_class(), Exception) as e:
                                if fut.cancelled():
                                    raise
                                self.log.exception("Execution %f failed", func_, exc_info=e)
                            finally:
                                func_ = None
                        else:
                            event_added.clear()
                            await event_added
                finally:
                    self._queue_map.pop(key)

            self._queue_map[key] = fut_ = self.call_soon(queue_loop, key=key, queue=queue, event_added=event_added)
        fut_.metadata["kwargs"]["queue"].append((contextvars.copy_context(), func, args, kwargs))
        fut_.metadata["kwargs"]["event_added"].set()

    def queue_close(self, func: Callable | int) -> None:
        """
        Close the execution queue associated with `func` (thread-safe).

        Args:
            func: The queue of the function to close.
        """
        key = func if isinstance(func, int) else hash(func)
        if fut := self._queue_map.pop(key, None):
            fut.cancel()

    @classmethod
    def stop_all(cls, *, _stop_protected: bool = False) -> None:
        """
        A [classmethod][] to stop all un-protected callers.

        Args:
            _stop_protected: A private argument to shutdown protected instances.
        """
        for caller in tuple(reversed(cls._instances.values())):
            caller.stop(force=_stop_protected)

    @classmethod
    def get_instance(cls, *, create: bool | NoValue = NoValue, **kwargs: Unpack[CallerStartNewOptions]) -> Self:  # pyright: ignore[reportInvalidTypeForm]
        """
        A [classmethod][] that gets the caller associated to the thread using the threads name.


        When called without a name `MainThread` will be used as the `name`.

        Args:
            create: Create a new instance if one with the corresponding name does not already exist.
                When not provided it defaults to `True` when `name` is `MainThread` otherwise `False`.
        kwargs:
            Options to use to identify or create a new instance if an instance does not already exist.
        """
        if "name" not in kwargs:
            kwargs["name"] = "MainThread"
        for caller in cls._instances.values():
            if caller.name == kwargs["name"]:
                return caller
        if create is True or (create is NoValue and kwargs["name"] == "MainThread"):
            return cls.start_new(**kwargs)
        msg = f"A Caller was not found for {kwargs['name']=}."
        raise RuntimeError(msg)

    @classmethod
    def to_thread(
        cls,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        """A [classmethod][] to call func in a separate thread see also [to_thread_advanced][async_kernel.Caller.to_thread_advanced]."""
        return cls.to_thread_advanced({"name": None}, func, *args, **kwargs)

    @classmethod
    def to_thread_advanced(
        cls,
        options: CallerStartNewOptions,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        """
        A [classmethod][] to call func in a Caller specified by the options.

        A Caller will be created if it isn't found.

        Args:
            options: A dict wht the `name` of the Caller to use and other Options to pass to [async_kernel.caller.Caller.start_new][]
                should a a new instance is started [^notes].

                [^notes]:  'MainThread' is special name corresponding to the main thread.
                    A `RuntimeError` will be raised if a Caller does not exist for the main thread.

            func: The function.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        Returns:
            A future that can be awaited for the  result of func.
        """
        caller = None
        if not options.get("name"):
            try:
                caller = cls._to_thread_pool.popleft()
            except IndexError:
                pass
        if caller is None:
            caller = cls.get_instance(create=True, **options)
        fut = caller.call_soon(func, *args, **kwargs)
        if not options.get("name"):

            def _to_thread_on_done(_) -> None:
                if not caller._stopped:
                    if len(cls._to_thread_pool) < cls.MAX_IDLE_POOL_INSTANCES:
                        cls._to_thread_pool.append(caller)
                    else:
                        caller.stop()

            fut.add_done_callback(_to_thread_on_done)
        return fut

    @classmethod
    def start_new(
        cls,
        *,
        name: str | None = None,
        log: logging.LoggerAdapter | None = None,
        backend: Backend | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        protected: bool = False,
        backend_options: dict | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
    ) -> Self:
        """
        A [classmethod][] that creates a new caller instance with the thread determined according to the provided `name`.

        When `name` equals the current thread's name it will use the current thread providing the backend is 'asyncio' and
        there is a running event loop available.

        When the name does not match the current thread name, a new thread will be started provided
        that the name provided is not the name does not overlap with any existing threads. When no
        name is provided, a new thread can always be started.

        Args:
            backend: The backend to use for the anyio event loop (anyio.run). Defaults to the backend from where it is called.
            log: A logging adapter to use for debug messages.
            protected: When True, the caller will not shutdown unless shutdown is called with `force=True`.
            backend_options: Backend options for [anyio.run][]. Defaults to `Kernel.backend_options`.

        Returns:
            Caller: The newly created caller.

        Raises:
            RuntimeError: If a caller already exists or when the caller can't be started.

        """
        if name and name in [t.name for t in cls._instances]:
            msg = f"A caller already exists with {name=}!"
            raise RuntimeError(msg)

        # Current thread
        if name is not None and name == threading.current_thread().name:
            if (backend := current_async_library()) == Backend.asyncio:
                loop = asyncio.get_running_loop()
                caller = cls(log=log, create=True, protected=protected)
                caller._task = loop.create_task(caller.get_runner()())  # pyright: ignore[reportAttributeAccessIssue]
                return caller
            msg = f"Starting a caller for the MainThread is not supported for {backend=}"
            raise RuntimeError(msg)

        # New thread
        if name and name in [t.name for t in threading.enumerate()]:
            msg = f"A thread with {name=} already exists!"
            raise RuntimeError(msg)

        def async_kernel_caller() -> None:
            anyio.run(caller.get_runner(started=ready_event.set), backend=backend_, backend_options=backend_options)

        backend_ = Backend(backend if backend is not NoValue else current_async_library())
        if backend_options is NoValue:
            backend_options = async_kernel.Kernel().anyio_backend_options.get(backend_)
        ready_event = Event()
        thread = threading.Thread(target=async_kernel_caller, name=name or None, daemon=True)
        caller = cls(thread=thread, log=log, create=True, protected=protected)
        thread.start()
        ready_event.wait()
        return caller

    @classmethod
    def current_future(cls) -> Future[Any] | None:
        """A [classmethod][] that returns the current future when called from inside a function scheduled by Caller."""
        return cls._future_var.get()

    @classmethod
    def all_callers(cls, running_only: bool = True) -> list[Caller]:
        """
        A [classmethod][] to get a list of the callers.

        Args:
            running_only: Restrict the list to callers that are active (running in an async context).
        """
        return [caller for caller in Caller._instances.values() if caller._running or not running_only]

    @classmethod
    async def as_completed(
        cls,
        items: Iterable[Future[T]] | AsyncGenerator[Future[T]],
        *,
        max_concurrent: NoValue | int = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        shield: bool = False,
    ) -> AsyncGenerator[Future[T], Any]:
        """
        A [classmethod][] iterator to get [Futures][async_kernel.caller.Future] as they complete.

        Args:
            items: Either a container with existing futures or generator of Futures.
            max_concurrent: The maximum number of concurrent futures to monitor at a time.
                This is useful when `items` is a generator utilising [async_kernel.caller.Caller.to_thread][].
                By default this will limit to `Caller.MAX_IDLE_POOL_INSTANCES`.
            shield: Shield existing items from cancellation.

        !!! tip

            1. Pass a generator if you wish to limit the number future jobs when calling to_thread/to_task etc.
            2. Pass a container with all [Futures][async_kernel.caller.Future] when the limiter is not relevant.
        """
        resume_event = REvent()
        future_done_event = REvent()
        done_futures: deque[Future[T]] = deque()
        futures: set[Future[T]] = set()
        done = False
        current_future = cls.current_future()
        if isinstance(items, set | list | tuple):
            max_concurrent_ = 0
        else:
            max_concurrent_ = cls.MAX_IDLE_POOL_INSTANCES if max_concurrent is NoValue else int(max_concurrent)

        def future_done(fut: Future[T]) -> None:
            done_futures.append(fut)
            future_done_event.set()

        async def iter_items():
            nonlocal done
            gen = items if isinstance(items, AsyncGenerator) else iter(items)
            try:
                while True:
                    fut = await anext(gen) if isinstance(gen, AsyncGenerator) else next(gen)
                    assert fut is not current_future, "Would result in deadlock"
                    fut.add_done_callback(future_done)
                    if not fut.done():
                        futures.add(fut)
                        if max_concurrent_ and (len(futures) == max_concurrent_):
                            resume_event.clear()
                            await resume_event

            except (StopAsyncIteration, StopIteration):
                return
            finally:
                done = True
                resume_event.set()
                future_done_event.set()

        fut_ = cls().call_soon(iter_items)
        try:
            while not done or futures:
                if done_futures:
                    fut = done_futures.popleft()
                    futures.discard(fut)
                    # Ensure all Future done callbacks are complete.
                    await fut.wait(result=False)
                    yield fut
                else:
                    if len(futures) < max_concurrent_:
                        resume_event.set()
                    future_done_event.clear()
                    await future_done_event

        finally:
            fut_.cancel()
            for fut in futures:
                fut.remove_done_callback(future_done)
                if not shield:
                    fut.cancel("Cancelled by as_completed")

    @classmethod
    async def wait(
        cls,
        items: Iterable[Future[T]],
        *,
        timeout: float | None = None,
        return_when: Literal["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"] = "ALL_COMPLETED",
    ) -> tuple[set[T], set[Future[T]]]:
        """
        A [classmethod][] to wait for the futures given by items to complete.

        Returns two sets of the futures: (done, pending).

        Args:
            items: An iterable of futures to wait for.
            timeout: The maximum time before returning.
            return_when: The same options as available for [asyncio.wait][].

        !!! example

            ```python
            done, pending = await asyncio.wait(items)
            ```

        !!! info

            - This does not raise a TimeoutError!
            - Futures that aren't done when the timeout occurs are returned in the second set.
        """
        done = set()
        if pending := set(items):
            with anyio.move_on_after(timeout):
                async for fut in cls.as_completed(pending.copy(), shield=True):
                    _ = (pending.discard(fut), done.add(fut))
                    if return_when == "FIRST_COMPLETED":
                        break
                    if return_when == "FIRST_EXCEPTION" and (fut.cancelled() or fut.exception()):
                        break
        return done, pending
