from __future__ import annotations

import contextlib
import contextvars
import functools
import inspect
import logging
import threading
import time
import weakref
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, cast, overload

import anyio
import sniffio
from anyio.streams.memory import MemoryObjectSendStream
from typing_extensions import override
from zmq import Context, Socket, SocketType

import async_kernel
from async_kernel.kernelspec import Backend
from async_kernel.typing import NoValue, PosArgsT, T
from async_kernel.utils import wait_thread_event

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import CoroutineType

    from anyio._core._synchronization import Event
    from anyio.abc import TaskGroup, TaskStatus
    from anyio.streams.memory import MemoryObjectSendStream

    from async_kernel.typing import P

__all__ = ["Caller", "Future", "FutureCancelledError", "InvalidStateError", "Lock"]


class FutureCancelledError(anyio.ClosedResourceError):
    "Used to indicate a `Future` is cancelled."


class InvalidStateError(RuntimeError):
    "An invalid state of a [Future][async_kernel.caller.Future]."


class Future(Awaitable[T]):
    """
    A class representing a future result modelled on [asyncio.Future][].

    This class provides an anyio compatible Future primitive. It is designed
    to work with `Caller` to enable thread-safe calling, setting and awaiting
    execution results.
    """

    __slots__ = [
        "__weakref__",
        "_anyio_event_done",
        "_cancel_scope",
        "_cancelled",
        "_done_callbacks",
        "_event_done",
        "_exception",
        "_result",
        "_setting_value",
        "thread",
    ]
    _result: T
    thread: threading.Thread
    "The thread in which the result is targeted to run."

    def __init__(self, thread: threading.Thread | None = None) -> None:
        self._event_done = threading.Event()
        self._exception = None
        self._anyio_event_done = None
        self.thread = thread or threading.current_thread()
        self._done_callbacks = []
        self._cancelled = False
        self._cancel_scope: anyio.CancelScope | None = None
        self._setting_value = False

    @override
    def __await__(self) -> Generator[Any, None, T]:
        return self.wait().__await__()

    def _set_value(self, mode: Literal["result", "exception"], value) -> None:
        if self._setting_value:
            raise InvalidStateError
        self._setting_value = True
        if self._cancelled:
            mode = "exception"
            value = self._make_cancelled_error()

        def set_value():
            if mode == "exception":
                self._exception = value
            else:
                self._result = value  # pyright: ignore[reportAttributeAccessIssue]
            self._event_done.set()
            if self._anyio_event_done:
                self._anyio_event_done.set()
            for cb in reversed(self._done_callbacks):
                try:
                    cb(self)
                except Exception:
                    pass

        if threading.current_thread() is not self.thread:
            try:
                Caller(thread=self.thread).call_direct(set_value)
            except RuntimeError:
                msg = f"The current thread is not {self.thread.name} and a `Caller` does not exist for that thread either."
                raise RuntimeError(msg) from None
        else:
            set_value()

    def _make_cancelled_error(self) -> FutureCancelledError:
        return FutureCancelledError(self._cancelled) if isinstance(self._cancelled, str) else FutureCancelledError()

    if TYPE_CHECKING:

        @overload
        async def wait(
            self, *, timeout: float | None = ..., shield: bool = False | ..., result: Literal[True] = True
        ) -> T: ...

        @overload
        async def wait(self, *, timeout: float | None = ..., shield: bool = ..., result: Literal[False]) -> None: ...

    async def wait(self, *, timeout: float | None = None, shield: bool = False, result: bool = True) -> T | None:
        """
        Wait for future to be done (thread-safe) returning the result if specified.

        Args:
            timeout: Timeout in seconds.
            shield: Shield the future from cancellation.
            result: Whether the result should be returned.
        """
        try:
            if not self.done():
                with anyio.fail_after(timeout):
                    if threading.current_thread() is self.thread:
                        if not self._anyio_event_done:
                            self._anyio_event_done = anyio.Event()
                        await self._anyio_event_done.wait()
                    else:
                        await wait_thread_event(self._event_done)
            return self.result() if result else None
        finally:
            if not self.done() and not shield:
                self.cancel("Cancelled with waiter cancellation.")

    if TYPE_CHECKING:

        @overload
        def wait_sync(
            self, *, timeout: float | None = ..., shield: bool = False | ..., result: Literal[True] = True
        ) -> T: ...

        @overload
        def wait_sync(self, *, timeout: float | None = ..., shield: bool = ..., result: Literal[False]) -> None: ...

    def wait_sync(self, *, timeout: float | None = None, shield: bool = False, result: bool = True) -> T | None:
        """
        Synchronously wait for future to be done (thread-safe) returning the result if specified.

        Args:
            timeout: Timeout in seconds.
            shield: Shield cancellation.
            result: Whether the result should be returned.
        """
        if self.thread in {threading.current_thread(), threading.main_thread()}:
            raise RuntimeError
        self._event_done.wait(timeout)
        if not self.done():
            if not shield:
                self.cancel("timeout from wait_sync")
            raise TimeoutError
        return self.result() if result else None

    def set_result(self, value: T) -> None:
        "Set the result (thread-safe using Caller)."
        self._set_value("result", value)

    def set_exception(self, exception: BaseException) -> None:
        "Set the exception (thread-safe using Caller)."
        self._set_value("exception", exception)

    def done(self) -> bool:
        """
        Returns True if the Future is done.

        Done means either that a result / exception is available."""
        return self._event_done.is_set()

    def add_done_callback(self, fn: Callable[[Self], object]) -> None:
        """
        Add a callback for when the callback is done (not thread-safe).

        If the Future is already done it will be scheduled for calling.

        The result of the future and done callbacks are always called for the futures thread.
        Callbacks are called in the reverse order in which they were added in the owning thread.
        """
        if not self.done():
            self._done_callbacks.append(fn)
        else:
            self.get_caller().call_direct(fn, self)

    def cancel(self, msg: str | None = None) -> bool:
        """
        Cancel the Future and schedule callbacks (thread-safe using Caller).

        Args:
            msg: The message to use when raising a FutureCancelledError.

        Returns if it has been cancelled.
        """
        if not self.done():
            if msg and isinstance(self._cancelled, str):
                msg = f"{self._cancelled}\n{msg}"
            self._cancelled = msg or self._cancelled or True
            if scope := self._cancel_scope:
                if threading.current_thread() is self.thread:
                    scope.cancel()
                else:
                    Caller(thread=self.thread).call_direct(self.cancel)
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

    def set_cancel_scope(self, scope: anyio.CancelScope) -> None:
        "Provide a cancel scope for cancellation."
        if self._cancelled or self._cancel_scope:
            raise InvalidStateError
        self._cancel_scope = scope

    def get_caller(self) -> Caller:
        "The the Caller the Future's thread corresponds."
        return Caller(thread=self.thread)


class Caller:
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
    "The number of `pool` instances to leave idle (See also[to_thread][async_kernel.Caller.to_thread])."
    MAX_BUFFER_SIZE = 1000
    "The default  maximum_buffer_size used in [queue_call][async_kernel.Caller.queue_call]."
    _instances: ClassVar[dict[threading.Thread, Self]] = {}
    __stack = None
    _outstanding = 0
    _to_thread_pool: ClassVar[deque[Self]] = deque()
    _pool_instances: ClassVar[weakref.WeakSet[Self]] = weakref.WeakSet()
    _queue_map: weakref.WeakKeyDictionary[Callable[..., Awaitable[Any]], MemoryObjectSendStream[tuple]]
    _taskgroup: TaskGroup | None = None
    _callers: deque[tuple[contextvars.Context, tuple[Future, float, float, Callable, tuple, dict]] | Callable[[], Any]]
    _callers_added: threading.Event
    _stopped_event: threading.Event
    _stopped = False
    _protected = False
    _running = False
    _future_var: contextvars.ContextVar[Future | None] = contextvars.ContextVar("_future_var", default=None)
    thread: threading.Thread
    "The thread in which the caller will run."
    backend: Backend
    "The `anyio` backend the caller is running in."
    log: logging.LoggerAdapter[Any]
    ""
    iopub_sockets: ClassVar[weakref.WeakKeyDictionary[threading.Thread, Socket]] = weakref.WeakKeyDictionary()
    iopub_url: ClassVar = "inproc://iopub"

    def __new__(
        cls,
        *,
        thread: threading.Thread | None = None,
        log: logging.LoggerAdapter | None = None,
        create: bool = False,
        protected: bool = False,
    ) -> Self:
        """
        Create the `Caller` instance for the current thread or retrieve an existing instance
        by passing the thread.

        The caller provides a way to execute synchronous code in a separate
        thread, and to call asynchronous code from synchronous code.

        Args:
            thread:
            log: Logger to use for logging messages.
            create: Whether to create a new instance if one does not exist for the current thread.
            protected : Whether the caller is protected from having its event loop closed.

        Returns
        -------
        Caller
            The `Caller` instance for the current thread.

        Raises
        ------
        RuntimeError
            If `create` is False and a `Caller` instance does not exist.
        """

        thread = thread or threading.current_thread()
        if not (inst := cls._instances.get(thread)):
            if not create:
                msg = f"A caller is not provided for {thread=}"
                raise RuntimeError(msg)
            inst = super().__new__(cls)
            inst.backend = Backend(sniffio.current_async_library())
            inst.thread = thread
            inst.log = log or logging.LoggerAdapter(logging.getLogger())
            inst._callers = deque()
            inst._callers_added = threading.Event()
            inst._protected = protected
            inst._queue_map = weakref.WeakKeyDictionary()
            cls._instances[thread] = inst
        return inst

    @override
    def __repr__(self) -> str:
        return f"Caller<{self.thread.name}>"

    async def __aenter__(self) -> Self:
        async with contextlib.AsyncExitStack() as stack:
            self._running = True
            self._stopped_event = threading.Event()
            self._taskgroup = tg = await stack.enter_async_context(anyio.create_task_group())
            await tg.start(self._server_loop, tg)
            self.__stack = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb) -> None:
        if self.__stack is not None:
            self.stop(force=True)
            await self.__stack.__aexit__(exc_type, exc_value, exc_tb)

    async def _server_loop(self, tg: TaskGroup, task_status: TaskStatus[None]) -> None:
        socket = Context.instance().socket(SocketType.PUB)
        socket.linger = 500
        socket.connect(self.iopub_url)
        try:
            self.iopub_sockets[self.thread] = socket
            task_status.started()
            while not self._stopped:
                while len(self._callers):
                    job = self._callers.popleft()
                    if isinstance(job, Callable):
                        try:
                            job()
                        except Exception as e:
                            self.log.exception("Simple call failed", exc_info=e)
                    else:
                        context, args = job
                        context.run(tg.start_soon, self._wrap_call, *args)
                    self._callers_added.clear()
                await wait_thread_event(self._callers_added)
        finally:
            self._running = False
            for job in self._callers:
                if not callable(job):
                    job[1][0].set_exception(FutureCancelledError())
            socket.close()
            self.iopub_sockets.pop(self.thread, None)
            self._stopped_event.set()
            tg.cancel_scope.cancel()

    async def _wrap_call(
        self,
        fut: Future[T],
        starttime: float,
        delay: float,
        func: Callable[..., T | Awaitable[T]],
        args: tuple,
        kwargs: dict,
    ) -> None:
        self._future_var.set(fut)
        if fut.cancelled():
            fut.set_result(cast("T", None))  # This will cancel
            return
        try:
            with anyio.CancelScope() as scope:
                fut.set_cancel_scope(scope)
                try:
                    if (delay_ := delay - time.monotonic() + starttime) > 0:
                        await anyio.sleep(float(delay_))
                    result = func(*args, **kwargs) if callable(func) else func  # pyright: ignore[reportAssignmentType]
                    if inspect.isawaitable(result) and result is not fut:
                        result: T = await result
                    if fut.cancelled() and not scope.cancel_called:
                        scope.cancel()
                    self._outstanding -= 1  # update first for _to_thread_on_done
                    fut.set_result(result)
                except anyio.get_cancelled_exc_class():
                    fut.cancel()
                    self._outstanding -= 1  # update first for _to_thread_on_done
                    fut.set_result(cast("T", None))  # This will cancel
                except Exception as e:
                    self._outstanding -= 1  # update first for _to_thread_on_done
                    fut.set_exception(e)
        except Exception as e:
            self.log.exception("Calling func %s failed", func, exc_info=e)

    def _to_thread_on_done(self, _) -> None:
        if not self._stopped:
            if (len(self._to_thread_pool) < self.MAX_IDLE_POOL_INSTANCES) or self._outstanding:
                self._to_thread_pool.append(self)
            else:
                self.stop()

    def _check_in_thread(self):
        if self.thread is not threading.current_thread():
            msg = "This function must be called from its own thread. Tip: Use `call_direct` to call this method from another thread."
            raise RuntimeError(msg)

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

    def stop(self, *, force=False) -> None:
        """
        Stop the caller, cancelling all pending tasks and close the thread.

        If the instance is protected, this is no-op unless force is used.
        """
        if self._protected and not force:
            return
        self._stopped = True
        for sender in self._queue_map.values():
            sender.close()
        self._queue_map.clear()
        self._callers_added.set()
        self._instances.pop(self.thread, None)
        if self in self._to_thread_pool:
            self._to_thread_pool.remove(self)
        if self.thread is not threading.current_thread():
            self._stopped_event.wait()

    def call_later(
        self, delay: float, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[T]:
        """
        Schedule func to be called in caller's event loop copying the current context.

        Args:
            func: The function (awaitables permitted, though discouraged).
            delay: The minimum delay to add between submission and execution.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.
        """
        if self._stopped:
            raise anyio.ClosedResourceError
        fut: Future[T] = Future(thread=self.thread)
        if threading.current_thread() is self.thread and (tg := self._taskgroup):
            tg.start_soon(self._wrap_call, fut, time.monotonic(), delay, func, args, kwargs)
        else:
            self._callers.append((contextvars.copy_context(), (fut, time.monotonic(), delay, func, args, kwargs)))
            self._callers_added.set()
        self._outstanding += 1
        return fut

    def call_soon(self, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """
        Schedule func to be called in caller's event loop copying the current context.

        Args:
            func: The function (awaitables permitted, though discouraged).
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.
        """
        return self.call_later(0, func, *args, **kwargs)

    def call_direct(self, func: Callable[P, Any], /, *args: P.args, **kwargs: P.kwargs) -> None:
        """
        Schedule func to be called in caller's event loop directly.

        The call is made without copying the context and does not use a future.

        Args:
            func: The function (awaitables permitted, though discouraged).
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        ??? warning

            **Use this method for lightweight calls only.**
        """
        self._callers.append(functools.partial(func, *args, **kwargs))
        self._callers_added.set()

    def queue_exists(self, func: Callable) -> bool:
        "Returns True if an execution queue exists for `func`."
        return func in self._queue_map

    if TYPE_CHECKING:

        @overload
        def queue_call(
            self,
            func: Callable[[*PosArgsT], Awaitable[Any]],
            /,
            *args: *PosArgsT,
            max_buffer_size: NoValue | int = NoValue,  # pyright: ignore[reportInvalidTypeForm]
            wait: Literal[True],
        ) -> CoroutineType[Any, Any, None]: ...
        @overload
        def queue_call(
            self,
            func: Callable[[*PosArgsT], Awaitable[Any]],
            /,
            *args: *PosArgsT,
            max_buffer_size: NoValue | int = NoValue,  # pyright: ignore[reportInvalidTypeForm]
            wait: Literal[False] | Any = False,
        ) -> None: ...

    def queue_call(
        self,
        func: Callable[[*PosArgsT], Awaitable[Any]],
        /,
        *args: *PosArgsT,
        max_buffer_size: NoValue | int = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        wait: bool = False,
    ) -> CoroutineType[Any, Any, None] | None:
        """
        Queue the execution of `func` with the arguments `*args` in a queue unique to it (not thread-safe).

        The args are added to a queue associated with the provided `func`. If queue does not already exist for
        func, a new queue is created with a specified maximum buffer size. The arguments are then sent to the queue,
        and an `execute_loop` coroutine is started to consume the queue and execute the function with the received
        arguments.  Exceptions during execution are caught and logged.

        Args:
            func: The asynchronous function to execute.
            *args: The arguments to pass to the function.
            max_buffer_size: The maximum buffer size for the queue. If NoValue, defaults to [async_kernel.Caller.MAX_BUFFER_SIZE].
            wait: Set as True to return a coroutine that will return once the request is sent.
                Use this to prevent experiencing exceptions if the buffer is full.

        !!! info

            The queue will stay open until one of the following occurs.

            1. It explicitly closed with the method `queue_close`.
            1. All strong references are lost the function/method.

        """
        self._check_in_thread()
        if not (sender := self._queue_map.get(func)):
            max_buffer_size = self.MAX_BUFFER_SIZE if max_buffer_size is NoValue else max_buffer_size
            sender, queue = anyio.create_memory_object_stream[tuple[*PosArgsT]](max_buffer_size=max_buffer_size)

            async def execute_loop():
                try:
                    with contextlib.suppress(anyio.get_cancelled_exc_class()):
                        async with queue as receive_stream:
                            async for args in receive_stream:
                                if func not in self._queue_map:
                                    break
                                try:
                                    await func(*args)
                                except Exception as e:
                                    self.log.exception("Execution %f failed", func, exc_info=e)
                finally:
                    self._queue_map.pop(func, None)

            self._queue_map[func] = sender
            self.call_soon(execute_loop)
        return sender.send(args) if wait else sender.send_nowait(args)

    def queue_close(self, func: Callable) -> None:
        """
        Close the execution queue associated with func (thread-safe).

        Args:
            func: The queue of the function to close.
        """
        if sender := self._queue_map.pop(func, None):
            self.call_direct(sender.close)

    @classmethod
    def stop_all(cls, *, _stop_protected: bool = False) -> None:
        """
        A classmethod to stop all un-protected callers.

        Args:
            _stop_protected: A private argument to shutdown protected instances.
        """
        for caller in tuple(reversed(cls._instances.values())):
            caller.stop(force=_stop_protected)

    @classmethod
    def get_instance(cls, name: str | None = "MainThread", *, create: bool = False) -> Self:
        """
        A classmethod that gets an instance by name, possibly starting a new instance.

        Args:
            name: The name to identify the caller.
            create: Create a new instance if one with the corresponding name does not already exist.
        """
        for thread in cls._instances:
            if thread.name == name:
                return cls._instances[thread]
        if create:
            return cls.start_new(name=name)
        msg = f"A Caller was not found for {name=}."
        raise RuntimeError(msg)

    @classmethod
    def to_thread(cls, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """A classmethod to call func in a separate thread see also [to_thread_by_name][async_kernel.Caller.to_thread_by_name]."""
        return cls.to_thread_by_name(None, func, *args, **kwargs)

    @classmethod
    def to_thread_by_name(
        cls, name: str | None, func: Callable[P, T | Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[T]:
        """
        A classmethod to call func in the thread specified by name.

        Args:
            name: The name of the `Caller`. A new `Caller` is created if an instance corresponding to name  [^notes].

                [^notes]:  'MainThread' is special name corresponding to the main thread.
                    A `RuntimeError` will be raised if a Caller does not exist for the main thread.

            func: The function to call. If it returns an awaitable, the awaitable will be awaited.
                Passing a coroutine as `func` discourage, but will be awaited.

            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        Returns:
            A future that can be awaited for the  result of func.
        """
        caller = (
            cls._to_thread_pool.popleft()
            if not name and cls._to_thread_pool
            else cls.get_instance(name=name, create=True)
        )
        fut = caller.call_soon(func, *args, **kwargs)
        if not name:
            cls._pool_instances.add(caller)
            fut.add_done_callback(caller._to_thread_on_done)
        return fut

    @classmethod
    def start_new(
        cls,
        *,
        backend: Backend | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        log: logging.LoggerAdapter | None = None,
        name: str | None = None,
        protected: bool = False,
        backend_options: dict | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
    ) -> Self:
        """
        Start a new thread with a new Caller open in the context of anyio event loop.

        A new thread and caller is always started and ready to start new jobs as soon as it is returned.

        Args:
            backend: The backend to use for the anyio event loop (anyio.run). Defaults to the backend from where it is called.
            log: A logging adapter to use for debug messages.
            protected: When True, the caller will not shutdown unless shutdown is called with `force=True`.
            backend_options: Backend options for [anyio.run][]. Defaults to `Kernel.backend_options`.
        """

        def anyio_run_caller() -> None:
            async def caller_context() -> None:
                nonlocal caller
                async with cls(log=log, create=True, protected=protected) as caller:
                    ready_event.set()
                    with contextlib.suppress(anyio.get_cancelled_exc_class()):
                        await anyio.sleep_forever()

            anyio.run(caller_context, backend=backend_, backend_options=backend_options)

        assert name not in [t.name for t in cls._instances], f"{name=} already exists!"
        backend_ = Backend(backend if backend is not NoValue else sniffio.current_async_library())
        if backend_options is NoValue:
            backend_options = async_kernel.Kernel().anyio_backend_options.get(backend_)
        caller = cast("Self", object)
        ready_event = threading.Event()
        thread = threading.Thread(target=anyio_run_caller, name=name, daemon=True)
        thread.start()
        ready_event.wait()
        assert isinstance(caller, cls)
        return caller

    @classmethod
    def current_future(cls) -> Future[Any] | None:
        """A classmethod that returns the current future when called from inside a function scheduled by Caller."""
        return cls._future_var.get()

    @classmethod
    def all_callers(cls, running_only: bool = True) -> list[Caller]:
        """
        A classmethod to get a list of the callers.

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
        A classmethod iterator to get [Futures][async_kernel.caller.Future] as they complete.

        Args:
            items: Either a container with existing futures or generator of Futures.
            max_concurrent: The maximum number of concurrent futures to monitor at a time.
                This is useful when `items` is a generator utilising Caller.to_thread.
                By default this will limit to `Caller.MAX_IDLE_POOL_INSTANCES`.
            shield: Shield existing items from cancellation.

        !!! tip

            1. Pass a generator should you wish to limit the number future jobs when calling to_thread/to_task etc.
            2. Pass a set/list/tuple to ensure all get monitored at once.
        """
        event_future_ready = threading.Event()
        has_result: deque[Future[T]] = deque()
        futures: set[Future[T]] = set()
        done = False
        resume: Event | None = cast("anyio.Event | None", None)
        current_future = cls.current_future()
        if isinstance(items, set | list | tuple):
            max_concurrent_ = 0
        else:
            max_concurrent_ = cls.MAX_IDLE_POOL_INSTANCES if max_concurrent is NoValue else int(max_concurrent)

        def _on_done(fut: Future[T]) -> None:
            has_result.append(fut)
            event_future_ready.set()

        async def iter_items():
            nonlocal done, resume
            gen = items if isinstance(items, AsyncGenerator) else iter(items)
            try:
                while True:
                    fut = await anext(gen) if isinstance(gen, AsyncGenerator) else next(gen)
                    if fut is not current_future:
                        futures.add(fut)
                        if fut.done():
                            has_result.append(fut)
                            event_future_ready.set()
                        else:
                            fut.add_done_callback(_on_done)
                        if max_concurrent_ and len(futures) == max_concurrent_:
                            resume = anyio.Event()
                            await resume.wait()
            except (StopAsyncIteration, StopIteration):
                return
            finally:
                done = True
                event_future_ready.set()

        fut = cls().call_soon(iter_items)
        try:
            while futures or not done:
                if has_result:
                    event_future_ready.clear()
                    fut = has_result.popleft()
                    futures.discard(fut)
                    yield fut
                    if resume:
                        resume.set()
                    continue
                if not has_result:
                    await wait_thread_event(event_future_ready)
        finally:
            fut.cancel()
            for fut in futures:
                fut.remove_done_callback(_on_done)
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
        A classmethod to wait for the futures given by items to complete.

        Returns two sets of the futures: (done, pending).

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
                async for fut in cls.as_completed(items, shield=True):
                    pending.discard(fut)
                    done.add(fut)
                    if return_when == "FIRST_COMPLETED":
                        break
                    if return_when == "FIRST_EXCEPTION" and (fut.cancelled() or fut.exception()):
                        break
        return done, pending


class Lock:
    """
    An asynchronous context reentrant capable lock.

    !!! example

        ```python
        # Inside a coroutine running inside a Caller thread.

        lock = Lock(reentrant=True)  # a reentrant lock
        async with lock:
            pass
        ```

    !!! note

        - Reentrant is defined here as: *reentrant* within the scope of the calling [context][contextvars.ContextVar] meaning
            the lock is shared in the async context ([async_kernel.caller.Caller.call_soon][], [async_kernel.caller.Caller.call_later][])
            and threads ([async_kernel.caller.Caller.to_thread][]).
        - There is no requirement to exit the context in the order of entry.

    """

    __slots__ = ["_count", "_ctx", "_ctx_count", "_ctx_lock", "_queue", "_reentrant"]

    def __init__(self, *, reentrant=False):
        self._count = 0
        self._ctx: contextvars.ContextVar[int] = contextvars.ContextVar(f"Lock:{id(self)}", default=0)
        self._ctx_count = 0
        self._ctx_lock = 0
        self._queue: deque[tuple[int, Future]] = deque()
        self._reentrant = reentrant

    async def __aenter__(self) -> Self:
        if not (ctx := self._ctx.get()):
            self._ctx_count = ctx = self._ctx_count + 1
            self._ctx.set(ctx)
        if not self._count or (self._reentrant and ctx == self._ctx_lock):
            self._ctx_lock = ctx
            self._count += 1
            return self
        fut: Future[Any] = Future()
        self._queue.append((ctx, fut))
        try:
            await fut
            self._count += 1
            return self
        finally:
            self._queue.remove((ctx, fut))

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._count -= 1
        if self._count == 0:
            ctx = 0
            for ctx_, fut in tuple(self._queue):
                if not fut.done():
                    if ctx == 0:
                        self._ctx_lock = ctx = ctx_
                    if ctx_ == ctx:
                        fut.set_result(None)
                        if not self._reentrant:
                            break

    def locked(self) -> bool:
        "Returns True when locked."
        return bool(self._count)
