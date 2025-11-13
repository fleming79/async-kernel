from __future__ import annotations

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
import anyio.from_thread
from aiologic import Event
from aiologic.lowlevel import create_async_event, current_async_library
from anyio.lowlevel import EventLoopToken, current_token
from typing_extensions import override
from zmq import Context, Socket, SocketType

import async_kernel
from async_kernel.kernelspec import Backend
from async_kernel.typing import CallerStartNewOptions, NoValue, T

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import CoroutineType

    from anyio.abc import TaskGroup, TaskStatus
    from anyio.lowlevel import EventLoopToken

    from async_kernel.typing import P

__all__ = ["Caller", "Future", "FutureCancelledError", "InvalidStateError"]

truncated_rep = reprlib.Repr()
truncated_rep.maxlevel = 1
truncated_rep.maxother = 100
truncated_rep.fillvalue = "…"


def noop():
    pass


class FutureCancelledError(anyio.ClosedResourceError):
    "Used to indicate a [Future][async_kernel.caller.Future] is cancelled."


class InvalidStateError(RuntimeError):
    "An invalid state of a [Future][async_kernel.caller.Future]."


class Future(Awaitable[T]):
    """
    A future represents a computation that may complete asynchronously, providing a result or exception in the future.

    This class implements the Awaitable protocol, allowing it to be awaited in async code in thread or event loop.
    It supports cancellation, result retrieval, exception handling, and attaching callbacks to be invoked upon completion.

    Notes:
        - The future is not considered done until set_result or set_exception is called.
        - If cancelled, the result is replaced with a FutureCancelledError.
        - A callback can be set to handle cancellation with `set_canceller`.
        - Its result can also be synchronously waited (blocking) using `wait_sync`.
    """

    __slots__ = ["__weakref__", "_cancelled", "_canceller", "_done", "_done_callbacks", "_exception", "_result"]

    REPR_OMIT: ClassVar[set[str]] = {"func", "args", "kwargs"}
    "Keys of metadata to omit when creating a repr of the future."

    _metadata_mappings: ClassVar[dict[int, dict[str, Any]]] = {}
    "A mapping of future's id its metadata."

    _cancelled: str
    _canceller: Callable[[str | None], Any]
    _exception: Exception
    _done: bool
    _result: T

    def __init__(self, **metadata) -> None:
        self._done_callbacks: deque[Callable[[Self], Any]] = deque()
        self._metadata_mappings[id(self)] = metadata
        self._done = False

    def __del__(self):
        self._metadata_mappings.pop(id(self), None)

    @override
    def __repr__(self) -> str:
        rep = "<Future" + (" ⛔" if self.cancelled() else "") + (" 🏁" if self._done else " 🏃")
        rep = f"{rep} at {id(self)}"
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

    def _set_done(self, mode: Literal["result", "exception"], value) -> None:
        if self._done:
            raise InvalidStateError
        self._done = True
        setattr(self, "_" + mode, value)
        while self._done_callbacks:
            cb = self._done_callbacks.pop()
            try:
                cb(self)
            except Exception:
                pass

    def _make_cancelled_error(self) -> FutureCancelledError:
        return FutureCancelledError(self._cancelled)

    @property
    def metadata(self) -> dict[str, Any]:
        """
        The metadata passed as keyword arguments to the future during creation.
        """
        return self._metadata_mappings[id(self)]

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
            result: Whether the result should be returned (use `result=False` to avoid exceptions raised by [Future.result][]).
        """
        try:
            if not self._done or self._done_callbacks:
                event = create_async_event()
                self._done_callbacks.appendleft(lambda _: event.set())
                with anyio.fail_after(timeout):
                    if not self._done or self._done_callbacks:
                        await event
            return self.result() if result else None
        finally:
            if not self._done and not shield:
                self.cancel("Cancelled with waiter cancellation.")

    if TYPE_CHECKING:

        @overload
        def wait_sync(self, *, timeout: float | None = ..., result: Literal[True] = True) -> T: ...

        @overload
        def wait_sync(self, *, timeout: float | None = ..., result: Literal[False]) -> None: ...

    def wait_sync(self, *, timeout: float | None = None, result: bool = True) -> T | None:
        """
        Wait for the result to be done synchronously (thread-safe) blocking the current thread.

        Args:
            timeout: Timeout in seconds.
            result: Whether the result should be returned (use `result=False` to avoid exceptions raised by [Future.result][]).

        Raises:
            TimeoutError: When the timeout expires and a result has not been set.

        Warning:
            **Blocking the thread in which the result is set will result in deadlock.**
        """
        if not self._done:
            done = Event()
            self.add_done_callback(lambda _: done.set())
            if not self._done:
                done.wait(timeout)
            if not self._done:
                msg = f"Timeout waiting for {self}"
                raise TimeoutError(msg)

        return self.result() if result else None

    def set_result(self, value: T) -> None:
        "Set the result (thread-safe)."
        self._set_done("result", value)

    def set_exception(self, exception: BaseException) -> None:
        "Set the exception (thread-safe)."
        self._set_done("exception", exception)

    def done(self) -> bool:
        """
        Returns True if the future has a result.

        Done means either that a result / exception is available or that cancellation is complete.
        """
        return self._done

    def add_done_callback(self, fn: Callable[[Self], Any]) -> None:
        """
        Add a callback for when the future is done (not thread-safe).

        If the future is already done it will called immediately.
        """
        if not self._done:
            self._done_callbacks.append(fn)
        else:
            fn(self)

    def cancel(self, msg: str | None = None) -> bool:
        """
        Cancel the Future.

        Args:
            msg: The message to use when cancelling.

        Notes:
            - Cancellation cannot be undone.
            - The future will not be *done* until either [Future.set_result][] or [Future.set_exception][] is called.

        Returns: If it has been cancelled.
        """
        if not self._done:
            cancelled = getattr(self, "_cancelled", "")
            if msg and isinstance(cancelled, str):
                msg = f"{cancelled}\n{msg}"
            self._cancelled = msg or cancelled
            if canceller := getattr(self, "_canceller", None):
                canceller(msg)
        return self.cancelled()

    def cancelled(self) -> bool:
        """Return True if the future is cancelled."""
        return isinstance(getattr(self, "_cancelled", None), str)

    def result(self) -> T:
        """
        Return the result of the future.

        Raises:
            FutureCancelledError: If the future has been cancelled.
            InvalidStateError: If the future isn't done yet.
        """
        if not self._done and not self.cancelled():
            raise InvalidStateError
        if e := self.exception():
            raise e
        return self._result

    def exception(self) -> BaseException | None:
        """
        Return the exception that was set on the future.

        Raises:
            FutureCancelledError: If the future has been cancelled.
            InvalidStateError: If the future isn't done yet.
        """
        if hasattr(self, "_cancelled"):
            raise self._make_cancelled_error()
        if not self._done:
            raise InvalidStateError
        return getattr(self, "_exception", None)

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
            canceller: A callback that performs the cancellation of the future.
                - It must accept the cancellation message as the first argument.
                - The cancellation call is not thread-safe.

        Notes:
            - `set_result` must be called to mark the future as completed.

        Example:
            ```python
            fut = Future()
            fut.cancel()
            assert not fut.done()
            fut.set_canceller(lambda msg: fut.set_result(None))
            assert fut.done()
            ```
        """
        if self._done or hasattr(self, "_canceller"):
            raise InvalidStateError
        self._canceller = canceller
        if self.cancelled():
            self.cancel()


class Caller(anyio.AsyncContextManagerMixin):
    """
    Caller is a task scheduler for running functions in a dedicated thread with an AnyIO event loop.

    This class manages the execution of callables in a thread-safe manner, providing mechanisms for
    scheduling, queuing, and managing the lifecycle of tasks and their associated threads and event loops.
    It supports advanced features such as delayed execution, per-function queues, cancellation, and
    specification of the AnyIO supported backend.

    Key Features:
        - One Caller instance per thread, accessible via class methods.
        - Thread-safe scheduling of synchronous and asynchronous functions.
        - Support for delayed and immediate execution (`call_later`, `call_soon`).
        - Per-function execution queues with lifecycle management (`queue_call`, `queue_close`).
        - Integration with AnyIO's async context management and task groups.
        - Mechanisms for stopping, protecting, and pooling Caller instances.
        - Utilities for running functions in separate threads (`to_thread`, `to_thread_advanced`).
        - Methods for waiting on and iterating over multiple futures as they complete.
        - IOpub socket per Caller instance.

    Usage:
        - Use `Caller.get_instance()` to get or create a Caller for the 'MainThread' or a named thread.
        - Use `call_soon`, `call_later`, or `schedule_call` to schedule work.
        - Use `queue_call` for per-function task queues.
        - Use `to_thread` to run work in a separate thread.
        - Use `as_completed` and `wait` to manage multiple Futures.
        - Use `async with Caller(create=True) = caller:` start the caller inside a coroutine.

    Raises:
        RuntimeError: For invalid operations such as duplicate Caller creation or missing instances.
        anyio.ClosedResourceError: When scheduling on a stopped Caller.

    Notes:
        - It is safe to use the underlying libraries taskgroups
        - [aiologic](https://aiologic.readthedocs.io/latest/) provides thread-safe synchronisation primiates for working across threads.
        - Once a caller is stopped it cannot be restarted.
    """

    MAX_IDLE_POOL_INSTANCES = 10
    "The number of `pool` instances to leave idle (See also [to_thread][async_kernel.Caller.to_thread])."

    _instances: ClassVar[dict[threading.Thread, Self]] = {}
    _to_thread_pool: ClassVar[deque[Self]] = deque()
    _backend: Backend
    _queue_map: dict[int, Future]
    _queue: deque[tuple[contextvars.Context, Future] | Callable[[], Any]]
    _thread: threading.Thread
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

        Warning:
            Only use `create` when the intending to enter with an async context to run it.

            For example:
                ```python
                async with Caller(create=True):
                    anyio.sleep_forever()
                ```

        Notes:
            - [Caller.get_instance][] is the recommended way to get a running caller.
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
            inst._resume = noop
            inst._protected = protected
            inst._queue_map = {}
            cls._instances[thread] = inst
        return inst

    @override
    def __repr__(self) -> str:
        return f"Caller<{self.name} {'🏃' if self.running else ('🏁 stopped' if self.stopped else '❗ not running')}>"

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        if self._stopped:
            msg = f"Already stopped and restarting is not allowed: {self}"
            raise RuntimeError(msg)
        self._backend = Backend(current_async_library())
        self._running = True
        self._stopped_event = Event()
        async with anyio.create_task_group() as tg:
            try:
                await tg.start(self._scheduler, tg)
                yield self
            finally:
                self.stop(force=True)

    async def _scheduler(self, tg: TaskGroup, task_status: TaskStatus[None]) -> None:
        """
        Asynchronous scheduler coroutine responsible for managing and executing tasks from an internal queue.

        This method sets up a PUB socket for sending iopub messages, processes queued tasks (either callables or tuples with runnables),
        and handles coroutine execution. It waits for new tasks when the queue is empty and ensures proper cleanup and exception
        handling on shutdown.

        Args:
            tg: The task group used to manage concurrent tasks.
            task_status: Used to signal when the scheduler has started.

        Raises:
            Exception: Logs and handles exceptions raised during direct callable execution.
            FutureCancelledError: Sets this exception on pending futures in the queue upon shutdown.
        """
        socket = Context.instance().socket(SocketType.PUB)
        socket.linger = 500
        socket.connect(self.iopub_url)
        self.iopub_sockets[self.thread] = socket
        task_status.started()
        try:
            while not self._stopped:
                if self._queue:
                    item, result = self._queue.popleft(), None
                    if callable(item):
                        try:
                            result = item()
                            if inspect.iscoroutine(result):
                                await result
                        except Exception as e:
                            self.log.exception("Direct call failed", exc_info=e)
                    else:
                        item[0].run(tg.start_soon, self._caller, item[1])
                    del item, result
                else:
                    event = create_async_event()
                    self._resume = event.set
                    if not self._stopped and not self._queue:
                        await event
                    self._resume = noop
        finally:
            self._running = False
            for item in self._queue:
                if isinstance(item, tuple):
                    item[1].set_exception(FutureCancelledError())
            socket.close()
            self.iopub_sockets.pop(self.thread, None)
            self._stopped_event.set()
            tg.cancel_scope.cancel()

    async def _caller(self, fut: Future) -> None:
        """
        Asynchronously executes the function associated with the given Future, handling cancellation, delays, and exceptions.

        Args:
            fut: The Future object containing metadata about the function to execute, its arguments, and execution state.

        Workflow:
            - Sets the current future in a context variable.
            - If the future is cancelled before starting, sets a FutureCancelledError.
            - Otherwise, enters a cancellation scope:
                - Registers a canceller for the future.
                - Waits for a specified delay if present in metadata.
                - Calls the function (sync or async) with provided arguments.
                - Sets the result or exception on the future as appropriate.
            - Handles cancellation and other exceptions, logging errors as needed.
            - Resets the context variable after execution.
        """
        md = fut.metadata
        token = self._future_var.set(fut)
        try:
            if fut.cancelled():
                if not fut.done():
                    fut.set_exception(FutureCancelledError("Cancelled before started."))
            else:
                with anyio.CancelScope() as scope:
                    fut.set_canceller(lambda msg: self.call_direct(scope.cancel, msg))
                    # Call later.
                    if (delay := md.get("delay")) and ((delay := delay - time.monotonic() + md["start_time"]) > 0):
                        await anyio.sleep(delay)
                    # Call now.
                    try:
                        result = md["func"](*md["args"], **md["kwargs"])
                        if inspect.iscoroutine(result):
                            result = await result
                        fut.set_result(result)
                    # Cancelled.
                    except anyio.get_cancelled_exc_class() as e:
                        if not fut.cancelled():
                            fut.cancel()
                        fut.set_exception(e)
                    # Catch exceptions.
                    except Exception as e:
                        fut.set_exception(e)
        except Exception as e:
            fut.set_exception(e)
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

    def stop(self, *, force=False) -> None:
        """
        Stop the caller, cancelling all pending tasks and close the thread.

        If the instance is protected, this is no-op unless force is used.
        """
        if self._protected and not force:
            return
        self._stopped = True
        self._instances.pop(self.thread, None)
        for func in tuple(self._queue_map):
            self.queue_close(func)
        self._resume()
        if self in self._to_thread_pool:
            self._to_thread_pool.remove(self)
        if self._running and self.thread is not threading.current_thread():
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

        The methods [call_soon][Caller.call_soon] and [call_later][Caller.call_later]
        use this method in the background,  they should be used in preference to this method since they provide type hinting for the arguments.

        Args:
            func: The function to be called. If it returns a coroutine, it will be awaited and its result will be returned.
            args: Arguments corresponding to in the call to  `func`.
            kwargs: Keyword arguments to use with in the call to `func`.
            context: The context to use, if not provided the current context is used.
            **metadata: Additional metadata to store in the future.
        """
        if self._stopped:
            msg = f"{self} is stopped!"
            raise RuntimeError(msg)
        fut = Future(func=func, args=args, kwargs=kwargs, caller=self, **metadata)
        self._queue.append((context or contextvars.copy_context(), fut))
        self._resume()
        return fut

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

        Info:
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

        Warning:

            **Use this method for lightweight calls only!**

        """
        self._queue.append(functools.partial(func, *args, **kwargs))
        self._resume()

    def queue_get(self, func: Callable) -> Future[Never] | None:
        """Returns future for `func` where the queue is running.

        Warning:
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
        Queue the execution of `func` in a queue unique to it and the caller instance (thread-safe).

        Args:
            func: The function.
            *args: Arguments to use with `func`.
            **kwargs: Keyword arguments to use with `func`.

        Notes:
            - The queue executor loop will stay open until one of the following occurs:
                1. The method [Caller.queue_close][] is called with `func`.
                2. If `func` is a method is deleted and garbage collected (using [weakref.finalize][]).
            - The [context][contextvars.Context] of the initial call is is used for subsequent queue calls.
        """
        key = hash(func)
        if not (fut_ := self._queue_map.get(key)):
            queue = deque()
            with contextlib.suppress(TypeError):
                weakref.finalize(func.__self__ if inspect.ismethod(func) else func, lambda: self.queue_close(key))

            async def queue_loop(key: int, queue: deque) -> None:
                fut = self.current_future()
                assert fut
                try:
                    while True:
                        if queue:
                            item, result = queue.popleft(), None
                            try:
                                result = item[0](*item[1], **item[2])
                                if inspect.iscoroutine(object=result):
                                    await result
                            except (anyio.get_cancelled_exc_class(), Exception) as e:
                                if fut.cancelled():
                                    raise
                                self.log.exception("Execution %f failed", item, exc_info=e)
                            finally:
                                del item, result
                            await anyio.sleep(0)
                        else:
                            event = create_async_event()
                            fut.metadata["resume"] = event.set
                            if not queue:
                                await event
                            fut.metadata.pop("resume")
                finally:
                    self._queue_map.pop(key)

            self._queue_map[key] = fut_ = self.call_soon(queue_loop, key=key, queue=queue)
        fut_.metadata["kwargs"]["queue"].append((func, args, kwargs))
        if resume := fut_.metadata.get("resume"):
            resume()

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
    def get_instance(cls, *, create: bool = True, **kwargs: Unpack[CallerStartNewOptions]) -> Self:
        """
        Retrieve an existing instance of the class based on the provided 'name' or 'thread', or create a new one if specified.

        Args:
            create: If True or NoValue (default), a new instance will be created if no matching instance is found.
            **kwargs: Additional keyword arguments used to identify or create the instance. Common options include 'name' and 'thread'.

        Returns:
            Self: An existing or newly created instance of the class.

        Raises:
            RuntimeError: If no matching instance is found and 'create' is set to False.

        Notes:
            - If both 'name' and 'thread' are provided, the method returns the first matching instance.
            - If no matching instance is found and 'create' is True or NoValue, a new instance is created using the provided kwargs.
            - If 'kwargs' is empty when creating a new instance, the main thread is used by default.
        """
        name, thread = kwargs.get("name"), kwargs.get("thread")
        for caller in cls._instances.values():
            if caller.thread == thread or caller.name == name:
                return caller
        if create:
            if not kwargs:
                kwargs["thread"] = threading.main_thread()
            return cls.start_new(**kwargs)
        msg = f"A Caller was not found for {kwargs=}."
        raise RuntimeError(msg)

    @classmethod
    def to_thread(
        cls,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        """
        A [classmethod][] to call func in a separate thread.

        Args:
            func: The function.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        Notes:
            - A minimum number of caller instances are retained for this method.
            - Async code run inside func should use taskgroups for creating task.

        See also:
            - [Caller.to_thread_advanced][]
        """
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
            options: Options to pass to [Caller.start_new][].
            func: The function.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        Returns:
            A future that can be awaited for the  result of func.

        Raises:
            ValueError: When a name is not supplied.
        Notes:
            - When `options == {"name": None}` the caller is associated with a pool of workers.
        """
        caller = None
        if pool := options == {"name": None}:
            try:
                caller = cls._to_thread_pool.popleft()
            except IndexError:
                pass
        elif not options.get("name"):
            msg = "A name was not provided in {options=}."
            raise ValueError(msg)
        if caller is None:
            caller = cls.get_instance(create=True, **options)
        fut = caller.call_soon(func, *args, **kwargs)
        if pool:

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
        thread: threading.Thread | None = None,
        log: logging.LoggerAdapter | None = None,
        backend: Backend | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        protected: bool = False,
        backend_options: dict | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        daemon: bool = True,
        token: EventLoopToken | None = None,
    ) -> Self:
        """
        A [classmethod][] that creates and starts a new caller instance, optionally in a new thread, with the specified configuration.

        Args:
            name: The name to assign to the new thread/caller. If not provided, defaults to the thread's name.
            thread: An existing thread to associate with the caller. If not provided, a new thread is created.
            log: A LoggerAdapter to use for this caller instance.
            backend: The async backend to use (e.g., 'asyncio', 'trio'). If not specified, it is inferred.
            protected: Whether the caller should be protected from certain interruptions. Defaults to False.
            backend_options: Additional options for the async backend. If not specified, inferred from kernel.
            daemon: Whether the thread should be a daemon thread. Defaults to True.
            token: An existing event loop token to use. If provided, a thread must also be specified.

        Returns:
            Self: The created caller instance.

        Raises:
            RuntimeError: If invalid combinations of thread, token, or name are provided, or if a caller already exists for the given thread or name.
            ValueError: If an invalid name is provided.

        Notes:
            - If a token is provided, a thread must also be specified.
            - If neither backend nor backend_options are specified, they are inferred from the current async kernel.
            - The caller is started in a new thread unless an existing thread is provided.
            - If a thread is provided, it must have a running backend.
        """
        # checks
        if token and not thread:
            msg = "When providing a token a thread must also be provided!"
            raise RuntimeError(msg)

        if thread and thread in cls._instances:
            msg = f"A caller already exists for {thread=}!"
            raise RuntimeError(msg)

        if name is not None:
            if name in [""]:
                msg = f"Invalid name {name=}"
                raise ValueError(msg)
            if thread and name != thread.name:
                msg = f"{name=} does not match {thread.name=}"
                raise RuntimeError(msg)
            if name in [t.name for t in cls._instances]:
                msg = f"A caller already exists with {name=}!"
                raise RuntimeError(msg)

        if thread and not token and thread == threading.current_thread():
            # Obtain a token so the event loop can be run from another thread.
            token = current_token()

        # settings
        if not token and (backend is NoValue or backend_options is NoValue):
            kernel = async_kernel.Kernel()
            if backend is NoValue:
                backend = Backend(current_async_library(failsafe=True) or kernel.anyio_backend)
            if backend_options is NoValue:
                backend_options = kernel.anyio_backend_options.get(backend) if kernel else None

        def async_kernel_caller() -> None:
            try:
                # Create the caller
                caller = cls(thread=thread, log=log, create=True, protected=protected)

                async def run_caller_in_context() -> None:
                    async with caller:
                        if not fut.done():
                            fut.set_result(caller)
                        await anyio.sleep_forever()

                if token:
                    # Start the caller
                    fut.set_result(caller)
                    anyio.from_thread.run(run_caller_in_context, token=token)
                else:
                    anyio.run(run_caller_in_context, backend=backend, backend_options=backend_options)
            except (BaseExceptionGroup, BaseException) as e:
                if not fut.done():
                    fut.set_exception(e)
                if e.__class__.__name__ not in {"Cancelled", "CancelledError"}:
                    raise

        fut: Future[Self] = Future()
        threading.Thread(target=async_kernel_caller, name=None if token else name, daemon=daemon).start()
        return fut.wait_sync()

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
                This is useful when `items` is a generator utilising [Caller.to_thread][].
                By default this will limit to `Caller.MAX_IDLE_POOL_INSTANCES`.
            shield: Shield existing items from cancellation.

        Tip:
            1. Pass a generator if you wish to limit the number future jobs when calling to_thread/to_task etc.
            2. Pass a container with all [Futures][async_kernel.caller.Future] when the limiter is not relevant.
        """
        resume = noop
        future_ready = noop
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
            future_ready()

        async def iter_items():
            nonlocal done, resume
            gen = items if isinstance(items, AsyncGenerator) else iter(items)
            try:
                while True:
                    fut = await anext(gen) if isinstance(gen, AsyncGenerator) else next(gen)
                    assert fut is not current_future, "Would result in deadlock"
                    fut.add_done_callback(future_done)
                    if not fut.done():
                        futures.add(fut)
                        if max_concurrent_ and (len(futures) == max_concurrent_):
                            event = create_async_event()
                            resume = event.set
                            if len(futures) == max_concurrent_:
                                await event
                            resume = noop

            except (StopAsyncIteration, StopIteration):
                return
            finally:
                done = True
                resume()
                future_ready()

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
                    if max_concurrent_ and len(futures) < max_concurrent_:
                        resume()
                    event = create_async_event()
                    future_ready = event.set
                    if not done or futures:
                        await event
                    future_ready = noop
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

        Example:
            ```python
            done, pending = await asyncio.wait(items)
            ```
        Info:
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
