from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import logging
import reprlib
import sys
import threading
import time
import weakref
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from types import CoroutineType
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, Unpack, cast

import anyio
import anyio.from_thread
from aiologic import BinarySemaphore, Event
from aiologic.lowlevel import async_checkpoint, create_async_event, current_async_library
from aiologic.meta import await_for
from anyio.lowlevel import current_token
from typing_extensions import override

from async_kernel import utils
from async_kernel.common import Fixed
from async_kernel.pending import Pending, PendingCancelled, PendingGroup
from async_kernel.typing import Backend, CallerCreateOptions, CallerState, NoValue, PendingCreateOptions, T

with contextlib.suppress(ImportError):
    # Monkey patch sniffio.current_async_library` with aiologic's version which does a better job.
    import sniffio

    sniffio.current_async_library = current_async_library

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import CoroutineType

    import zmq
    from anyio.abc import TaskGroup, TaskStatus

    from async_kernel.typing import P

__all__ = ["Caller", "PendingGroup"]

truncated_rep = reprlib.Repr()
truncated_rep.maxlevel = 1
truncated_rep.maxother = 100
truncated_rep.fillvalue = "…"


def noop() -> None:
    pass


class Caller(anyio.AsyncContextManagerMixin):
    """
    Caller is an advanced asynchronous context manager and scheduler for managing function calls within an async kernel environment.

    Features:
        - Manages a pool of worker threads and async contexts for efficient scheduling and execution.
        - Supports synchronous and asynchronous startup, shutdown, and cleanup of idle workers.
        - Provides thread-safe scheduling of functions (sync/async), with support for delayed and queued execution.
        - Integrates with ZeroMQ (zmq) for PUB socket communication.
        - Tracks child caller instances, enabling hierarchical shutdown and resource management.
        - Offers mechanisms for direct calls, queued calls, and thread offloading.
        - Implements `as_completed` and `wait` utilities for monitoring and collecting results from scheduled tasks.
        - Handles cancellation, exceptions, and context propagation robustly.

    Usage:
        - Use Caller to manage async/sync function execution, worker threads, and task scheduling in complex async applications.
        - Integrate with ZeroMQ for PUB socket communication.
        - Leverage child management for hierarchical resource cleanup.
        - Use `as_completed` and `wait` for efficient result collection and monitoring.
    """

    MAX_IDLE_POOL_INSTANCES = 10
    "The number of `pool` instances to leave idle (See also [to_thread][async_kernel.caller.Caller.to_thread])."
    IDLE_WORKER_SHUTDOWN_DURATION = 0 if "pytest" in sys.modules else 60
    """
    The minimum duration in seconds for a worker to remain in the worker pool before it is shutdown.
    
    Set to 0 to disable (default when running tests).
    """

    _instances: ClassVar[dict[threading.Thread, Self]] = {}
    _lock: ClassVar = BinarySemaphore()

    _name: str
    _idle_time: float = 0.0
    _thread: threading.Thread
    _backend: Backend
    _backend_options: dict[str, Any] | None
    _protected = False
    _state: CallerState = CallerState.initial
    _state_reprs: ClassVar[dict] = {
        CallerState.initial: "❗ not running",
        CallerState.start_sync: "starting sync",
        CallerState.running: "🏃 running",
        CallerState.stopping: "🏁 stopping",
        CallerState.stopped: "🏁 stopped",
    }
    _zmq_context: zmq.Context[Any] | None = None

    _parent_ref: weakref.ref[Self] | None = None

    # Fixed
    _child_lock = Fixed(BinarySemaphore)
    _children: Fixed[Self, set[Self]] = Fixed(set)
    _worker_pool: Fixed[Self, deque[Self]] = Fixed(deque)
    _queue_map: Fixed[Self, dict[int, Pending]] = Fixed(dict)
    _queue: Fixed[Self, deque[tuple[contextvars.Context, Pending] | tuple[Callable, tuple, dict]]] = Fixed(deque)
    stopped = Fixed(Event)
    "A thread-safe Event for when the caller is stopped."

    _pending_var: contextvars.ContextVar[Pending | None] = contextvars.ContextVar("_pending_var", default=None)

    log: logging.LoggerAdapter[Any]
    ""
    iopub_sockets: ClassVar[dict[threading.Thread, zmq.Socket]] = {}
    ""
    iopub_url: ClassVar = "inproc://iopub"
    ""

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
    def backend_options(self) -> dict | None:
        return self._backend_options

    @property
    def protected(self) -> bool:
        "Returns `True` if the caller is protected from stopping."
        return self._protected

    @property
    def zmq_context(self) -> zmq.Context | None:
        "A zmq socket, which if present indicates that an iopub socket is loaded."
        return self._zmq_context

    @property
    def running(self):
        "Returns `True` when the caller is available to run requests."
        return self._state is CallerState.running

    @property
    def children(self) -> frozenset[Self]:
        """A frozenset copy of the instances that were created by the caller.

        Notes:
            - When the parent is stopped, all children are stopped.
            - All children are stopped prior to the parent exiting its async context.
        """
        return frozenset(self._children)

    @property
    def parent(self) -> Self | None:
        "The parent if it exists."
        if (ref := self._parent_ref) and (inst := ref()) and not inst.stopped:
            return inst
        return None

    @override
    def __repr__(self) -> str:
        n = len(self._children)
        children = "" if not n else ("1 child" if n == 1 else f"{n} children")
        name = self.name or self.thread.name
        return f"Caller<{name!s} {self.backend} {self._state_reprs.get(self._state)} {children}>"

    def __new__(
        cls,
        modifier: Literal["CurrentThread", "MainThread", "NewThread", "manual"] = "CurrentThread",
        /,
        **kwargs: Unpack[CallerCreateOptions],
    ) -> Self:
        """
        Create or retrieve a Caller instance.

        Args:
            modifier: Specifies how the Caller instance should be created or retrieved.

                - "CurrentThread": Automatically create or retrieve the instance.
                - "MainThread": Use the main thread for the Caller.
                - "NewThread": Create a new thread.
                - "manual": Manually create a new instance for the current thread.

            **kwargs: Additional options for Caller creation, such as:
                - name: The name to use.
                - backend: The async backend to use.
                - backend_options: Options for the backend.
                - protected: Whether the Caller is protected.
                - zmq_context: ZeroMQ context.
                - log: Logger instance.

        Returns:
            Self: The created or retrieved Caller instance.

        Raises:
            RuntimeError: If the backend is not provided and backend can't be determined.
            ValueError: If the thread and caller's name do not match.
        """
        with cls._lock:
            name, backend = kwargs.get("name", ""), kwargs.get("backend")
            match modifier:
                case "CurrentThread" | "manual":
                    thread = threading.current_thread()
                case "MainThread":
                    thread = threading.main_thread()
                case "NewThread":
                    thread = None

            # Locate existing
            if thread and (caller := cls._instances.get(thread)):
                if modifier == "manual":
                    msg = f"An instance already exists for {thread=}"
                    raise RuntimeError(msg)
                if name and name != caller.name:
                    msg = f"The thread and caller's name do not match! {name=} {caller=}"
                    raise ValueError(msg)
                if backend and backend != caller.backend:
                    msg = f"The backend does not match! {backend=} {caller.backend=}"
                    raise ValueError(msg)
                return caller

            # create a new instance
            inst = super().__new__(cls)
            inst._resume = noop
            inst._name = name
            inst._backend = Backend(backend or current_async_library())
            inst._backend_options = kwargs.get("backend_options")
            inst._protected = kwargs.get("protected", False)
            inst._zmq_context = kwargs.get("zmq_context")
            inst.log = kwargs.get("log") or logging.LoggerAdapter(logging.getLogger())
            if thread:
                inst._thread = thread

            # finalize
            if modifier != "manual":
                inst.start_sync()
            assert inst._thread not in cls._instances
            cls._instances[inst._thread] = inst
        return inst

    def start_sync(self) -> None:
        "Start synchronously."

        if self._state is CallerState.initial:
            self._state = CallerState.start_sync

            async def run_caller_in_context() -> None:
                if self._state is CallerState.start_sync:
                    self._state = CallerState.initial
                async with self:
                    if self._state is CallerState.running:
                        await anyio.sleep_forever()

            if thread := getattr(self, "thread", None):
                # An event loop for the current thread.
                assert thread is threading.current_thread()

                if self.backend == Backend.asyncio:
                    self._task = asyncio.create_task(run_caller_in_context())
                else:
                    # trio
                    token = current_token()

                    def to_thread():
                        utils.mark_thread_pydev_do_not_trace()
                        try:
                            anyio.from_thread.run(run_caller_in_context, token=token)
                        except (BaseExceptionGroup, BaseException) as e:
                            if not "shutdown" not in str(e):
                                raise

                    threading.Thread(target=to_thread, daemon=False).start()
            else:
                # An event loop in a new thread.
                def run_event_loop() -> None:
                    anyio.run(run_caller_in_context, backend=self.backend, backend_options=self.backend_options)

                name = self.name or "async_kernel_caller"
                self._thread = t = threading.Thread(target=run_event_loop, name=name, daemon=True)
                t.start()

    def stop(self, *, force=False) -> CallerState:
        """
        Stop the caller, cancelling all pending tasks and close the thread.

        If the instance is protected, this is no-op unless force is used.
        """
        if (self._protected and not force) or self._state in {CallerState.stopped, CallerState.stopping}:
            return self._state
        set_stop = self._state is CallerState.initial
        self._state = CallerState.stopping
        self._instances.pop(self.thread, None)
        if parent := self.parent:
            try:
                parent._worker_pool.remove(self)
            except ValueError:
                pass
        for child in self.children:
            child.stop(force=True)
        while self._queue:
            item = self._queue.pop()
            if len(item) == 2:
                item[1].cancel()
                item[1].set_result(None)
        for func in tuple(self._queue_map):
            self.queue_close(func)
        self._resume()
        if set_stop:
            self.stopped.set()
            self._state = CallerState.stopped
        return self._state

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        if self._state is CallerState.start_sync:
            msg = 'Already starting! Did you mean to use Caller("manual")?'
            raise RuntimeError(msg)
        if self._state is CallerState.stopped:
            msg = f"Restarting is not allowed: {self}"
            raise RuntimeError(msg)
        socket = None
        async with anyio.create_task_group() as tg:
            if self._state is CallerState.initial:
                self._state = CallerState.running
                await tg.start(self._scheduler, tg)
            if self._zmq_context:
                socket = self._zmq_context.socket(1)  # zmq.SocketType.PUB
                socket.linger = 50
                socket.connect(self.iopub_url)
                self.iopub_sockets[self.thread] = socket
            try:
                yield self
            finally:
                self.stop(force=True)
                if socket:
                    self.iopub_sockets.pop(self.thread, None)
                    socket.close()
                with anyio.CancelScope(shield=True):
                    while self._children:
                        await self._children.pop().stopped
                    if parent := self.parent:
                        parent._children.discard(self)
                    self._state = CallerState.stopped
                    self.stopped.set()
                    await async_checkpoint(force=True)

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
            PendingCancelled: Sets this exception on pending results in the queue upon shutdown.
        """
        task_status.started()
        try:
            while self._state is CallerState.running:
                if self._queue:
                    item, result = self._queue.popleft(), None
                    if len(item) == 3:
                        try:
                            result = item[0](*item[1], **item[2])
                            if inspect.iscoroutine(result):
                                await result
                        except Exception as e:
                            self.log.exception("Direct call failed", exc_info=e)
                    else:
                        item[0].run(tg.start_soon, self._call_scheduled, item[1])
                    del item, result
                else:
                    event = create_async_event()
                    self._resume = event.set
                    if self._state is CallerState.running and not self._queue:
                        await event
                    self._resume = noop
        finally:
            tg.cancel_scope.cancel()

    async def _call_scheduled(self, pen: Pending) -> None:
        """
        Asynchronously executes the function associated with the given instance, handling cancellation, delays, and exceptions.

        Args:
            pen: The [async_kernel.Pending][] object containing metadata about the function to execute, its arguments, and execution state.

        Workflow:
            - Sets the current instance in a context variable.
            - If the instance is cancelled before starting, sets a `PendingCancelled` error.
            - Otherwise, enters a cancellation scope:
                - Registers a canceller for the instance.
                - Waits for a specified delay if present in metadata.
                - Calls the function (sync or async) with provided arguments.
                - Sets the result or exception on the instance as appropriate.
            - Handles cancellation and other exceptions, logging errors as needed.
            - Resets the context variable after execution.
        """
        md = pen.metadata
        token = self._pending_var.set(pen)
        try:
            if pen.cancelled():
                if not pen.done():
                    pen.set_exception(PendingCancelled("Cancelled before started."))
            else:
                with anyio.CancelScope() as scope:
                    pen.set_canceller(lambda msg: self.call_direct(scope.cancel, msg))
                    # Call later.
                    if (delay := md.get("delay")) and ((delay := delay - time.monotonic() + md["start_time"]) > 0):
                        await anyio.sleep(delay)
                    # Call now.
                    try:
                        result = md["func"](*md["args"], **md["kwargs"])
                        if inspect.iscoroutine(result):
                            result = await result
                        pen.set_result(result)
                    # Cancelled.
                    except anyio.get_cancelled_exc_class() as e:
                        if not pen.cancelled():
                            pen.cancel()
                        pen.set_exception(e)
                    # Catch exceptions.
                    except Exception as e:
                        pen.set_exception(e)
        except Exception as e:
            pen.set_exception(e)
        finally:
            self._pending_var.reset(token)

    @classmethod
    def _start_idle_worker_cleanup_thead(cls) -> None:
        "A single thread to shutdown idle workers that have not been used for an extended duration."
        if cls.IDLE_WORKER_SHUTDOWN_DURATION > 0 and not hasattr(cls, "_thread_cleanup_idle_workers"):

            def _cleanup_workers():
                utils.mark_thread_pydev_do_not_trace()
                n = 0
                cutoff = time.monotonic()
                time.sleep(cls.IDLE_WORKER_SHUTDOWN_DURATION)
                for caller in tuple(cls._instances.values()):
                    for worker in frozenset(caller._worker_pool):
                        n += 1
                        if worker._idle_time < cutoff:
                            with contextlib.suppress(IndexError):
                                caller._worker_pool.remove(worker)
                                worker.stop()
                if n:
                    _cleanup_workers()
                else:
                    del cls._thread_cleanup_idle_workers

            cls._thread_cleanup_idle_workers = threading.Thread(target=_cleanup_workers, daemon=True)
            cls._thread_cleanup_idle_workers.start()

    @classmethod
    def get_current(cls, thread: threading.Thread) -> Self | None:
        "A [classmethod][] to get the caller instance from the corresponding thread if it exists."
        with cls._lock:
            return cls._instances.get(thread)

    @classmethod
    def current_pending(cls) -> Pending[Any] | None:
        """A [classmethod][] that returns the current result when called from inside a function scheduled by Caller."""
        return cls._pending_var.get()

    @classmethod
    def all_callers(cls, running_only: bool = True) -> list[Caller]:
        """
        A [classmethod][] to get a list of the callers.

        Args:
            running_only: Restrict the list to callers that are active (running in an async context).
        """
        return [caller for caller in Caller._instances.values() if caller.running or not running_only]

    def get(self, **kwargs: Unpack[CallerCreateOptions]) -> Self:
        """
        Retrieves an existing child caller by name and backend, or creates a new one if not found.

        Args:
            **kwargs: Options for creating or retrieving a caller instance.
                - name: The name of the child caller to retrieve.
                - backend: The backend to match or assign to the caller.
                - backend_options: Options for the backend.
                - zmq_context: ZeroMQ context to use.

        Returns:
            Self: The retrieved or newly created caller instance.

        Raises:
            RuntimeError: If a caller with the specified name exists but the backend does not match.

        Notes:
            - The returned caller is added to `children` and stopped with this instance.
            - If 'backend' and 'zmq_context' are not specified they are copied from this instance.
        """

        with self._child_lock:
            if name := kwargs.get("name"):
                for caller in self.children:
                    if caller.name == name:
                        if (backend := kwargs.get("backend")) and caller.backend != backend:
                            msg = f"Backend mismatch! {backend=} {caller.backend=}"
                            raise RuntimeError(msg)
                        return caller
            if "backend" not in kwargs:
                kwargs["backend"] = self.backend
                kwargs["backend_options"] = self.backend_options
            if "zmq_context" not in kwargs and self._zmq_context:
                kwargs["zmq_context"] = self._zmq_context
            existing = frozenset(self._instances.values())
            caller = self.__class__("NewThread", **kwargs)
            if caller not in existing:
                self._children.add(caller)
                caller._parent_ref = weakref.ref(self)
            return caller

    def schedule_call(
        self,
        func: Callable[..., CoroutineType[Any, Any, T] | T],
        args: tuple,
        kwargs: dict,
        pending_create_options: PendingCreateOptions | None = None,
        context: contextvars.Context | None = None,
        /,
        **metadata: Any,
    ) -> Pending[T]:
        """
        Schedule `func` to be called inside a task running in the callers thread (thread-safe).

        The methods [call_soon][Caller.call_soon] and [call_later][Caller.call_later]
        use this method in the background,  they should be used in preference to this method since they provide type hinting for the arguments.

        Args:
            func: The function to be called. If it returns a coroutine, it will be awaited and its result will be returned.
            args: Arguments corresponding to in the call to  `func`.
            kwargs: Keyword arguments to use with in the call to `func`.
            pending_create_options: Options are passed to [Pending][async_kernel.pending.Pending].
            context: The context to use, if not provided the current context is used.
            **metadata: Additional metadata to store in the instance.
        """
        if self._state in {CallerState.stopping, CallerState.stopped}:
            msg = f"{self} is {self._state.name}!"
            raise RuntimeError(msg)
        pen = Pending(pending_create_options, func=func, args=args, kwargs=kwargs, caller=self, **metadata)
        self._queue.append((context or contextvars.copy_context(), pen))
        self._resume()
        return pen

    def call_later(
        self,
        delay: float,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Pending[T]:
        """
        Schedule func to be called in caller's event loop copying the current context.

        Args:
            func: The function.
            delay: The minimum delay to add between submission and execution.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        Info:
            All call arguments are packed into the instance's metadata.
        """
        return self.schedule_call(func, args, kwargs, delay=delay, start_time=time.monotonic())

    def call_soon(
        self,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Pending[T]:
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
        self._queue.append((func, args, kwargs))
        self._resume()

    def to_thread(
        self,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Pending[T]:
        """
        Call func in a worker thread using the same backend as the current instance.

        Args:
            func: The function.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        Notes:
            - A minimum number of caller instances are retained for this method.
            - Async code run inside func should use taskgroups for creating task.
        """

        def _to_thread_on_done(_) -> None:
            if not caller.stopped and self.running:
                if len(self._worker_pool) < self.MAX_IDLE_POOL_INSTANCES:
                    caller._idle_time = time.monotonic()
                    self._worker_pool.append(caller)
                    self._start_idle_worker_cleanup_thead()
                else:
                    caller.stop()

        try:
            caller = self._worker_pool.popleft()
        except IndexError:
            caller = self.get()
        pen = caller.call_soon(func, *args, **kwargs)
        pen.add_done_callback(_to_thread_on_done)
        return pen

    def queue_get(self, func: Callable) -> Pending[None] | None:
        """Returns `Pending` instance for `func` where the queue is running.

        Warning:
            - This instance loops until the instance is closed or func is garbage collected.
            - The pending has been modified such that waiting it will wait for the queue to be empty.
            - `queue_close` is the preferred means to shutdown the queue.
        """
        return self._queue_map.get(hash(func))

    def queue_call(
        self,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Pending[T]:
        """
        Queue the execution of `func` in a queue unique to it and the caller instance (thread-safe).

        Args:
            func: The function.
            *args: Arguments to use with `func`.
            **kwargs: Keyword arguments to use with `func`.

        Warning:
            - Do not assume the result matches the function call.
            - The returned pending returns the last result of the queue call once the queue becomes empty.

        Notes:
            - The queue runs in a *task* wrapped with a [async_kernel.pending.Pending][] that remains running until one of the following occurs:
                1. The pending is cancelled.
                2. The method [Caller.queue_close][] is called with `func` or `func`'s hash.
                3. `func` is deleted (utilising [weakref.finalize][]).
            - The [context][contextvars.Context] of the initial call is used for subsequent queue calls.
            - Exceptions are 'swallowed'; the last successful result is set on the pending.

        Returns:
            Pending: The pending where the queue loop is running.
        """
        key = hash(func)
        if not (pen_ := self._queue_map.get(key)):
            queue = deque()
            with contextlib.suppress(TypeError):
                weakref.finalize(func.__self__ if inspect.ismethod(func) else func, self.queue_close, key)

            async def queue_loop() -> None:
                pen = self.current_pending()
                assert pen
                item = result = None
                try:
                    while True:
                        await async_checkpoint(force=True)
                        if queue:
                            item = queue.popleft()
                            try:
                                result = item[0](*item[1], **item[2])
                                if inspect.iscoroutine(object=result):
                                    await result
                            except (anyio.get_cancelled_exc_class(), Exception) as e:
                                if pen.cancelled():
                                    raise
                                self.log.exception("Execution %s failed", item, exc_info=e)
                        else:
                            pen.set_result(result, reset=True)
                            del item  # pyright: ignore[reportPossiblyUnboundVariable]
                            event = create_async_event()
                            pen.metadata["resume"] = event.set
                            await async_checkpoint(force=True)
                            if not queue:
                                await event
                            pen.metadata["resume"] = noop
                finally:
                    self._queue_map.pop(key)

            self._queue_map[key] = pen_ = self.schedule_call(queue_loop, (), {}, key=key, queue=queue, resume=noop)
        pen_.metadata["queue"].append((func, args, kwargs))
        pen_.metadata["resume"]()
        return pen_  # pyright: ignore[reportReturnType]

    def queue_close(self, func: Callable | int) -> None:
        """
        Close the execution queue associated with `func` (thread-safe).

        Args:
            func: The queue of the function to close.
        """
        key = func if isinstance(func, int) else hash(func)
        if pen := self._queue_map.pop(key, None):
            pen.cancel()

    async def as_completed(
        self,
        items: Iterable[Awaitable[T]] | AsyncGenerator[Awaitable[T]],
        *,
        max_concurrent: NoValue | int = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        cancel_unfinished: bool = True,
    ) -> AsyncGenerator[Pending[T], Any]:
        """
        An iterator to get result as they complete.

        Args:
            items: Either a container with existing results or generator of Pendings.
            max_concurrent: The maximum number of concurrent results to monitor at a time.
                This is useful when `items` is a generator utilising [Caller.to_thread][].
                By default this will limit to `Caller.MAX_IDLE_POOL_INSTANCES`.
            cancel_unfinished: Cancel any `pending` when exiting.

        Tip:
            1. Pass a generator if you wish to limit the number result jobs when calling to_thread/to_task etc.
            2. Pass a container with all results when the limiter is not relevant.
        """
        resume = noop
        result_ready = noop
        done_results: deque[Pending[T]] = deque()
        results: set[Pending[T]] = set()
        done = False
        current_pending = self.current_pending()
        if isinstance(items, set | list | tuple):
            max_concurrent_ = 0
        else:
            max_concurrent_ = self.MAX_IDLE_POOL_INSTANCES if max_concurrent is NoValue else int(max_concurrent)

        def result_done(pen: Pending[T]) -> None:
            done_results.append(pen)
            result_ready()

        async def iter_items():
            nonlocal done, resume
            gen = items if isinstance(items, AsyncGenerator) else iter(items)
            try:
                while True:
                    pen = await anext(gen) if isinstance(gen, AsyncGenerator) else next(gen)
                    assert pen is not current_pending, "Would result in deadlock"
                    if not isinstance(pen, Pending):
                        pen = cast("Pending[T]", self.call_soon(await_for, pen))
                    pen.add_done_callback(result_done)
                    if not pen.done():
                        results.add(pen)
                        if max_concurrent_ and (len(results) == max_concurrent_):
                            event = create_async_event()
                            resume = event.set
                            if len(results) == max_concurrent_:
                                await event
                            resume = noop
                            await async_checkpoint(force=True)

            except (StopAsyncIteration, StopIteration):
                return
            finally:
                done = True
                resume()
                result_ready()

        pen_ = self.call_soon(iter_items)
        try:
            while not done or results:
                if done_results:
                    pen = done_results.popleft()
                    results.discard(pen)
                    # Ensure all done callbacks are complete.
                    await pen.wait(result=False)
                    yield pen
                else:
                    if max_concurrent_ and len(results) < max_concurrent_:
                        resume()
                    event = create_async_event()
                    result_ready = event.set
                    if not done or results:
                        await event
                    result_ready = noop
        finally:
            pen_.cancel()
            for pen in results:
                pen.remove_done_callback(result_done)
                if cancel_unfinished:
                    pen.cancel("Cancelled by as_completed")

    async def wait(
        self,
        items: Iterable[Awaitable[T]],
        *,
        timeout: float | None = None,
        return_when: Literal["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"] = "ALL_COMPLETED",
    ) -> tuple[set[Pending[T]], set[Pending[T]]]:
        """
        Wait for the results given by items to complete.

        Returns two sets of the results: (done, pending).

        Args:
            items: An iterable of results to wait for.
            timeout: The maximum time before returning.
            return_when: The same options as available for [asyncio.wait][].

        Example:
            ```python
            done, pending = await asyncio.wait(items)
            ```
        Info:
            - This does not raise a TimeoutError!
            - Pendings that aren't done when the timeout occurs are returned in the second set.
        """
        pending: set[Pending[T]]
        done = set()
        pending = {item if isinstance(item, Pending) else self.call_soon(await_for, item) for item in items}
        if pending:
            with anyio.move_on_after(timeout):
                async for pen in self.as_completed(pending.copy(), cancel_unfinished=False):
                    _ = (pending.discard(pen), done.add(pen))
                    if return_when == "FIRST_COMPLETED":
                        break
                    if return_when == "FIRST_EXCEPTION" and (pen.cancelled() or pen.exception()):
                        break
        return done, pending

    def create_pending_group(self, *, shield: bool = False):
        "Create a new [pending group][async_kernel.pending.PendingGroup]."
        return PendingGroup(shield=shield)
