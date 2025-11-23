from __future__ import annotations

import contextlib
import contextvars
import functools
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
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Never, Self, Unpack, cast

import anyio
import anyio.from_thread
import zmq
from aiologic import Event, RLock
from aiologic.lowlevel import async_checkpoint, create_async_event, current_async_library
from aiologic.meta import await_for
from anyio.lowlevel import current_token
from typing_extensions import override

import async_kernel
from async_kernel.common import Fixed
from async_kernel.kernelspec import Backend
from async_kernel.pending import Pending, PendingCancelled
from async_kernel.typing import CallerCreateOptions, CallerState, NoValue, T
from async_kernel.utils import mark_thread_pydev_do_not_trace

with contextlib.suppress(ImportError):
    # Monkey patch sniffio.current_async_library` with aiologic's version which does a better job.
    import sniffio

    sniffio.current_async_library = current_async_library

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import CoroutineType

    from anyio.abc import TaskGroup, TaskStatus

    from async_kernel.typing import P

__all__ = ["Caller"]

truncated_rep = reprlib.Repr()
truncated_rep.maxlevel = 1
truncated_rep.maxother = 100
truncated_rep.fillvalue = "…"


def noop():
    pass


class Caller(anyio.AsyncContextManagerMixin):
    """
    `Caller` is a scheduler for running code in the asynchronous event loop of the thread to which the caller is associated.

    This class manages the execution of callables in a thread-safe manner, providing mechanisms for
    scheduling, queuing, and managing the lifecycle of tasks and their associated threads and event loops.
    It supports advanced features such as delayed execution, per-function queues, cancellation, and
    specification of the AnyIO supported backend.

    Key Features:
        - One Caller instance per thread.
        - Thread-safe scheduling of synchronous and asynchronous functions.
        - Support for delayed and immediate execution (`call_later`, `call_soon`).
        - Per-function execution queues with lifecycle management (`queue_call`, `queue_close`).
        - Integration with AnyIO's async context management and task groups.
        - Mechanisms for stopping, protecting, and pooling Caller instances.
        - Utilities for running functions in separate threads (`to_thread`, `to_thread_advanced`).
        - Methods for waiting on and iterating over multiple pending instances as they complete.
        - IOpub socket per Caller instance.
        - Threads

    Usage:
        - Use `Caller()` to get or create a Caller instances.
        - Use `get` for inherited stopping.
        - Use `call_soon`, `call_later`, or `schedule_call` to schedule work.
        - Use `queue_call` for per-function task queues.
        - Use `to_thread` to run work in a separate thread.
        - Use `as_completed` and `wait` to manage multiple pendings.
        - Use `async with Caller("async-context") = caller:` to use Caller as an
            asynchronous context manager (useful to provide pytest fixtures for example).

    Raises:
        RuntimeError: For invalid operations such as duplicate Caller creation or missing instances.
        anyio.ClosedResourceError: When scheduling on a stopped Caller.

    Notes:
        - It is safe to use the underlying libraries taskgroups
        - [aiologic](https://aiologic.readthedocs.io/latest/) provides thread-safe synchronisation primiates for working across threads.
        - Once a caller is stopped it cannot be restarted, instead a new caller should be started.
    """

    MAX_IDLE_POOL_INSTANCES = 10
    "The number of `pool` instances to leave idle (See also [to_thread][async_kernel.Caller.to_thread])."
    IDLE_WORKER_SHUTDOWN_DURATION = 0 if "pytest" in sys.modules else 60
    """
    The minimum duration for a worker to remain in the worker pool before it is shutdown.
    
    Set to 0 to disable (default when running tests).
    """

    _instances: ClassVar[dict[threading.Thread, Self]] = {}
    _rlock: ClassVar = RLock()

    _name: str
    _idle_time: float = 0.0
    _thread: threading.Thread
    _backend: Backend
    _backend_options: dict[str, Any] | None
    _protected = False
    _state: CallerState = CallerState.initial
    _state_reprs: ClassVar[dict] = {
        CallerState.initial: "❗ not running",
        CallerState.auto_starting: "starting",
        CallerState.running: "🏃 running",
        CallerState.stopping: "🏁 stopping",
        CallerState.stopped: "🏁 stopped",
    }
    _zmq_context: zmq.Context[Any] | None = None

    _parent_thread: threading.Thread | None = None

    # Fixed
    _children: Fixed[Self, set[Self]] = Fixed(set)
    _worker_pool: Fixed[Self, deque[Self]] = Fixed(deque)
    _queue_map: Fixed[Self, dict[int, Pending]] = Fixed(dict)
    _queue: Fixed[Self, deque[tuple[contextvars.Context, Pending] | Callable[[], Any]]] = Fixed(deque)
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

    @override
    def __repr__(self) -> str:
        n = len(self._children)
        children = "" if not n else ("1 child" if n == 1 else f"{n} children")
        return f"Caller<{self.name} {self.backend} {self._state_reprs.get(self._state)} {children}>"

    def __new__(
        cls,
        modifier: None | Literal["existing", "MainThread", "async-context"] = None,
        /,
        **kwargs: Unpack[CallerCreateOptions],
    ) -> Self:
        """
        Creates or retrieves an instance of the caller.

        Args:
            modifier: Modifies which instance is returned and whether it should be started.

                - `None`: (Default) A new instance is created if no existing instance is found.
                - `"existing"`: Only checks for existing instances.
                - `"MainThread"`: Shorthand for kwargs = `{"thread":threading.main_thread()}`
                - `"async-context"`: The only way to directly create a new instance.
                    The scheduler will run whilst its async-context has been entered.

            **kwargs: Additional options for caller creation, which may include:

                - name: Name for the caller instance.
                - thread: The thread to associate with the caller.
                - backend: The backend to use. Defaults to the current async library or the Kernel's.
                - backend_options: Additional options for the backend.
                - log: LoggerAdapter for the instance.
                - protected: Whether the instance is protected. Defaults to False.
                - zmq_context: ZeroMQ context to use.

        Returns:
            Self: An instance of the caller.

        Raises:
            RuntimeError: If a caller already exists for the specified thread when `mode=="async-context"`.

        Notes:
            - There is only **one caller per thread**.
            - A caller retains its own pool of workers and child threads.
            - When a caller is shutdown its children are shutdown.
            - New instances created duing [Caller.get][] get added to its children.
                The following methods use [Caller.get][]:
                - [caller.to_thread][Caller.to_thread]
                - [caller.to_thread_advanced][Caller.to_thread_advanced]
                - [caller.get][Caller.get] (called via the instance)
            - The 'name' of children is always unique and can be used to retrieve it with the above selected methods.
        """

        thread = kwargs.get("thread") or threading.current_thread()
        if modifier != "async-context":
            return cls._get_instance(modifier or "auto", **kwargs)
        with cls._rlock:
            if thread in cls._instances:
                msg = f"A caller already exists for {thread=}"
                raise RuntimeError(msg)
            inst = super().__new__(cls)
            inst._backend = Backend(kwargs.get("backend") or current_async_library())
            inst._thread = thread
            inst._name = kwargs.get("name") or thread.name or str(thread)
            inst.log = kwargs.get("log") or logging.LoggerAdapter(logging.getLogger())
            inst._protected = kwargs.get("protected", False)
            inst._backend_options = kwargs.get("backend_options")
            inst._zmq_context = kwargs.get("zmq_context")
            inst._resume = noop
            cls._instances[thread] = inst
        return inst

    @classmethod
    def _get_instance(
        cls,
        mode: Literal["auto", "existing", "MainThread"],
        /,
        **kwargs: Unpack[CallerCreateOptions],
    ) -> Self:
        with cls._rlock:
            # Existing instances.
            main, current = threading.main_thread(), threading.current_thread()
            if (name := kwargs.get("name")) and (name.lower() == "mainthread"):
                msg = f'{name=} is reserved! To get the caller for the main thread use `Caller("MainThread")`'
                raise RuntimeError(msg)
            if mode == "MainThread":
                kwargs = {"thread": main}
            if thread := current if not kwargs else kwargs.get("thread"):
                if caller_ := cls._instances.get(thread):
                    if name and name != caller_.name:
                        msg = f"The thread and caller's name do not match! {name=} {caller_=}"
                        raise ValueError(msg)
                    return caller_
                if thread is not current:
                    msg = f"A caller does not exist for {thread=}!"
                    raise RuntimeError(msg)
            if mode == "existing":
                msg = f"Caller instance not found for {kwargs=}"
                raise RuntimeError(msg)

            # New instances:
            pen: Pending[Self] = Pending()
            if thread:
                # `anyio.from_thread.run`.
                thread_name = f"async_kernel_caller in {thread.name}"
                pen.metadata["token"] = current_token()
            else:
                # `anyio.run`.
                thread_name = name or "async_kernel_caller"
                kernel = async_kernel.Kernel()
                backend = Backend(kwargs.get("backend") or current_async_library(failsafe=True) or kernel.anyio_backend)
                backend_options = kwargs.get("backend_options", kernel.anyio_backend_options.get(backend))
                kwargs.update(backend=backend, backend_options=backend_options)

            # Create a new thread to run the scheduler in the async context.
            thread_ = threading.Thread(target=cls._open_async_context, name=thread_name, args=[pen])
            kwargs["thread"] = thread if "token" in pen.metadata else thread_
            pen.metadata["caller"] = cls("async-context", **kwargs)
            pen.metadata["caller"]._state = CallerState.auto_starting
            thread_.start()
        return pen.wait_sync()

    @classmethod
    def _open_async_context(cls, pen: Pending[Self]) -> None:
        caller: Self = pen.metadata["caller"]

        async def run_caller_in_context() -> None:
            if caller._state is CallerState.auto_starting:
                caller._state = CallerState.initial
                async with caller:
                    if not pen.done():
                        pen.set_result(caller)
                    await caller.stopped

        try:
            if token := pen.metadata.get("token"):
                pen.set_result(caller)
                mark_thread_pydev_do_not_trace()
                anyio.from_thread.run(run_caller_in_context, token=token)
            else:
                anyio.run(run_caller_in_context, backend=caller.backend, backend_options=caller.backend_options)
        except (BaseExceptionGroup, BaseException) as e:
            if not pen.done():
                pen.set_exception(e)
            if not "shutdown" not in str(e):
                raise

    @classmethod
    def _start_idle_worker_cleanup_thead(cls) -> None:
        "A single thread to shutdown idle workers that have not been used for an extended duration."
        if cls.IDLE_WORKER_SHUTDOWN_DURATION > 0 and not hasattr(cls, "_thread_cleanup_idle_workers"):

            def _cleanup_workers():
                mark_thread_pydev_do_not_trace()
                n = 0
                cutoff = time.monotonic()
                time.sleep(cls.IDLE_WORKER_SHUTDOWN_DURATION)
                for caller in tuple(cls._instances.values()):
                    for worker in frozenset(caller._worker_pool):
                        n += 1
                        if worker._idle_time < cutoff:
                            with contextlib.suppress(IndexError), cls._rlock:
                                caller._worker_pool.remove(worker)
                                worker.stop()
                if n:
                    _cleanup_workers()
                else:
                    del cls._thread_cleanup_idle_workers

            cls._thread_cleanup_idle_workers = threading.Thread(target=_cleanup_workers, daemon=True)
            cls._thread_cleanup_idle_workers.start()

    def stop(self, *, force=False) -> CallerState:
        """
        Stop the caller, cancelling all pending tasks and close the thread.

        If the instance is protected, this is no-op unless force is used.
        """
        if (self._protected and not force) or self._state in {CallerState.stopped, CallerState.stopping}:
            return self._state
        if self._state is CallerState.auto_starting:
            self._state = CallerState.stopped
            self.stopped.set()
        else:
            assert self._state is CallerState.running
            self._state = CallerState.stopping
        self._instances.pop(self.thread, None)
        self._worker_pool.clear()
        while self._queue:
            item = self._queue.pop()
            if isinstance(item, tuple):
                item[1].cancel()
                item[1].set_result(None)
        for func in tuple(self._queue_map):
            self.queue_close(func)
        if self._parent_thread and (parent := self._instances.get(self._parent_thread)):
            try:
                parent._worker_pool.remove(self)
            except ValueError:
                pass
        self._resume()
        return self._state

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        if self._state is CallerState.auto_starting:
            msg = 'Already starting! Did you mean to use Caller("async-context")?'
            raise RuntimeError(msg)
        if self._state in {CallerState.stopped, CallerState.stopping}:
            msg = f"Restarting is not allowed: {self}"
            raise RuntimeError(msg)
        self._state = CallerState.running
        async with anyio.create_task_group() as tg:
            await tg.start(self._scheduler, tg)
            if self._zmq_context:
                socket = self._zmq_context.socket(zmq.SocketType.PUB)
                socket.linger = 500
                socket.connect(self.iopub_url)
                self.iopub_sockets[self.thread] = socket
            else:
                socket = None
            try:
                yield self
            finally:
                if socket:
                    self.iopub_sockets.pop(self.thread, None)
                    socket.close()
                self.stop(force=True)
                for c in tuple(self._children):
                    c.stop(force=True)
                with anyio.CancelScope(shield=True):
                    if self._children:
                        while self._children:
                            await self._children.pop().stopped
                    if self._parent_thread and (parent := self._instances.get(self._parent_thread)):
                        parent._children.discard(self)
                    self._state = CallerState.stopped
                    self.stopped.set()

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
                    if callable(item):
                        try:
                            result = item()
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

    def get(
        self, mode: Literal["auto", "existing", "MainThread"] = "auto", /, **kwargs: Unpack[CallerCreateOptions]
    ) -> Self:
        """
        Get a new or existing caller as a child of the current caller.

        Notes:
            - If 'mode' is not "MainThread" and 'thread' is not specified in kwargs, the method attempts to find an existing child with the given 'name'.
            - If no suitable child is found, it sets default backend and context options if not provided.
            - Ensures that new instances are tracked as children and maintains parent references.
        """

        with self._rlock:
            if mode != "MainThread" and "thread" not in kwargs:
                if name := kwargs.get("name"):
                    for caller in self.children:
                        if caller.name == name:
                            return caller
                if "backend" not in kwargs:
                    kwargs["backend"] = self.backend
                    kwargs["backend_options"] = self.backend_options
                if "zmq_context" not in kwargs and self._zmq_context:
                    kwargs["zmq_context"] = self._zmq_context
            existing = frozenset(self._instances.values())
            caller = self._get_instance(mode, **kwargs)
            if caller not in existing:
                self._children.add(caller)
                caller._parent_thread = self.thread
            return caller

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

        See also:
            - [Caller.to_thread_advanced][]
        """
        return self.to_thread_advanced({"name": None}, func, *args, **kwargs)

    def to_thread_advanced(
        self,
        options: CallerCreateOptions,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Pending[T]:
        """
        Call func in a current or new Caller according to the options.

        Args:
            options: Options to pass to [Caller.get][].
            func: The function.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        Returns:
            A result that can be awaited for the  result of func.

        Raises:
            ValueError: When a name is not supplied.

        Notes:
            - When `options == {"name": None}` the caller is associated with a pool of workers.
            - When called via from an instance any new callers are added to the the instances children (done in `_catch_new_instances`).
        """

        if is_worker := options == {"name": None}:
            try:
                while not (caller_ := self._worker_pool.popleft()).running:
                    pass
                caller = caller_
            except IndexError:
                caller = self.get(name=None)
        else:
            if not options.get("name"):
                msg = "A name was not provided in {options=}."
                raise ValueError(msg)
            caller = self.get(**options)
        pen = caller.call_soon(func, *args, **kwargs)
        if is_worker:

            def _to_thread_on_done(_) -> None:
                if not caller.stopped and self.running:
                    if len(self._worker_pool) < self.MAX_IDLE_POOL_INSTANCES:
                        caller._idle_time = time.monotonic()
                        self._worker_pool.append(caller)
                        self._start_idle_worker_cleanup_thead()
                    else:
                        caller.stop()

            pen.add_done_callback(_to_thread_on_done)
        return pen

    def schedule_call(
        self,
        func: Callable[..., CoroutineType[Any, Any, T] | T],
        /,
        args: tuple,
        kwargs: dict,
        context: contextvars.Context | None = None,
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
            context: The context to use, if not provided the current context is used.
            **metadata: Additional metadata to store in the instance.
        """
        if self._state in {CallerState.stopping, CallerState.stopped}:
            msg = f"{self} is {self._state.name}!"
            raise RuntimeError(msg)
        pen = Pending(func=func, args=args, kwargs=kwargs, caller=self, **metadata)
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
        self._queue.append(functools.partial(func, *args, **kwargs))
        self._resume()

    def queue_get(self, func: Callable) -> Pending[Never] | None:
        """Returns `Pending` instance for `func` where the queue is running.

        Warning:
            - This instance loops until the instance is closed or func is garbage collected.
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
        if not (pen_ := self._queue_map.get(key)):
            queue = deque()
            with contextlib.suppress(TypeError):
                weakref.finalize(func.__self__ if inspect.ismethod(func) else func, lambda: self.queue_close(key))

            async def queue_loop(key: int, queue: deque) -> None:
                pen = self.current_pending()
                assert pen
                try:
                    while True:
                        if queue:
                            item, result = queue.popleft(), None
                            try:
                                result = item[0](*item[1], **item[2])
                                if inspect.iscoroutine(object=result):
                                    await result
                            except (anyio.get_cancelled_exc_class(), Exception) as e:
                                if pen.cancelled():
                                    raise
                                self.log.exception("Execution %s failed", item, exc_info=e)
                            finally:
                                del item, result
                            await async_checkpoint()
                        else:
                            event = create_async_event()
                            pen.metadata["resume"] = event.set
                            if not queue:
                                await event
                            pen.metadata.pop("resume")
                finally:
                    self._queue_map.pop(key)

            self._queue_map[key] = pen_ = self.call_soon(queue_loop, key=key, queue=queue)
        pen_.metadata["kwargs"]["queue"].append((func, args, kwargs))
        if resume := pen_.metadata.get("resume"):
            resume()

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
        A [classmethod][] iterator to get result as they complete.

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
                            await async_checkpoint()

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
        A [classmethod][] to wait for the results given by items to complete.

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
