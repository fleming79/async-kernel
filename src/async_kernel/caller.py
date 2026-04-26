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
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, Unpack, cast, final

import anyio
from aiologic import BinarySemaphore, Event
from aiologic.lowlevel import create_async_event, create_async_waiter, current_async_library
from aiologic.meta import await_for
from typing_extensions import override
from wrapt import lazy_import

import async_kernel.event_loop
from async_kernel import utils
from async_kernel.common import Fixed, KernelInterrupt, SingleAsyncQueue, noop
from async_kernel.event_loop.run import Host, get_start_guest_run
from async_kernel.pending import Pending, PendingGroup, PendingManager, PendingTracker
from async_kernel.typing import Backend, CallerCreateOptions, CallerState, Hosts, NoValue, RunSettings, T

with contextlib.suppress(ImportError):
    # Monkey patch sniffio.current_async_library` with aiologic's version which does a better job.
    import sniffio

    sniffio.current_async_library = current_async_library

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import CoroutineType

    import trio  # noqa: TC004
    import zmq
    from aiologic.lowlevel import AsyncEvent

    from async_kernel.typing import P

__all__ = ["Caller"]

globals()["trio"] = lazy_import("trio")

truncated_rep = reprlib.Repr()
truncated_rep.maxlevel = 1
truncated_rep.maxother = 100
truncated_rep.fillvalue = "…"


@asynccontextmanager
async def task_factory() -> AsyncGenerator[Callable[[contextvars.Context | None, Callable, Unpack[tuple]], None]]:
    """
    An async context that yields a function to start tasks for the current async library ('asyncio' or 'trio').

    Asyncio will use `eager_start` where the loop supports it.

    When the context is exited:

    - All unfinished tasks are cancelled.
    - The context waits for all tasks to stop prior to returning.
    """
    backend = Backend(current_async_library())
    if backend is Backend.asyncio:
        loop = asyncio.get_running_loop()
        coro = asyncio.sleep(0)
        tasks = set()
        eager = False
        all_done = create_async_waiter(shield=True)
        active = True
        try:
            await loop.create_task(coro, eager_start=True)  # pyright: ignore[reportCallIssue]
            eager = True
        except Exception:
            coro.close()

        def done_callback(task: asyncio.Task) -> None:
            tasks.discard(task)
            if not active and not tasks:
                all_done.wake()

        def create_task(context: contextvars.Context | None, func: Callable, *args) -> None:
            if eager:
                task = loop.create_task(func(*args), context=context, eager_start=True)  # pyright: ignore[reportCallIssue]
            else:
                task = loop.create_task(func(*args), context=context)
            if not task.done():
                tasks.add(task)
                task.add_done_callback(done_callback)

        try:
            yield create_task
        finally:
            active = False
            if tasks:
                for task in tasks:
                    task.cancel()
                await all_done
    else:
        async with trio.open_nursery() as nursery:

            def create_task(context: contextvars.Context | None, func: Callable, *args) -> None:
                if context:
                    context.run(nursery.start_soon, func, *args)
                else:
                    nursery.start_soon(func, *args)

            try:
                yield create_task
            finally:
                nursery.cancel_scope.cancel("Shutting down")


@final
class Caller(anyio.AsyncContextManagerMixin):
    """
    A thread-local class that facilitates inter-thread function and coroutine scheduling in asynchronous backends (asyncio or trio).

    - CPython: there is only one caller instance per thread.
    - Pyodide: Pyodide does not support threads, It is a context-varible local class instead.

    Multi-eventloop management is supported including:

    - zero or one host gui event loop.
    - one or two backends.

    Code execution is always done within the context of an asynchronous backend.

    **High level methods**

    - [Caller.call_soon][]: Schedule a function call in the caller's thread.
    - [Caller.call_later][]: Schedule a function call in the caller's thread after a delay.
    - [Caller.to_thread][]: Schedule a function call using a worker caller (separate thread).
    - [Caller.call_using_backend][]: Schedule a function call using the backend in the caller's thread.
    - [Caller.as_completed][]: An async iterator to access pending as they complete.
    - [Caller.wait][]: A method to wait for pending to complete with a timeout.
    - [Caller.create_pending_group][]: Create a new pending group to use as an asynchronous context.
    - [Caller.get][]: Get a new caller instance (child).

    **Low level methods**

    - [Caller.schedule_call][]: Schedule a function call in the caller's thread - configurable (used by high level methods).
    - [Caller.call_direct][]: Call a function directly in the scheduler in the caller's thread.
    - [Caller.queue_call][]: Execute a function in the caller's thread using a queue (sequential).
    - [Caller.queue_get][]: Get the pending associated with the queue call.
    - [Caller.queue_close][]: Close the queue associated with the function.

    **Class methods**

    - [Caller.current_pending][]: Get the active pending in the current context.
    - [Caller.id_current][]: Get the id of the caller in the current thread/context.
    - [Caller.get_existing][]: Get the caller by id.
    - [Caller.all_callers][]: Get a list of all callers.
    """

    MAX_IDLE_POOL_INSTANCES = 10
    "The number of `pool` instances to leave idle (See also [to_thread][async_kernel.caller.Caller.to_thread])."

    IDLE_WORKER_SHUTDOWN_DURATION = 0 if "pytest" in sys.modules else 60
    """
    The minimum duration in seconds for a worker to remain in the worker pool before it is shutdown.
    
    Set to 0 to disable (default when running tests).
    """

    CALLER_MAIN_THREAD_ID: int = id(threading.main_thread())

    _caller_token = contextvars.ContextVar("caller_tokens", default=CALLER_MAIN_THREAD_ID)
    _instances: ClassVar[dict[int, Self]] = {}
    _lock: ClassVar = BinarySemaphore()

    _thread: threading.Thread
    _caller_id: int
    _name: str
    _idle_time: float = 0.0
    _backend: Backend
    _backend_options: dict[str, Any] | None
    _host: Hosts | None
    _host_options: dict[str, Any] | None
    _protected = False
    _use_safe_checkpoint = False
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
    _tasks: Fixed[Self, set[asyncio.Task]] = Fixed(set)
    _worker_pool: Fixed[Self, deque[Self]] = Fixed(deque)
    _queue_map: Fixed[Self, dict[int, Pending]] = Fixed(dict)
    _queue: Fixed[Self, SingleAsyncQueue[Pending | tuple[Callable, tuple, dict]]] = Fixed(
        lambda c: SingleAsyncQueue(reject=c["owner"]._reject)
    )
    _guest_queues: Fixed[Self, dict[Backend, SingleAsyncQueue[Pending | tuple[Callable, tuple, dict]]]] = Fixed(dict)
    _guest_done_events: Fixed[Any, set[AsyncEvent]] = Fixed(set)
    _stopping = Fixed(Pending[None])
    "A pending that is set done the first time stop is called."

    stopped = Fixed(Event)
    "An event that is set when the caller has stopped."

    _pending_var: contextvars.ContextVar[Pending | None] = contextvars.ContextVar("_pending_var", default=None)

    log: logging.LoggerAdapter[Any]
    ""
    iopub_sockets: ClassVar[dict[int, zmq.Socket]] = {}
    ""
    iopub_url: ClassVar = "inproc://iopub"
    ""

    @property
    def name(self) -> str:
        "The name of the thread when the caller was created."
        return self._name

    @property
    def id(self) -> int:
        "The id for the caller."
        return self._caller_id

    @property
    def backend(self) -> Backend:
        "The backend used by caller."
        return self._backend

    @property
    def backend_options(self) -> dict | None:
        "Options used to create the backend."
        return self._backend_options

    @property
    def host(self) -> Hosts | None:
        "The [name][async_kernel.typing.Hosts] of the gui event loop if there is one."
        return self._host

    @property
    def host_options(self) -> dict | None:
        "Options used to create the gui event loop."
        return self._host_options

    @property
    def protected(self) -> bool:
        "Returns `True` if the caller is protected from stopping."
        return self._protected

    @property
    def zmq_context(self) -> zmq.Context | None:
        "A zmq socket, which if present indicates that an iopub socket is loaded."
        return self._zmq_context

    @property
    def running(self) -> bool:
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
    def thread(self) -> threading.Thread:
        "The thread where the caller is running."
        return self._thread

    @property
    def parent(self) -> Self | None:
        "The parent caller if it exists."
        if (ref := self._parent_ref) and (inst := ref()) and not inst.stopped:
            return inst
        return None

    def _get_info(self) -> dict[str, Any]:
        return {
            "name": self._name,
            "backend": str(self._backend),
            "host": self._host,
            "thread": self._thread.name,
            "id": self._caller_id,
        }

    @override
    def __repr__(self) -> str:
        info = " ".join(f"{k}={v!r}" for k, v in self._get_info().items())
        current = "⚪" if self.id_current() == self._caller_id else "⚫"
        protected = "🔐 " if self.protected else " "
        n = len(self._children)
        children = "" if not n else (" 1 child" if n == 1 else f" {n} children")
        return f"<Caller {current}{self._state_reprs[self._state]}{protected}{info}{children}>"

    def __new__(
        cls,
        modifier: Literal["CurrentThread", "MainThread", "NewThread", "manual"] = "CurrentThread",
        /,
        **kwargs: Unpack[CallerCreateOptions],
    ) -> Self:
        """
        Create or retrieve a caller.

        Args:
            modifier: Specifies the caller instance to retrieve.

                - "CurrentThread": The caller for the current thread.
                - "MainThread": The Caller associated with the main thread.
                - Advanced:
                    - "NewThread": Create a caller with a new thread.
                        [Caller.get][] and [Caller.to_thread][] are recommended for normal usage.
                    - "manual": Create a instance for the current thread.

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
                    caller_id = cls.id_current()
                case "MainThread":
                    caller_id = cls.CALLER_MAIN_THREAD_ID
                case "NewThread":
                    caller_id = None

            # Locate existing
            if caller_id is not None and (caller := cls._instances.get(caller_id)):
                if modifier == "manual":
                    msg = f"An instance already exists for {caller_id=}"
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
            inst._name = name
            inst._backend = Backend(backend or current_async_library())
            if inst._backend is Backend.trio:
                trio.sleep  # noqa: B018 # Check trio is available.
            inst._host = Hosts(loop) if (loop := kwargs.get("host")) else None
            inst._backend_options = kwargs.get("backend_options")
            inst._host_options = kwargs.get("host_options")
            inst._protected = kwargs.get("protected", False)
            inst._zmq_context = kwargs.get("zmq_context")
            inst.log = kwargs.get("log") or logging.LoggerAdapter(logging.getLogger())
            if (sys.platform == "emscripten") and (caller_id is None):
                caller_id = id(inst)
            if caller_id is not None:
                inst._caller_id = caller_id
                inst._thread = threading.current_thread()

            # finalize
            if modifier != "manual":
                inst.start_sync(no_debug=kwargs.get("no_debug", False))
            assert inst._caller_id
            assert inst._caller_id not in cls._instances
            cls._instances[inst._caller_id] = inst
        return inst

    def start_sync(self, *, no_debug: bool = False) -> None:
        """
        Start synchronously.

        Args:
            no_debug: If debugpy should be disabled in the thread.
        """

        assert self._state is CallerState.initial
        self._state = CallerState.start_sync

        async def run_caller_in_context() -> None:
            if self._state is CallerState.start_sync:
                self._state = CallerState.initial
            if no_debug:
                utils.mark_thread_pydev_do_not_trace()
            try:
                async with self:
                    await self._stopping.wait(result=False)
            except Exception as e:
                self.log.exception("Caller did not exit context nicely!", exc_info=e)

        if getattr(self, "_caller_id", None) is not None:
            # An event loop for the current thread.

            if self.backend == Backend.asyncio:
                self._tasks.add(asyncio.create_task(run_caller_in_context()))
            else:
                # Use another thread to schedule a trio Task
                trio_token = trio.lowlevel.current_trio_token()

                def to_thread() -> None:
                    utils.mark_thread_pydev_do_not_trace()
                    try:
                        trio.from_thread.run(run_caller_in_context, trio_token=trio_token)
                    except (BaseExceptionGroup, BaseException) as e:
                        if not "shutdown" not in str(e):
                            raise

                threading.Thread(target=to_thread, daemon=False).start()
        else:
            settings = RunSettings(
                backend=self.backend,
                host=self.host,
                backend_options=self.backend_options,
                host_options=self.host_options,
            )

            def run() -> None:
                try:
                    async_kernel.event_loop.run(run_caller_in_context, (), settings)
                except Exception as e:
                    self.log.exception("A start_sync exception occurred.", exc_info=e)
                    if not self.stopped:
                        raise

            self._thread = threading.Thread(target=run, name=self.name or "async_kernel_caller", daemon=False)
            self._caller_id = id(self._thread)
            self._thread.start()

    def stop(self, *, force: bool = False) -> CallerState:
        """
        Stop the caller cancelling all incomplete tasks.

        Args:
            force: If the caller is protected the call is a no-op unless force=True.
        """
        if (self._protected and not force) or self._state is CallerState.stopped:
            return self._state
        state = self._state
        self._state = CallerState.stopping
        # Invoke stop callbacks
        self._stopping.set_result(None)
        if parent := self.parent:
            try:
                parent._worker_pool.remove(self)
            except ValueError:
                pass
        for func in tuple(self._queue_map):
            self.queue_close(func)
        if state is CallerState.initial and not self._children:
            self._stop_finalize()
        else:
            for child in self.children:
                child.stop(force=True)
        return self._state

    def _stop_finalize(self) -> None:
        self._queue.stop()
        self._state = CallerState.stopped
        self._instances.pop(self._caller_id, None)
        self.stopped.set()
        if parent := self.parent:
            parent._children.discard(self)

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        "The asynchronous context for caller."
        if self._state is CallerState.start_sync:
            msg = 'Already starting! Did you mean to use Caller("manual")?'
            raise RuntimeError(msg)
        if self._state is CallerState.stopped:
            msg = f"Stopped: {self}"
            raise RuntimeError(msg)
        socket = None
        try:
            if not self._name:
                self._name = threading.current_thread().name
            if self._zmq_context:
                socket = self._zmq_context.socket(1)  # zmq.SocketType.PUB
                socket.connect(self.iopub_url)
                self.iopub_sockets[self._caller_id] = socket
            async with task_factory() as create_task:
                try:
                    create_task(contextvars.Context(), self._scheduler, self._queue)
                    with anyio.CancelScope() as scope:
                        self._stopping.add_done_callback(lambda _: self.call_direct(scope.cancel, "Stopping"))
                        self._state = CallerState.running
                        yield self
                finally:
                    self.stop(force=True)
                    for event in self._guest_done_events:
                        await event  # Internally shielded from cancellation
                    self._queue.stop()
        finally:
            while self._children:
                try:
                    if not (caller := self._children.pop()).stopped:
                        with anyio.CancelScope(shield=True):
                            await caller.stopped
                except IndexError:
                    pass
            if socket:
                self.iopub_sockets.pop(self._caller_id, None)
                socket.close(linger=0)
            self._stop_finalize()

    async def _scheduler(self, queue: SingleAsyncQueue) -> None:
        """
        A function that async iterates the queue and executes items as they arrive.

        It handles two types of items:
            - tuple: A tuple of `func`, `args`, `kwargs` is called directly in the scheduler.
            - Pending: A pending is started as an 'task'. The pending provides `func`, `args` and `kwargs`
                in it's metadata. The pending is set as `active_pending`in the context for the duration of execution.

        Args:
            queue: The queue to access the items for scheduling.
        """
        backend = Backend(current_async_library())

        async def run_pending_function(pen: Pending[Any]) -> None:
            if pen.done():
                return  # pragma: no cover
            md = pen.metadata
            token_pending = self._pending_var.set(pen)
            token_ident = self._caller_token.set(self._caller_id)
            e = None
            try:
                if inspect.iscoroutine(result := md["func"](*md["args"], **md["kwargs"])):
                    if backend is Backend.asyncio:
                        task = asyncio.current_task()
                        assert task
                        pen.set_canceller(lambda msg: self.call_direct(task.cancel, msg))
                        try:
                            pen.set_result(await result)
                        except asyncio.CancelledError:
                            pen.cancel("Task was cancelled")
                            raise
                    else:
                        with trio.CancelScope() as scope:
                            pen.set_canceller(lambda msg: self.call_direct(scope.cancel, msg))
                            try:
                                pen.set_result(await result)
                            except trio.Cancelled:
                                pen.cancel("Task was cancelled")
                                raise
                else:
                    pen.set_result(result)
            except (Exception, KernelInterrupt) as exc:
                pen.set_exception(exc)
            except BaseException as exc:
                e = exc
            finally:
                if not pen.done():
                    pen.cancel("Unable to finish execution")
                    pen.set_result(None)
                del pen
                self._pending_var.reset(token_pending)
                self._caller_token.reset(token_ident)
                if e:
                    raise e from None

        if not queue.stopped:
            async with task_factory() as create_task:
                async for item in queue:
                    if isinstance(item, Pending):
                        if not item.done():
                            create_task(item.context, run_pending_function, item)
                    else:
                        try:
                            result = item[0](*item[1], **item[2])
                            if inspect.iscoroutine(result):
                                await result
                            del result
                        except Exception as e:
                            self.log.exception("Direct call failed", exc_info=e)
                    del item

    @staticmethod
    def _reject(item: tuple | Pending) -> None:
        if isinstance(item, Pending):
            item.cancel("The caller has been closed")

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

            cls._thread_cleanup_idle_workers = threading.Thread(target=_cleanup_workers, daemon=False)
            cls._thread_cleanup_idle_workers.start()

    @classmethod
    def id_current(cls) -> int:
        "The id that is used for a caller for the current thread in CPython or context in Pyodide."
        if sys.platform == "emscripten":
            return cls._caller_token.get()
        return id(threading.current_thread())

    @classmethod
    def get_existing(cls, caller_id: int | None = None, /) -> Self | None:
        """
        A [classmethod][] to get the caller instance from the corresponding thread if it exists.

        Args:
            caller_id: The id of the caller which in CPython is also the the id of the thread in which it is running.
        """
        caller_id = cls.id_current() if caller_id is None else caller_id
        with cls._lock:
            return cls._instances.get(caller_id)

    @classmethod
    def current_pending(cls) -> Pending[Any] | None:
        """A [classmethod][] that returns the current pending when called from inside a function scheduled by Caller."""
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
        Retrieves an existing child caller by `name` and `backend`, or creates a new one if not found.

        Args:
            **kwargs: Options for creating or retrieving a caller.

        Returns:
            Self: The retrieved or newly created caller.

        Raises:
            RuntimeError: If a caller with the specified name exists but the backend does not match.

        Notes:
            - The returned caller is added to `children` and stopped with the caller.
            - If 'backend' or 'zmq_context' are not specified they are copied from the caller.
        """
        if self._state in [CallerState.stopping, CallerState.stopped]:
            msg = f"Caller is stopping or stopped {self}"
            raise RuntimeError(msg)
        with self._child_lock:
            if name := kwargs.get("name"):
                for caller in self.children:
                    if caller.name == name:
                        if (backend := kwargs.get("backend")) and caller.backend != backend:
                            msg = f"Backend mismatch! {backend=} {caller.backend=}"
                            raise RuntimeError(msg)
                        if (host := kwargs.get("host")) and caller.host != host:
                            msg = f"Host mismatch! {host=} {caller.host=}"
                            raise RuntimeError(msg)
                        return caller
            if "backend" not in kwargs:
                kwargs["backend"] = self._backend
                kwargs["backend_options"] = self.backend_options
            if "zmq_context" not in kwargs and self._zmq_context:
                kwargs["zmq_context"] = self._zmq_context
            caller = self.__class__("NewThread", **kwargs)
            self._children.add(caller)
            caller._parent_ref = weakref.ref(self)
            return caller

    def schedule_call(
        self,
        func: Callable[..., CoroutineType[Any, Any, T] | T],
        args: tuple,
        kwargs: dict,
        context: contextvars.Context | None = None,
        trackers: type[PendingTracker] | tuple[type[PendingTracker], ...] = PendingTracker,
        backend: NoValue | Backend = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        /,
        **metadata: Any,
    ) -> Pending[T]:
        """
        A low-level function to schedule execution of `func`in a task running in the caller's thread.

        Args:
            func: The function to be called.
            args: Arguments corresponding to in the call to  `func`.
            kwargs: Keyword arguments to use with in the call to `func`.
            context: A context to copy, if not provided the current context is copied.
            trackers: The tracker subclasses of active trackers which to add the pending.
            **metadata: Additional metadata to store in the instance.

        Returns:
            Pending: A pending that can be awaited to obtain the result of func.
        """
        pen = Pending(context, trackers, func=func, args=args, kwargs=kwargs, caller=self, **metadata)
        if backend is NoValue or (backend := Backend(backend)) is self.backend:
            queue = self._queue
        else:
            if not (queue := self._guest_queues.get(backend)):
                with self._child_lock:
                    if backend is Backend.trio:
                        trio.sleep  # noqa: B018 # Check trio is available.
                    if not (queue := self._guest_queues.get(backend)):
                        queue = SingleAsyncQueue(reject=self._reject)
                        self._guest_queues[backend] = queue
                        self._stopping.add_done_callback(lambda _: queue.stop())

                        def _start_guest() -> None:
                            "Start a guest event loop"
                            if self._state is CallerState.running:
                                host = Host.current(self.thread)
                                done = create_async_event(shield=True)
                                get_start_guest_run(backend)(
                                    self._scheduler,
                                    queue,
                                    done_callback=lambda _: done.set(),
                                    run_sync_soon_threadsafe=host.run_sync_soon_threadsafe
                                    if host
                                    else self.call_direct,
                                    run_sync_soon_not_threadsafe=host.run_sync_soon_not_threadsafe if host else None,
                                    host_uses_signal_set_wakeup_fd=host.host_uses_signal_set_wakeup_fd
                                    if host
                                    else True,
                                )
                                self._guest_done_events.add(done)

                        self.call_direct(_start_guest)
        queue.append(pen)
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
            *args: Arguments to use with `func`.
            **kwargs: Keyword arguments to use with `func`.

        Info:
            All call arguments are packed into the instance's metadata.
        """

        async def _call_later(*args: P.args, **kwargs: P.kwargs) -> T:
            if (delay := time.monotonic() - start_time) > 0:
                await anyio.sleep(delay)
            if inspect.iscoroutine(result := func(*args, **kwargs)):
                result = await result
            return result  # pyright: ignore[reportReturnType]

        start_time = time.monotonic()
        return self.schedule_call(_call_later, args, kwargs)

    def call_soon(
        self,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Pending[T]:
        """
        Schedule func to be executed.

        Args:
            func: The function.
            *args: Arguments to use with `func`.
            **kwargs: Keyword arguments to use with `func`.
        """
        return self.schedule_call(func, args, kwargs)

    def call_using_backend(
        self,
        backend: Backend | Literal["asyncio", "trio"],
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Pending[T]:
        """
        Schedule func to be executed using the specified `backend`.

        This methods enables coroutines written for a specific function to be run irresective
        of the callers backend.

        - `backend == caller.backend` - `func` is executed via [Caller.call_soon][].
        - `backend != caller.backend` - `func` is executed with a backend running as a guest.

        Args:
            backend: The backend in which `func` must be executed.
            func: The function.
            *args: Arguments to use with `func`.
            **kwargs: Keyword arguments to use with `func`.

        See also:
            - [Caller.get][]
        """
        return self.schedule_call(func, args, kwargs, None, PendingTracker, Backend(backend))

    def call_direct(
        self,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """
        A low-level function to schedule execution of `func` in caller's scheduler.

        Use this for short-running function calls only.

        Args:
            func: The function.
            *args: Arguments to use with `func`.
            **kwargs: Keyword arguments to use with `func`.

        Warning:

            **Use this method for lightweight calls only!**
        """
        self._queue.append((func, args, kwargs))

    def to_thread(
        self,
        func: Callable[P, T | CoroutineType[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Pending[T]:
        """
        Call `func` in a worker thread using the same backend as the caller.

        Args:
            func: The function.
            *args: Arguments to use with func.
            **kwargs: Keyword arguments to use with func.

        Notes:
            - A pool of workers are maintained.
            - Structured concurrency tools should be used to creating task such as:
                - [Caller.create_pending_group][]
                - [asyncio.TaskGroup][]
                - [anyio.create_task_group][]
        """

        def _to_thread_on_done(_) -> None:
            if not caller.stopped and self.running:
                with self._child_lock:
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
        """
        Returns the pending associated with the `queue_call` for func.

        Notes:
            - `queue_close` is the preferred means to shutdown the queue.
        """
        return self._queue_map.get(hash(func))

    def queue_call(
        self,
        func: Callable[P, Any | Awaitable[Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """
        A low-level function to queue the execution of `func` in a queue unique to it and the caller.

        This sets up a long-lived task to provide a fast pathway for repetitive calls to a function.

        Args:
            func: The function.
            *args: Arguments to use with `func`.
            **kwargs: Keyword arguments to use with `func`.

        Notes:
            - The queue runs inside a task that remains running until one of the following occurs:
                1. The queue is stopped.
                2. The method [Caller.queue_close][] is called with `func` or `func`'s hash.
                3. `func` is deleted (utilising [weakref.finalize][]).
            - The [context][contextvars.Context] of the initial call is used for subsequent queue calls.
            - Exceptions are logged to caller.log but not propagated.
            - The pending created on the first call will only registered with PendingManager subclassed
                trackers and **not** PendingGroup.
        """
        if not (pen_ := self._queue_map.get(key := hash(func))):
            with self._child_lock:
                if not (pen_ := self._queue_map.get(key)):
                    queue = SingleAsyncQueue[tuple[Callable, tuple, dict]](reject=self._reject)
                    with contextlib.suppress(TypeError):
                        weakref.finalize(func.__self__ if inspect.ismethod(func) else func, self.queue_close, key)

                    async def queue_loop() -> None:
                        pen = self.current_pending()
                        assert pen
                        try:
                            async for item in queue:
                                try:
                                    await item[0](*item[1], **item[2])
                                except TypeError:
                                    continue
                                except (anyio.get_cancelled_exc_class(), Exception) as e:
                                    if pen.cancelled():
                                        raise
                                    self.log.exception("Execution of %s failed! args:%s kwargs:%s", *item, exc_info=e)
                                finally:
                                    del item
                        finally:
                            self._queue_map.pop(key)

                    pen_ = self.schedule_call(queue_loop, (), {}, None, PendingManager, key=key, queue=queue)
                    self._queue_map[key] = pen_
        pen_.metadata["queue"].append((func, args, kwargs))

    def queue_close(self, func: Callable | int) -> None:
        """
        Close the [Caller.queue_call][async_kernel.caller.Caller.queue_call] execution queue associated with `func`.

        Args:
            func: The queue of the function to close.
        """
        if pen := self._queue_map.pop(func if isinstance(func, int) else hash(func), None):
            pen.metadata["queue"].stop()
            pen.cancel()

    async def as_completed(
        self,
        items: Iterable[Awaitable[T]] | AsyncGenerator[Awaitable[T]],
        *,
        max_concurrent: NoValue | int = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        cancel_unfinished: bool = True,
    ) -> AsyncGenerator[Pending[T], Any]:
        """
        An async iterator to yield a pending for each awaitable in items as they complete.

        Args:
            items: A container or a generator that yields awaitables.
            max_concurrent: The maximum number of pending to monitor at a time if `items` is a generator.
            cancel_unfinished: Cancel any `pending` when exiting.

        Tip:
            - Pass a generator if you wish to limit the number result jobs when calling to_thread/to_task etc.
            - Pass a container with all results when the limiter is not relevant.
            -  `Caller.MAX_IDLE_POOL_INSTANCES`
        """
        resume = noop
        done: SingleAsyncQueue[Pending[T]] = SingleAsyncQueue()
        unfinished: set[Pending[T]] = set()
        pen_current = self.current_pending()
        if isinstance(items, set | list | tuple):
            max_concurrent_ = 0
        else:
            max_concurrent_ = self.MAX_IDLE_POOL_INSTANCES if max_concurrent is NoValue else int(max_concurrent)

        async def scheduler():
            nonlocal resume
            gen = items if isinstance(items, AsyncGenerator) else iter(items)
            is_async = isinstance(gen, AsyncGenerator)
            while not done.stopped and (pen := await anext(gen, None) if is_async else next(gen, None)) is not None:
                if pen is pen_current:
                    done.stop()
                    msg = "Waiting for the pending in which it is running would result in deadlock!"
                    raise RuntimeError(msg)
                if not isinstance(pen, Pending):
                    pen = cast("Pending[T]", self.call_soon(await_for, pen))
                if not pen.done():
                    unfinished.add(pen)
                    pen.add_done_callback(done.append)
                    if max_concurrent_ and len(unfinished) == max_concurrent_:
                        event = create_async_event()
                        resume = event.set
                        if len(unfinished) == max_concurrent_:
                            await event
                        resume = noop
                else:
                    done.append(pen)
            if not done.queue and not unfinished:
                done.stop()

        pen_ = self.call_soon(scheduler)
        try:
            async for pen in done:
                unfinished.discard(pen)
                yield pen
                if pen_.done() and not unfinished and not done.queue:
                    break
                else:
                    if max_concurrent_ and len(unfinished) < max_concurrent_:
                        resume()
            pen_.result()
        finally:
            done.stop()
            for pen in unfinished:
                pen.remove_done_callback(done.append)
                if cancel_unfinished:
                    pen.cancel("Cancelled by as_completed")
            await pen_.cancel_wait(shield=True)

    async def wait(
        self,
        items: Iterable[Awaitable[T]],
        *,
        timeout: float | None = None,
        return_when: Literal["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"] = "ALL_COMPLETED",
    ) -> tuple[set[Pending[T]], set[Pending[T]]]:
        """
        Wait for one or more of the awaitable items to complete.

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
        pending: set[Pending[T]] = set()
        done = set()
        for item in items:
            if isinstance(item, Pending):
                done.add(item) if item.done() else pending.add(item)
            else:
                assert inspect.isawaitable(item)
                pending.add(self.call_soon(await_for, item))
        if done:
            if return_when == "FIRST_COMPLETED":
                return done, pending
            if return_when == "FIRST_EXCEPTION":
                for pen in done:
                    if pen.cancelled() or pen.exception():
                        return done, pending
        if pending:
            with anyio.move_on_after(timeout):
                async for pen in self.as_completed(pending.copy(), cancel_unfinished=False):
                    pending.discard(pen)
                    done.add(pen)
                    if return_when == "FIRST_COMPLETED":
                        break
                    if return_when == "FIRST_EXCEPTION" and (pen.cancelled() or pen.exception()):
                        break
        return done, pending

    def create_pending_group(self, *, shield: bool = False, mode: Literal[0, 1, 2, 3] = 0) -> PendingGroup:
        """
        Create a new [PendingGroup][async_kernel.pending.PendingGroup].

        [Pending][async_kernel.pending.Pending] created in the context that opt-in by including `PendingTracker`
        as a 'tracker', including all methods on [Caller][] that return pending are automatically registered.

        The context will not exit until all registered pending are complete. The exit and cancellation behaviour
        is determined by the `mode`.

        Args:
            shield: Shield the pending group from external cancellation.
            mode: The mode.
                - 0: Ignore cancellation of pending, if any pending is cancelled - exit quietly.
                - 1: Cancel if any pending is cancelled - raise PendingCancelled on exit.
                - 2: Cancel if any pending is cancelled - exit quietly.
                - 3: Ignore cancellation of pending, if any pending is cancelled - raise PendingCancelled on exit.

        Usage:

            ```python
            async with Caller().create_pending_group() as pg:
                pg.caller.to_thread(my_func)
            ```
        """
        return PendingGroup(shield=shield, mode=mode)
