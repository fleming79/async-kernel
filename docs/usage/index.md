---
title: Usage
description: Usage tips for async kernel.
icon: material/note-text
# subtitle: A sub title
---

# Usage

Async kernel provides a Jupyter kernel that can be used in:

- Jupyter
- VS code
- Other places that can us a python kernel without a gui event loop

Normal Execute requests are queued for execution and will be run sequentially.
Awaiting in cells is fully supported and will not block shell messaging.

Please refer to the notebooks which demonstrate some usage examples.

## Blocking code

Blocking code should be run in a separate thread using one of the following:

1. [anyio][anyio.to_thread.run_sync]
2. [async_kernel.caller.Caller.to_thread][]
3. Using the backend's library
    - [asyncio.to_thread][]
    - [trio.to_thread.run_sync][]

## Caller

[Caller][async_kernel.caller.Caller] was originally developed to simplify message handling in the
[Kernel][async_kernel.kernel.Kernel]. It is now a capable tool in its own right with a convenient
interface for executing synchronous and asynchronous code in a given thread's event loop.

Job scheduling is synchronous, and for methods that return a [Pending][async_kernel.pending.Pending],
the execution result can be cancelled, awaited from any thread or waited synchronously blocking the
thread until the Pending is done.

### Get a Caller

If there is an event loop in the current thread, it is recommended to use:

```python
caller = Caller()
```

### Modifier

A modifier can be passed as the first arg to modify which caller instance is returned:

```python
caller = Caller("MainThread")
```

The options are:

- "CurrentThread": A caller for the current thread. An event loop must be running in the current thread for this to work.
- "MainThread": A caller for the main thread. An event loop must be running in the main thread and if called from inside
  a different thread, the caller must have already been created in the main thread.
- "NewThread": A new thread is always created.
- "manual": A new thread is created. The scheduler must be started manually by either entering the async context
  or calling [Caller.start_sync][async_kernel.caller.Caller.start_sync].

### `Caller.get`

Caller.get [caller.get][async_kernel.caller.Caller.get] can be used to create child callers that belong to the parent.
When the parent is stopped the children are stopped.

The following options are copied from the parent or can be specified.

- 'zmq_context'
- 'backend'
- 'backend_options' (only if the backend matches)

### `Caller.to_thread`

[Caller.to_thread][async_kernel.caller.Caller.to_thread] performs execute requests in a dedicated caller
using the same backend and zmq context as the parent. A pool of workers is retained to handle to thread
calls, but are shutdown when no longer required.

#### worker lifespan

The `to_thread` call is synchronous and returns a Pending for the result of execution. When the
Pending is done the worker becomes 'idle'. The following settings affect what happens to the idle worker:

- [Caller.MAX_IDLE_POOL_INSTANCES][async_kernel.caller.Caller.MAX_IDLE_POOL_INSTANCES]:
  When a worker becomes idle it will stop immediately if the number of idle workers equals this value.
- [Caller.IDLE_WORKER_SHUTDOWN_DURATION][async_kernel.caller.Caller.IDLE_WORKER_SHUTDOWN_DURATION]:
  If this value is greater than zero a thread is started that periodically checks and stops workers
  that have been idle for a duration exceeding this value.

```python
my_worker = caller.get("my own worker", backend="trio")
```

When called inside a thread without a running event loop, a new thread can be started with
an event loop.

```python
caller = Caller(name="my event loop", backend="asyncio")
```
