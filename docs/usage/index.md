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
interface for executing synchronous and asynchronous code in its corresponding thread.

### Get a Caller

When there is a backend already running in the current thread use:

```python
caller = Caller()
```

### Typical usage

Caller is thread based scheduler that is designed to run code asynchronously.

```python
pen = Caller().call_soon(my_async_func, *args, **kwargs)

my_result = await pen
```

It can also be used for structured concurrency styled workflows.

```python
async with Caller().create_pending_group() as pg:
    pg.caller.call_soon(my_async_func, *args, **kwargs)
```

#### Modifier

A modifier can be passed as the first arg to modify which caller instance is returned:

```python
caller = Caller("MainThread")
```

The options are:

- "CurrentThread": A caller for the current thread (default). An backend must already be running in the current thread.
- "MainThread": A caller for the main thread.
- "NewThread": A new thread is always created with the desired configuration.
- "manual": A new caller instance is created in the current thread, there must not already be a caller
  for the current thread. It must be started either - synchronously: with [caller.start_sync][async_kernel.caller.Caller.start_sync]. - asynchronously: by entering the async context.

### `Caller.get`

[Caller.get][async_kernel.caller.Caller.get] can be used to create a child thread with a caller.
A child caller is closed when the parent is closed. The same caller can be access by specifying a name.

The following options are copied from the parent when not specified in the function call.

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
