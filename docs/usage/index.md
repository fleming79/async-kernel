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
2. [async_kernel.Caller.to_thread][]
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

### `Caller.get`

Once you have a Caller instance you can use [caller.get][async_kernel.caller.Caller.get]
to create child callers that belong to the parent. When the parent is stopped the
children are stopped.

The following options are copied from the parent or can be specified.

- 'zmq_context'
- 'backend'
- 'backend_options' (only if the backend matches)

### `Caller.to_thread`

[Caller.to_thread][async_kernel.caller.Caller.to_thread] internally uses `Caller.get` to
create its workers. A worker uses the same 'backend' and 'zmq_context' as its parent. The parent
maintains a pool of idle workers, and starts additional workers on demand. There is no constraint
on the number of workers a parent constraint.

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

=== "Async context current thread"

    ```python
    async with Caller("manual") as caller:
        pass
    ```
    - A caller must **not** already be running in the current thread.
    - When the async context is exited the caller and its children are stopped immediately.
    - A context can be entered again with a new caller instance.
    - This can be useful in testing where a fixture can be used to get a running caller.

=== "Sync thread specify backend"

    ```python
    caller = Caller(name="My trio thread", backend="trio")
    ```
    -  The caller should be stopped when it is no longer required.

=== "Child threads"

    ```python
    async with Caller("manual") as caller:
        child_1 = caller.get()
        child_2 = caller.get(name="asyncio backend", backend="asyncio")
        child_3 = caller.get(name="trio backend", backend="trio")
        assert caller.children == {child_1, child_2, child_3}
    assert not caller.children
    ```

    - Child threads are shutdown with the 'parent'.
