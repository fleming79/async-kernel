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

Blocking code should be run in outside the shell thread using one of the following:

1. [anyio][anyio.to_thread.run_sync]
2. [async_kernel.Caller.to_thread][]
3. Using the backend's library
    - [asyncio.to_thread][]
    - [trio.to_thread.run_sync][]

## Caller

Caller was originally developed to simplify message handling the the [Kernel][async_kernel.kernel.Kernel].
It is now a capable tool with a convenient interface for executing synchronous and asynchronous code
in a given thread's event loop.

It has a few unique features worth mentioning:

- [async_kernel.Caller.get][]
    - Retrieves existing or creates a caller instance according the 'thread' or 'name'.
- [async_kernel.Caller.to_thread][]
    - runs an event loop matching the backend of the originator.
    - maintains a pool of worker threads per caller, which in turn can have its own pool of workers.
- [async_kernel.Caller.queue_call][]
    - A dedicated queue is created specific to the [hash][] of the function.
    - Only one call will run at a time.
    - The context of the original call is retained until the queue is stopped with [async_kernel.caller.Caller.queue_close][].
- The result of [async_kernel.Caller.call_soon][], [async_kernel.Caller.call_later][] and [async_kernel.Caller.to_thread][]
  return a [async_kernel.Pending][] which can be used to wait/await for the result of execution.

There is only ever one caller instance per thread (assuming there is only one event-loop-per-thread).

### `Caller()`

[Caller()][async_kernel.caller.Caller.__new__] is means to obtain a specific kernel.

A child thread caller can be obtained by using the

=== "A thread with a running event loop"

    ```python
    caller = Caller()
    ```

    - Useful where the caller should stay open for the life of the thread.

=== "A thread with a running event loop (async context)"

    ```python
    async with Caller("async-context") as caller:
        ...
    ```

    - When the context is exited, the caller and its children are stopped immediately.
    - A new caller context can be started after exiting.
    - This is recommended in testing where you have control of the thread and context.

=== "New thread specifying the backend"

    ```python
    asyncio_caller = Caller().get(name="asyncio backend", backend="asyncio")
    trio_caller = asyncio_caller.get(name="trio backend", backend="trio")
    assert trio_caller in asyncio_caller.children

    asyncio_caller.stop()
    await asyncio_caller.stopped
    assert trio_caller.stopped
    ```
