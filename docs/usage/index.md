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
    - Is both a [classmethod][] and `method` depending of if it is called on the 'Class' or 'instance'.
- [async_kernel.Caller.to_thread][]
    - runs an event loop matching the backend of the originator.
    - maintains a pool of worker threads per caller, which in turn can have its own pool of workers.
- [async_kernel.Caller.queue_call][]
    - A dedicated queue is created specific to the [hash][] of the function.
    - Only one call will run at a time.
    - The context of the original call is retained until the queue is stopped with [async_kernel.caller.Caller.queue_close][].

There is caller instance exists per thread (assuming there is only one event-loop-per-thread).

### `Caller.get`

[Caller.get][async_kernel.caller.Caller.get] is the primary method to obtain a **running** kernel.

When using `get` via a caller instance rather than as a class method, any newly created instances
are considered children and will be stopped if the caller is stopped.

=== "To get a caller from inside an event loop use"

    ```python
    caller = Caller.get()
    ```

=== "New thread specify the backend"

    ```python
    asyncio_caller = Caller.get(name="asyncio backend", backend="asyncio")
    trio_caller = asyncio_caller.get(name="trio backend", backend="trio")
    assert trio_caller in asyncio_caller.children

    asyncio_caller.stop()
    await asyncio_caller.stopped
    assert trio_caller.stopped
    ```
