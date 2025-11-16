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

## Blocking calls

Unless you deliberately want to block the shells event loop; blocking calls should be run in a separate thread.

To run code in another thread there are a few options:

1. Use [anyio][anyio.to_thread.run_sync]
2. Use Async kernel's [Caller.get().to_thread][async_kernel.Caller.to_thread]
3. Use the backend's library
    - [asyncio][asyncio.to_thread]
    - [trio][trio.to_thread.run_sync]

## Caller

Caller was developed to enable scheduling for [async_kernel.Kernel] but is also has wider usage.

There is caller instance exists per thread (assuming there is only one event-loop-per-thread).

### `Caller.get`

[Caller.get][async_kernel.caller.Caller.get] is the primary method to obtain a **running** kernel.

When using `get` via a caller instance rather than as a class method, any newly created instances
are considered children and will be stopped if the caller is stopped.

=== To get a caller from inside an event loop use:

    ```python
    caller = Caller.get()
    ```

=== New thread specify the backend:

```python
asyncio_caller = Caller.get(name="asyncio backend", backend="asyncio")
trio_caller = asyncio_caller.get(name="trio backend", backend="trio")
assert trio_caller in asyncio_caller.children

asyncio_caller.stop()
await asyncio_caller.stopped
assert trio_caller.stopped
```
