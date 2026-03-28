# Thread Safety of Built-in Types

- [Terminology](https://py-free-threading.github.io/documentation-principles/#free-threading-terminology)

_aync-kernel_ utilises [aiologic](https://aiologic.readthedocs.io/latest/index.html) for its _thread-safe_ primitives.

## [async_kernel.pending.Pending][]

### Guarantees

- **Internally synchronised**
    - All Public methods can be safely called from any thread.
    - Instance are sync/async awaitable from any thread with any backend.

- **Limitations**

    [add_done_callback][async_kernel.pending.Pending.add_done_callback]: The callback is called from the thread in which it is set
    so should provide it's own synchronisation as needed.

## [async_kernel.caller.Caller][]

`Caller` is thread-local in CPython and context-local in Pyodide.

### Guarantees

- **Internally synchronised**

- **Limitations**
    - [queue_call][async_kernel.caller.Caller.queue_call] is internally synchronised per-function after
      the first call (per-function) has returned.

## [async_kernel.common.Fixed][]

### Guarantees

- **Internally synchronised**

### Other classes

TODO
