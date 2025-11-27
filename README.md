# Async kernel

[![pypi](https://img.shields.io/pypi/pyversions/async-kernel.svg)](https://pypi.python.org/pypi/async-kernel)
[![downloads](https://img.shields.io/pypi/dm/async-kernel?logo=pypi&color=3775A9)](https://pypistats.org/packages/async-kernel)
[![CI](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![basedpyright - checked](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com)
[![Built with Material for MkDocs](https://img.shields.io/badge/Material_for_MkDocs-526CFE?style=plastic&logo=MaterialForMkDocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)
[![codecov](https://codecov.io/github/fleming79/async-kernel/graph/badge.svg?token=PX0RWNKT85)](https://codecov.io/github/fleming79/async-kernel)

Async kernel is an asynchronous Python [Jupyter kernel](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages)
with [concurrent message handling](#run-mode).

## Highlights

- [Concurrent message handling](https://fleming79.github.io/async-kernel/latest/notebooks/simple_example/)
- [Configurable backend](https://fleming79.github.io/async-kernel/latest/commands/#add-a-kernel-spec)
- [Debugger client](https://jupyterlab.readthedocs.io/en/latest/user/debugger.html#debugger)
    - [anyio](https://pypi.org/project/anyio/)
        - [`asyncio` backend](https://docs.python.org/3/library/asyncio.html) (default)[^uv-loop]
        - [`trio` backend](https://pypi.org/project/trio/)
- [IPython shell](https://ipython.readthedocs.io/en/stable/overview.html#enhanced-interactive-python-shell)
  provides:
    - code execution
    - magic
    - code completions
    - history
- Thread-safe execution and cancellation (utilising [aiologic](https://aiologic.readthedocs.io/latest/) synchronisation primitives).
    - [Caller](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller) - code execution in a chosen event loop
    - [Pending](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.pending.Pending) - wait/await/cancel the pending result
- GUI event loops
    - [x] inline
    - [x] ipympl
    - [ ] tk
    - [ ] qt

**[Documentation](https://fleming79.github.io/async-kernel/)**

## Installation

```bash
pip install async-kernel
```

### Trio backend

To add a kernel spec for a `trio` backend.

```bash
pip install trio
async-kernel -a async-trio
```

See also: [command line usage](https://fleming79.github.io/async-kernel/latest/commands/#command-line).

## Asynchronous event loops

Async kernel uses [`Caller`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller)
for concurrent message handling.

There are two callers:

- `Shell` - runs in the `MainThread` handling user related requests[^non-main-thread].
- `Control` - runs in a separate thread handling control related requests.

### Messaging

Messages are received in a separate thread (per-channel) then handled in the associated thread (shell/control) concurrently according to the determined run mode.

### Run mode

The run modes available are:

- `RunMode.direct` → [`Caller.call_direct`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller.call_direct):
  Run the request in the scheduler.
- `RunMode.queue` → [`Caller.queue_call`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller.queue_call):
  Run the request in a queue dedicated to the handler & channel.
- `RunMode.task` → [`Caller.call_soon`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller.call_soon):
  Run the request in a separate task.
- `RunMode.thread` → [`Caller.to_thread`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller.to_thread):
  Run the request in a separate worker thread.

These are the currently assigned run modes.

| MsgType             | shell  | control |
| ------------------- | ------ | ------- |
| comm_close          | queue  | queue   |
| comm_info_request   | queue  | queue   |
| comm_msg            | queue  | queue   |
| comm_open           | queue  | queue   |
| complete_request    | queue  | queue   |
| debug_request       | None   | queue   |
| execute_request     | queue  | task    |
| history_request     | thread | thread  |
| inspect_request     | thread | thread  |
| interrupt_request   | queue  | queue   |
| is_complete_request | queue  | queue   |
| kernel_info_request | queue  | queue   |
| shutdown_request    | None   | queue   |

#### Further detail

- [`MsgType`](https://fleming79.github.io/async-kernel/latest/reference/typing/#async_kernel.typing.MsgType) docs.
- [`Kernel.receive_msg_loop`](https://fleming79.github.io/async-kernel/latest/reference/kernel/#async_kernel.kernel.Kernel.receive_msg_loop) docs.
- [Concurrency](https://fleming79.github.io/async-kernel/latest/notebooks/concurrency/) notebook.

## Origin

Async kernel started as a [fork](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d)
of [IPyKernel](https://github.com/ipython/ipykernel). Thank you to the original contributors of IPyKernel that made Async kernel possible.

[^uv-loop]: Uvloop is not a dependency of async-kernel but will be used if it has been installed.

[^non-main-thread]: The Shell can run in other threads with the associated limitations with regard to signalling and interrupts.
