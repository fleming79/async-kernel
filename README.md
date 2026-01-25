# Async kernel

[![pypi](https://img.shields.io/pypi/pyversions/async-kernel.svg)](https://pypi.python.org/pypi/async-kernel)
[![downloads](https://img.shields.io/pypi/dm/async-kernel?logo=pypi&color=3775A9)](https://pypistats.org/packages/async-kernel)
[![CI](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![basedpyright - checked](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com)
[![Built with Material for MkDocs](https://img.shields.io/badge/Material_for_MkDocs-526CFE?style=plastic&logo=MaterialForMkDocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)
[![codecov](https://codecov.io/github/fleming79/async-kernel/graph/badge.svg?token=PX0RWNKT85)](https://codecov.io/github/fleming79/async-kernel)

![logo-svg](https://github.com/user-attachments/assets/6781ec08-94e9-4640-b8f9-bb07a08e9587)

Async kernel is a Python [Jupyter](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages) kernel
with concurrent message handling.

Messages are processed fairly whilst preventing asynchronous deadlocks by using a unique message handler per `channel`, `message_type` and `subshell_id`.

## Highlights

- [Experimental](https://github.com/fleming79/echo-kernel) support for [Jupyterlite](https://github.com/jupyterlite/jupyterlite) try it online [here](https://fleming79.github.io/echo-kernel/) 👈
- [Debugger client](https://jupyterlab.readthedocs.io/en/latest/user/debugger.html#debugger)
- [anyio](https://pypi.org/project/anyio/) compatible event loops
    - [`asyncio`](https://docs.python.org/3/library/asyncio.html) (default)
    - [`trio`](https://pypi.org/project/trio/)
- [aiologic](https://aiologic.readthedocs.io/latest/) thread-safe synchronisation primitives
- [Easy multi-thread / multi-event loop management](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller)
- [IPython shell](https://ipython.readthedocs.io/en/stable/overview.html#enhanced-interactive-python-shell)
- Per-subshell user_ns
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

## Asyncio

An asyncio kernel based backend with the name 'async' is installed when the kernel is installed.

### Trio backend

To add a kernel spec for a `trio` backend.

```bash
pip install trio
async-kernel -a async-trio --interface.backend=trio
```

For further detail about kernel spec customisation see [command line usage](https://fleming79.github.io/async-kernel/latest/commands/#command-line).

## Message handling

- When a message is received the `msg_handler` is called with:
    - 'job' (a dict of `msg`, `received_time` and `ident`)
    - the [`channel`](#channel)
    - `msg_type`
    - A function `send_reply`

- The `msg_handler`
    - determines the `subshell_id` and [run mode](#run-mode).
    - obtains the `handler` from the kernel with the same name as the `msg_type`.
    - determines the [run mode](#run-mode)
    - creates cached version of the `run_handler` with a unique version per:
        - The `handler`
        - `channel`
        - `subshell_id`
        - send_reply (constant or per-channel)
    - Obtains the caller associated with the channel and schedules execution of the cached handler

### Run mode

The run modes available are:

- `RunMode.direct` → [`Caller.call_direct`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller.call_direct):
  Run the request directly in the scheduler.
- `RunMode.queue` → [`Caller.queue_call`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller.queue_call):
  Run the request in a queue dedicated to the subshell, handler & channel.
- `RunMode.task` → [`Caller.call_soon`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller.call_soon):
  Run the request in a separate task.
- `RunMode.thread` → [`Caller.to_thread`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller.to_thread):
  Run the request in a separate worker thread.

These are the currently assigned run modes.

| SocketID                | shell  | control |
| ----------------------- | ------ | ------- |
| comm_close              | direct | direct  |
| comm_info_request       | direct | direct  |
| comm_msg                | queue  | queue   |
| comm_open               | direct | direct  |
| complete_request        | thread | thread  |
| create_subshell_request | None   | thread  |
| debug_request           | None   | queue   |
| delete_subshell_request | None   | thread  |
| execute_request         | queue  | queue   |
| history_request         | thread | thread  |
| inspect_request         | thread | thread  |
| interrupt_request       | direct | direct  |
| is_complete_request     | thread | thread  |
| kernel_info_request     | direct | direct  |
| list_subshell_request   | None   | direct  |
| shutdown_request        | None   | direct  |

## Origin

Async kernel started as a [fork](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d)
of [IPyKernel](https://github.com/ipython/ipykernel). Thank you to the original contributors of IPyKernel that made Async kernel possible.

[^non-main-thread]: The Shell can run in other threads with the associated limitations with regard to signalling and interrupts.
