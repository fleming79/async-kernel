# Async kernel

[![pypi](https://img.shields.io/pypi/pyversions/async-kernel.svg)](https://pypi.python.org/pypi/async-kernel)
[![downloads](https://img.shields.io/pypi/dm/async-kernel?logo=pypi&color=3775A9)](https://pypistats.org/packages/async-kernel)
[![CI](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![basedpyright - checked](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com)
[![Built with Material for MkDocs](https://img.shields.io/badge/Material_for_MkDocs-526CFE?style=plastic&logo=MaterialForMkDocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)
[![codecov](https://codecov.io/github/fleming79/async-kernel/graph/badge.svg?token=PX0RWNKT85)](https://codecov.io/github/fleming79/async-kernel)

Async kernel is a Python [Jupyter kernel](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages) that enables concurrent handling of messages and execution.

## Highlights

- [Concurrent message handling](https://fleming79.github.io/async-kernel/latest/notebooks/concurrency/)
- [Debugger client](https://jupyterlab.readthedocs.io/en/latest/user/debugger.html#debugger)
- [Configurable backend](https://fleming79.github.io/async-kernel/latest/commands/#add-a-kernel-spec)
    - [anyio](https://pypi.org/project/anyio/)
        - [`asyncio` backend](https://docs.python.org/3/library/asyncio.html) (default)[^uv-loop]
        - [`trio` backend](https://pypi.org/project/trio/)
- [IPython shell](https://ipython.readthedocs.io/en/stable/overview.html#enhanced-interactive-python-shell)
  provides:
    - code execution
    - magic
    - code completions
    - history
- Thread safe (thanks to [aiologic](https://aiologic.readthedocs.io/latest/))
    - `Caller` for starting and scheduling of code execution in its event loop
    - `Future` for a pending result
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

### Trio

To add a kernel spec for `trio`.

```bash
pip install trio
async-kernel -a async-trio
```

## Asynchronous event loops

Async kernel provides two event loops (one per channel):

- `shell`: The shell event loop runs in the thread where it is started; normally the `MainThread` (it is possible to run the shell in other threads)
- `control`: The control event loop always starts in a new thread named 'ControlThread'

### Messaging

Messages are received in a separate thread (per-channel) and scheduled in in the channels event loop.

### Run mode

The run mode is defined per-message type and channel and is one of the following:

- `queue`
- `thread`
- `task`
- `blocking`

The currently defined run modes are:

| MsgType             | control  | shell    |
| ------------------- | -------- | -------- |
| comm_close          | blocking | blocking |
| comm_info_request   | blocking | blocking |
| comm_msg            | queue    | queue    |
| comm_open           | blocking | blocking |
| complete_request    | thread   | thread   |
| debug_request       | queue    | None     |
| execute_request     | task     | queue    |
| history_request     | thread   | thread   |
| inspect_request     | thread   | thread   |
| interrupt_request   | blocking | blocking |
| is_complete_request | thread   | thread   |
| kernel_info_request | blocking | blocking |
| shutdown_request    | blocking | None     |

For further detail see the [notebook on concurrency](https://fleming79.github.io/async-kernel/latest/notebooks/concurrency/).

## Origin

Async kernel started as a [fork](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d)
of [IPyKernel](https://github.com/ipython/ipykernel). Thank you to the original contributors of IPyKernel that made Async kernel possible.

[^uv-loop]: Uvloop is not a dependency of async-kernel but will be used if it has been installed.
