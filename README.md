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
- GUI event loops [^gui note]
    - [x] inline
    - [x] ipympl
    - [x] tk with asyncio[^asyncio guest] or trio backend running as a guest
    - [x] qt with asyncio[^asyncio guest] or trio backend running as a guest

[^gui note]: The kernel must be set to use the required event loop and the necessary dependencies must be installed
separately. Switching event loops is not supported, however starting an event loop in a thread is possible provided
the gui supports it (qt can only run in the main thread).

[^asyncio guest]: The asyncio implementation of `start_guest_run` was written by [the author of aiologic](https://github.com/x42005e1f/aiologic)
and provided as a ([gist](https://gist.github.com/x42005e1f/857dcc8b6865a11f1ffc7767bb602779)).

**[Documentation](https://fleming79.github.io/async-kernel/)**

## Installation

```bash
pip install async-kernel
```

## Kernel specs

A kernel spec with the name 'async' is added when async kernel is installed.

Kernel specs can be added/removed via the command line.

The kernel is configured via the interface with the options:

- [`interface.backend`](#backends)
- `interface.backend_options`
- `interface.loop`
- `interface.loop_options`

### Backends

The backend defines the asynchronous library provided in the thread in which it is running.

- asyncio
- trio

**Example - change kernel spec to use trio**

```bash
pip install trio
async-kernel -a async --interface.backend=trio
```

### Gui event loop

The kernel can be started with a gui event loop as the _host_ and the _backend_ running as a guest.

**asyncio backend:**

```bash
# tk
async-kernel -a async-tk --interface.loop=tk

# qt
pip install PySide6-Essentials
async-kernel -a async-qt --interface.loop=qt
```

**trio backend**

```bash
pip install trio
# tk
async-kernel -a async-tk --interface.loop=tk --interface.backend=trio

# qt
pip install PySide6-Essentials
async-kernel -a async-qt --interface.loop=qt --interface.backend=trio
```

For further detail about kernel spec customisation see [command line usage](https://fleming79.github.io/async-kernel/latest/commands/#command-line).

## Origin

Async kernel started as a [fork](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d)
of [IPyKernel](https://github.com/ipython/ipykernel). Thank you to the original contributors of IPyKernel that made Async kernel possible.
