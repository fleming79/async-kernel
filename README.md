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

Async kernel is an IPython kernel for [Jupyter](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages)
that provides concurrent message handling via an asynchronous backend (asyncio or trio).

The kernel provides two external interfaces:

1. Direct ZMQ socket messaging via a configuration file and kernel spec - (Jupyter, VScode, etc).
2. An experimental callback style interface (Jupyterlite).

**[Documentation](https://fleming79.github.io/async-kernel/)**

## Highlights

- [Debugger client](https://jupyterlab.readthedocs.io/en/latest/user/debugger.html#debugger)
- [anyio](https://pypi.org/project/anyio/) compatible asynchronous backend ([`asyncio`](https://docs.python.org/3/library/asyncio.html) (default) or [`trio`](https://pypi.org/project/trio/))
- [aiologic](https://aiologic.readthedocs.io/latest/) thread-safe synchronisation primitives
- [Backend agnostic multi-thread / multi-event loop management](https://fleming79.github.io/async-kernel/latest/usage/#caller)
- [IPython shell](https://ipython.readthedocs.io/en/stable/overview.html#enhanced-interactive-python-shell)
- Per-subshell user_ns
- GUI event loops [^1][^2]
    - [x] inline
    - [x] ipympl
    - [x] tk host and asyncio[^3] or trio[^4] backend running as a guest
    - [x] qt host and asyncio[^3] or trio[^4] backend running as a guest
- [Experimental](https://github.com/fleming79/echo-kernel) support for
  [Jupyterlite](https://github.com/jupyterlite/jupyterlite) (try it online [here](https://fleming79.github.io/echo-kernel/) 👈)

[^1]:
    A gui (_host_) enabled kernel runs a gui event loop with the asynchronous backend
    running as guest. The host must be set before the kernel is started. This was a
    deliberate design choice to to ensure good performance and reliability.

[^2]:
    It is also possible to use a caller to run a gui event loop
    in a separate thread (with a backend running as a guest) if the gui allows it
    (qt will only run in the main thread). Also note that pyplot will only permit
    one interactive gui library in a process.

[^3]:
    The asyncio implementation of `start_guest_run` was written by
    [the author of aiologic](https://github.com/x42005e1f/aiologic) and provided as a
    [gist](https://gist.github.com/x42005e1f/857dcc8b6865a11f1ffc7767bb602779).

[^4]: trio's [start_guest_run](https://trio.readthedocs.io/en/stable/reference-lowlevel.html#trio.lowlevel.start_guest_run).

### Prevent asynchronous deadlocks

The standard (synchronous) kernel implementation processes messages sequentially irrespective
of the message type. The problem being that long running requests make the kernel
non-responsive. Furthermore, Any asynchronous request that awaits a result delivered
via a kernel message will suffer deadlock since the message will be stuck in the queue.

One example could be an 'execute request' that awaits an event set by a widget button click[^5].

Async kernel handles messages according to the message type, so widget com messages can pass
freely while an execute request is being processed; [further detail](https://fleming79.github.io/async-kernel/latest/notebooks/concurrency/).

[^5]:
    Ipykernel _solves_ this issue specifically for widgets by using the concept of
    'widget coms over subshells'. Widget messages arrive in a different thread which on
    occasion can cause unexpected behaviour, especially when using asynchronous libraries.

## Installation

```bash
pip install async-kernel
```

## Kernel specs

A kernel spec with the name 'async' is added when async kernel is installed.

Kernel specs can be added/removed via the command line.

The kernel is configured via the interface with the options:

- `name`
- `display_name`
- Parameters on the kernel including:
    - [`interface.backend`](#backends)
    - `interface.backend_options`
    - `interface.loop`
    - `interface.loop_options`
    - `shell.timeout`

### Backends

The backend set on the interface is the asynchronous library the kernel uses for message handling.
It is also the asynchronous library directly available when executing code in cells or via a console[^4].

[^4]:
    Irrespective of the configured backend, functions/coroutines can be executed using a specific backend
    with the method [`call_using_backend`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller.call_using_backend).

#### Example - overwrite the 'async' kernel spec to use a trio backend

```bash
pip install trio
async-kernel -a async --interface.backend=trio
```

### Gui event loop

The kernel can be started with a gui event loop as the _host_ and the _backend_ running as a guest.

#### asyncio backend

```bash
# tk
async-kernel -a async-tk --interface.loop=tk

# qt
pip install PySide6-Essentials
async-kernel -a async-qt --interface.loop=qt
```

#### trio backend

```bash
pip install trio
# tk
async-kernel -a async-tk --interface.loop=tk --interface.backend=trio

# qt
pip install PySide6-Essentials
async-kernel -a async-qt --interface.loop=qt --interface.backend=trio
```

For further detail about kernel spec customisation see [command line and kernel configuration](https://fleming79.github.io/async-kernel/latest/usage/commands/).

## Origin

Async kernel started as a [fork](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d)
of [IPyKernel](https://github.com/ipython/ipykernel). Thank you to the original contributors of IPyKernel that made Async kernel possible.
