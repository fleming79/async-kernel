# async-kernel

[![pypi](https://img.shields.io/pypi/pyversions/async-kernel.svg)](https://pypi.python.org/pypi/async-kernel)
[![downloads](https://img.shields.io/pypi/dm/async-kernel?logo=pypi&color=3775A9)](https://pypistats.org/packages/async-kernel)
[![CI](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![basedpyright - checked](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com)
[![Built with Material for MkDocs](https://img.shields.io/badge/Material_for_MkDocs-526CFE?style=plastic&logo=MaterialForMkDocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)
[![codecov](https://codecov.io/github/fleming79/async-kernel/graph/badge.svg?token=PX0RWNKT85)](https://codecov.io/github/fleming79/async-kernel)

![logo-svg](https://github.com/user-attachments/assets/6781ec08-94e9-4640-b8f9-bb07a08e9587)

async-kernel provides Python [Jupyter](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages) kernels and clients
compatible with CPython (Jupyter & VS code) and Pyodide (Jupyterlite).

The kernel interface supports multiple connections including:

1. Messaging via ZMQ sockets (Jupyter, VS Code, etc).
2. Same-process local client enabling (Jupyterlite) and user access.

## Highlights

- Built using [aiologic](https://aiologic.readthedocs.io/latest/) thread-safe synchronisation primitives.
- The [`Caller`](https://fleming79.github.io/async-kernel/latest/reference/caller/#async_kernel.caller.Caller)
  class provides a powerful but simple interface for cross-thread code execution in asyncio and trio
  backends, with guest event loop support built in.
- A zmq poll event loop for thread-safe zmq sockets.
- [IPython shell](https://ipython.readthedocs.io/en/stable/overview.html#enhanced-interactive-python-shell)
    - top-level await ('asyncio' or 'trio' backend) in cells
    - async magic function support in cells
- Per-subshell user_ns
- GUI event loops [^1]
    - [x] inline
    - [x] ipympl
    - [x] tk host and asyncio[^2] or trio[^3] backend running as a guest
    - [x] qt host and asyncio[^2] or trio[^3] backend running as a guest
- [Experimental](https://github.com/fleming79/echo-kernel) support for
  [Jupyterlite](https://github.com/jupyterlite/jupyterlite) (try it online [here](https://fleming79.github.io/echo-kernel/) 👈)
    - `%pip install` magic (using micropip)
- [Debugger client](https://jupyterlab.readthedocs.io/en/latest/user/debugger.html#debugger)
- Local client.
- ZMQ client.

[^1]:
    A gui (_host_) enabled kernel interface starts a gui's mainloop (host) which starts
    the backend as a guest, then finally the Kernel is started.

[^2]:
    The asyncio implementation of `start_guest_run` was written by
    [the author of aiologic](https://github.com/x42005e1f/aiologic) and provided as a
    [gist](https://gist.github.com/x42005e1f/857dcc8b6865a11f1ffc7767bb602779).

[^3]: trio's [start_guest_run](https://trio.readthedocs.io/en/stable/reference-lowlevel.html#trio.lowlevel.start_guest_run).

## Installation

```bash
pip install async-kernel
```

## Kernelspecs

A kernelspec with the name 'async' is added when async-kernel is installed.

Kernel specs can be installed/uninstalled via the command line.

```bash
async-kernel install

# To install for a user
async-kernel install --user
```

For further detail about kernel spec customisation see [command line and kernel configuration](https://fleming79.github.io/async-kernel/latest/usage/commands/) and [custom kernel.ipynb](https://fleming79.github.io/async-kernel/latest/notebooks/custom_kernel/).

## Faster data serialization

[orjson](https://github.com/ijl/orjson) (a fast JSON library) is supported and will be used by default if it has been installed.

## Free-threading support

async-kernel's Caller's are _thread-local_ and it's methods are _internally synchronised_[^4].

[^4]: [free threading terminology](https://py-free-threading.github.io/documentation-principles/#free-threading-terminology)

## Origin

async-kernel started as a [fork](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d)
of [IPyKernel](https://github.com/ipython/ipykernel). Thank you to the original contributors of IPyKernel that made async-kernel possible.
