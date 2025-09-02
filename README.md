# Async kernel

[![pypi](https://img.shields.io/pypi/pyversions/async-kernel.svg)](https://pypi.python.org/pypi/async-kernel)
[![downloads](https://img.shields.io/pypi/dm/async-kernel?logo=pypi&color=3775A9)](https://pypistats.org/packages/async-kernel)
[![CI](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![basedpyright - checked](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com)
[![Built with Material for MkDocs](https://img.shields.io/badge/Material_for_MkDocs-526CFE?style=plastic&logo=MaterialForMkDocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)
[![codecov](https://codecov.io/github/fleming79/async-kernel/graph/badge.svg?token=PX0RWNKT85)](https://codecov.io/github/fleming79/async-kernel)

Async kernel is a Python [Jupyter kernel](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages) that runs in an [anyio](https://pypi.org/project/anyio/) event loop.

**[Documentation](https://fleming79.github.io/async-kernel/)**

## Highlights

- [Concurrent message handling](https://fleming79.github.io/async-kernel/latest/notebooks/concurrency/)
- [Debugger client](https://jupyterlab.readthedocs.io/en/latest/user/debugger.html#debugger)
- [Configurable backend](https://fleming79.github.io/async-kernel/latest/commands/#add-a-kernel-spec)
    - Asyncio (default)
        - [uvloop](https://pypi.org/project/uvloop/) enabled by default[^uv-loop]
    - [trio](https://pypi.org/project/trio/) backend
- [IPython shell](https://ipython.readthedocs.io/en/stable/overview.html#enhanced-interactive-python-shell) provides:
    - code execution
    - magic
    - code completions
    - history

[![Link to demo](https://github.com/user-attachments/assets/9a4935ba-6af8-4c9f-bc67-b256be368811)](https://fleming79.github.io/async-kernel/simple_example/ "Show demo notebook.")

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

## Origin

Async kernel started as a [fork](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d)
of [IPyKernel](https://github.com/ipython/ipykernel). Thank you to the original contributors of IPyKernel that made Async kernel possible.

[^uv-loop]: Uvloop is not a dependency of async-kernel but will be used if it has been installed.
