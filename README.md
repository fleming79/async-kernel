# Async kernel

[![image](https://img.shields.io/pypi/pyversions/async-kernel.svg)](https://pypi.python.org/pypi/async-kernel)
[![CI](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fleming79/async-kernel/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![basedpyright - checked](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com)
[![Built with Material for MkDocs](https://img.shields.io/badge/Material_for_MkDocs-526CFE?style=plastic&logo=MaterialForMkDocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)

Async kernel is a Python [Jupyter kernel](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages) that runs in an [anyio](https://pypi.org/project/anyio/) event loop.

## Highlights

- Concurrent message handling
- [Debugger client](https://jupyterlab.readthedocs.io/en/latest/user/debugger.html#debugger)
- Configurable backend - "asyncio" (default) or "trio backend" [^config-backend]
- [IPython shell](https://ipython.readthedocs.io/en/stable/overview.html#enhanced-interactive-python-shell) provides:
    - code execution
    - magic
    - code completions
    - history

## Installation

```shell
pip install async-kernel
```

### Trio

To add a kernel spec for `trio`.

```shell
pip install trio
```

```shell
async-kernel -a async-trio
```

[![Link to demo](https://github.com/user-attachments/assets/9a4935ba-6af8-4c9f-bc67-b256be368811)](https://fleming79.github.io/async-kernel/simple_example/ "Show demo notebook.")

## Origin

Async kernel started as a [fork](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d)
of [IPyKernel](https://github.com/ipython/ipykernel). Thank you to the original contributors of IPyKernel that made Async kernel possible.
