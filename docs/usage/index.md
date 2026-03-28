---
title: Usage
description: Usage tips for async-kernel.
icon: material/note-text
# subtitle: A sub title
---

async-kernel feels like a standard kernel that offers advanced features that improved user experience.

## Features

- Separate handlers for msg_type and channel allow messages to be processed concurrently.
- Code execution is top-level awaitable.
- Choice of async backend (asyncio or trio).
- Choice of asyncio event loop when started by anyio (not as a guest).
- Optional gui host event loop.
- Using [Caller.call_using_backend][async_kernel.caller.Caller.call_using_backend], code from any backend can be called in any thread.

## [Command line](../usage/commands.md): Detail about command line usage including:

    - Adding a kernel spec
    - Deleting a kernel spec
    - Starting a kernel

## Notebooks

- [Caller: A backend agnostic executor](../notebooks/caller.ipynb)
- [Concurrent message handling](../notebooks/concurrency.ipynb)

Notebooks[^1] in this documentation show the result of each cell after executing for a short duration (~100ms).
Notebooks are designed run with a Jupyter frontend.

You can download the notebook with the button at the top right of the page for the notebook.

[^1]: Further detail about how notebooks are generated is provided [here](../about/contributing.md#notebooks).
