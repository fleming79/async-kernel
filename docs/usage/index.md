---
title: Usage
description: Usage tips for async-kernel.
icon: material/note-text
# subtitle: A sub title
---

The kernel experience will feel the same as IPykernel, but offers some extended features
which have been proposed as a [JEP](https://github.com/jupyter/enhancement-proposals/issues/144).
These extended features are illustrated in the [notebooks](#notebooks) section.

In the process of implementing the capabilities imagined in the kernel, the [Caller][async_kernel.caller.Caller]
and [Pending][async_kernel.pending.Pending] objects were created. These are thread-safe objects
which provide a consistent and powerful cross-thread code execution and waiting.

## Command line

[Command line usage](../usage/commands.md) including:

- Adding a kernel spec.
- Deleting a kernel spec.
- Starting a kernel.
- Listing configuration options.

## Notebooks

- [Caller: A backend agnostic executor](../notebooks/caller.ipynb)
- [Concurrent message handling](../notebooks/async_kernel.ipynb)
- [Client](../notebooks/client.ipynb)
- [Custom kernel](../notebooks/client.ipynb)

Notebooks[^1] in this documentation show the result of each cell after executing for a short duration (~1s).
Notebooks are designed run with a Jupyter frontend.

You can download the notebook with the button at the top right of the page for the notebook.

[^1]: Further detail about how notebooks are generated is provided [here](../about/contributing.md#notebooks).
