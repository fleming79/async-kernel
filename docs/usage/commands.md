# Command line

The command `async-kernel` and alias `async_kernel` are provided at the command line.

The primary options available are:

- [Add kernel spec](#kernel-spec): `-a <name> <options>`
- [Remove a kernel spec](#remove-a-kernel-spec) `-r <name>`
- [Start a kernel](#start-a-kernel): `-f <path to config file>`

## Kernel spec

Kernel specs can be added/removed via the command line:

**Example**

```console
async-kernel -a async --backend=trio --loop=tk --display_name="Async python tk trio"
```

The kernel spec looks like this:

```json
{
    "argv": [
        "python",
        "-m",
        "async_kernel",
        "-f",
        "{connection_file}",
        "--start_interface=launch_zmq_kernel",
        "--name=async",
        "--backend=trio",
        "--host=tk"
    ],
    "env": {},
    "display_name": "Async python tk trio",
    "language": "python",
    "interrupt_mode": "message",
    "metadata": {
        "debugger": true,
        "concurrent": true
    },
    "kernel_protocol_version": "5.5"
}
```

A single kernel spec is created in the folder `<sys.prefix>/share/jupyter/kernels/async` when async-kernel is installed with the following defaults.

**Defaults**

- name: 'async'
- display_name: 'async'
- backend: `asyncio`
- backend_options: `{'use_uvloop':True}` if uvloop or winloop is installed
- host: `None`
- host_options: `None`

The kernel spec can be updated by adding a kernel spec with the same name ('async').

## Backend (`interface-backend`)

There are two supported backends 'asyncio' and 'trio'.

In CPython the [backend][async_kernel.typing.Backend] is started using [anyio.run][].
The type of backend can be specified at the attribute [interface.backend][async_kernel.interface.BaseKernelInterface.backend].
The backend_options can be specified at the attribute [interface.backend_options][async_kernel.interface.zmq.ZMQKernelInterface.backend_options].
Options can be written as a literal python string.

```console
async-kernel -a async-trio --backend=trio
```

## Kernel spec location

See [here](https://jupyter-client.readthedocs.io/en/latest/kernels.html#kernel-specs) for a list of locations where Jupyter/IPython
searches for the kernel specs.

The path where the kernel spec is installed/deleted can also be specified by either `prefix` or `folder`.

**Options**

- prefix (optional): the prefix to use with `PREFIX/share/jupyter/kernels` (defaults is [sys.prefix][]).
- folder (optional) the full path to the `kernels` folder.

### Examples

```bash
# Install for a user on linux
async-kernel -a async --path="~/.local/share/jupyter/kernels"

# Install for a user on Mac
async-kernel -a async --path="~/Library/Jupyter/kernels"

# Install for a user on windows
async-kernel -a async --path="%APPDATA%\jupyter\kernels"
```

## Host loop (gui event loops - tk, qt)

Typically event loops don't like to share the thread with any other event loop.
Trio provides the function [trio.lowlevel.start_guest_run][] which allows it to run
as a guest alongside the host event loop by means of callbacks. The author of
aiologic has provided an (experimental) asyncio equivalent ([gist](https://gist.github.com/x42005e1f/857dcc8b6865a11f1ffc7767bb602779)).

async-kernel supports configuration of one host and one backend for the kernel.

```console
async-kernel -a async-with-host --host=<hostname>
```

## Host options

Options can be provided to configure how a host loads. There are only a few options available
at present.

- `host_class' `[type[Host| str]]` : A customised subclass of a [Host][async_kernel.event_loop.run.Host]
  or a dotted import path to the customised Host.
- `'module': The module name on which to base the event loop. (Only applies to [qt][async_kernel.event_loop.qt_host.QtHost]).

````console

# PySide6 is default.  You can specify a different module via `host_options`
async-kernel -a async-qt --host=qt --host_options module=PySide2


## Backend options

Options can be provided for how the backend is started.

- With a (gui) host: Options for `start_guest_run`
    - [trio.lowlevel.start_guest_run][]
    - asyncio
        - host_uses_signal_set_wakeup_fd
        - host_uses_sys_set_asyncgen_hooks
        - loop_factory,
        - task_factory,
        - context,
        - debug,
- Without a (gui) host: `backend_options` in [anyio.run][]

```console
# If uvloop is installed it will be used by default. You can do this to disable it.
async-kernel -a async --backend_options use_uvloop=False
````

#### Examples

=== "write_kernel_spec argument"

    **start_interface**

    To specify an alternate start_interface.

    ```console
    async-kernel -a my-async-kernel --start_interface=my_module.start_interface
    ```

    **display name**

    To set the kernel display name to `True`.

    ```console
    async-kernel -a my-async-kernel --display_name "My kernel display name"
    ```

## Remove a kernel spec

Use the flag `-r` or `--remove` to remove a kernelspec.

If you added the custom kernel spec above, you can remove it with:

```bash
async-kernel -r async-trio-custom
```

## Start a kernel

Use the flag `-f` or `--connection_file` followed by the full path to the connection file.
To skip providing a connection file

This will start the default kernel (async).

```bash
async-kernel -f .
```

Additional settings can be passed as arguments.

```bash
async-kernel -f . --name=async-trio-custom --display_name='My custom kernel' --quiet=False
```

The call above will start a new kernel with a 'trio' backend. The quiet setting is
a parameter that gets set on kernel. Parameters of this type are converted using [eval]
prior to setting.

For further detail, see the API for the command line handler [command_line][async_kernel.command.command_line].
