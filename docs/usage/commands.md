# Command line and kernel configuration

The command `async-kernel` and alias `async_kernel` are provided at the command line.

The primary options available are:

- [Add kernel spec](#kernel-spec): `-a <name> <options>`
- [Remove a kernel spec](#remove-a-kernel-spec) `-r <name>`
- [Start a kernel](#start-a-kernel): `-f <path to config file>`

## Kernel spec

The kernel spec can be configured via the command line:

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
        "--start_interface=async_kernel.interface.start_kernel_zmq_interface",
        "--kernel_name=async",
        "--backend=trio",
        "--loop=tk"
    ],
    "env": {},
    "display_name": "My tk trio kernel",
    "language": "python",
    "interrupt_mode": "message",
    "metadata": {
        "debugger": true,
        "concurrent": true
    },
    "kernel_protocol_version": "5.5"
}
```

A single kernel spec is created when async kernel is installed with the following defaults.

**Defaults**

- name: 'async'
- display_name: 'async'
- backend: `asyncio`
- backend_options: `{'use_uvloop':True}` if uvloop or winloop is installed
- loop: `None`
- loop_options: `None`

The kernel spec can be updated by adding a kernel spec with the same name ('async').

## Backend

The name _backend_ is adopted from anyio which applies to the asynchronous library.
For async kernel this applies to the asynchronous library used shell and control channels.
async kernel facilitates this using the [caller][async_kernel.caller.Caller] class with
one caller per channel.

In CPython the [backend][async_kernel.typing.Backend] is started using [anyio.run][].
The backend can be specified via the zmq [`interface.backend`][async_kernel.interface.BaseKernelInterface.backend] and backend_options can be specified on [`interface.backend_options'][async_kernel.interface.zmq.ZMQKernelInterface.backend_options].
Options can be written as a literal python string.

```console
# options are 'asycio', 'trio'

--interface.backend=trio

# asyncio & trio event loops are started using anyio. 'backend_options'


# 'use_uv' is set by default.

# To disable uv
"--interface.loop_options={'use_uv':False}"
```

## Host loop (gui event loops - tk, qt)

Generally event loops don't like to share the thread with other event loops.
Trio provides the function [trio.lowlevel.start_guest_run][] to run the event loop
as a guest of the host event loop by means of callbacks.

Ilya Egorov recently wrote an experimental
asyncio version ([gist](https://gist.github.com/x42005e1f/857dcc8b6865a11f1ffc7767bb602779)).

This means we can have a gui event loop and and asynchronous event loop running in the same thread. This is especially useful when the gui isn't thread safe.

Here are some example kernel specs for host and backend

### tk

```console
# tk host asyncio backend
async-kernel -a async-tk --interface.loop=tk

# tk host trio backend
async-kernel -a async-tk --interface.loop=tk --backend=trio
```

### qt

```console
# qt host asyncio backend
async-kernel -a async-qt --interface.loop=qt

# qt host trio backend
async-kernel -a async-qt-trio --interface.loop=qt --interface.backend=trio

# PySide6 is default.  You can specify a different module via `loop_options`
async-kernel -a async-qt --interface.loop=qt --interface.loop_options={'module':'PySide2'}
```

## Backend options

Options can be provided for how the backend is started.

- With loop: Options for `start_guest_run`
    - [trio.lowlevel.start_guest_run][]
    - asyncio
        - host_uses_signal_set_wakeup_fd
        - loop_factory,
        - task_factory,
        - context,
        - debug,
- Without loop: `backend_options` in [anyio.run][]

### Custom arguments

Additional arguments can be included when defining the kernel spec, these include:

- Arguments for [async_kernel.kernelspec.write_kernel_spec][]
    - `--start_interface`
    - `--fullpath=False`
    - `--display_name`
    - `--prefix`
- Nested attributes can be set on the kernel via `kernel.[nested.attribute.name']'.
  Each parameter should be specified as if it were a 'flag' as follows.

Prefix each setting with "--" and join using the delimiter "=".

```console
--<PARAMETER or DOTTED.ATTRIBUTE.NAME>=<VALUE>
```

or, with compact notation to set a Boolean value as a Boolean flag.

```console
# True
--<PARAMETER or DOTTED.ATTRIBUTE.NAME>

# False
--no-<PARAMETER or DOTTED.ATTRIBUTE.NAME>
```

#### Examples

=== "write_kernel_spec argument"

    **start_interface**

    To specify an alternate kernel factory.

    ```console
    --start_interface=my_module.my_interface_factory
    ```

    **fullpath (True)**

    ```console
    --fullpath
    ```

    **display name**

    To set the kernel display name to `True`.

    ```console
    "--display_name=My kernel display name"
    ```

=== "Kernel attribute"

    Set the execute request timeout trait on the kernel shell.

    ```console
    --shell.timeout=0.1
    ```

=== "Kernel Boolean attribute as a flag"

    Set `kernel.quiet=True`:

    ```console
    --quiet
    ```

    Set `kernel.quiet=False`:

    ```console
    --no=quiet
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
async-kernel -f . --kernel_name=async-trio-custom --display_name='My custom kernel' --quiet=False
```

The call above will start a new kernel with a 'trio' backend. The quiet setting is
a parameter that gets set on kernel. Parameters of this type are converted using [eval]
prior to setting.

For further detail, see the API for the command line handler [command_line][async_kernel.command.command_line].
