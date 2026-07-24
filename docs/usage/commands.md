# Command line

Once async-kernel is installed, you can use the command `async-kernel` at the command line.

## Sub-commands

- [`install`](#install): install a kernelspec.
- [`uninstall`](#uninstall): uninstall a kernelspec.
- [`start`](#start-a-kernel): start a kernel.
- `-l` `--list`: list installed kernelspecs.
- `--help-all`: Display all config options available when the kernel.
- `--show-config`: Print all customized options.
- `--show-config-json`: Print all customized options in json format.
- `-h`: Display short help.

## Options

Configuration is done using [Traitlets configuration](https://traitlets.readthedocs.io/en/stable/config.html#module-traitlets.config).
Many configuration options are available, use the command `'--help-all'` to see a list options.

### Main options

- `--name`: The name of a kernel (`default="async"`).
- `--display-name` A user friendly name to identify the kernel (`default=f"Python {sys.version.split()[0]} ({name})"`).
- [`host`](#host-options) `[None, "tk", "qt"]`: The name of a gui event loop to use (`default=None`).
- [`backend`](#backend-interface-backend) `["asyncio", "trio"]`: The backend to use (`default="asyncio"`).

## [Kernelspec](https://jupyter-client.readthedocs.io/en/stable/kernels.html#kernel-specs)

### Install

```bash
# default
async-kernel install

# To install the user environment
async-kernel install --user
```

#### Kernel customisation

The kernel can be customised/configured by passing additional command line options. Here are a few examples:

```bash
# trio backend
async-kernel install --name=async-trio --backend=trio

# tk host (gui event loop)
async-kernel install --name=async-tk --host=<host name>
```

#### Backend (`interface-backend`)

There are two supported backends 'asyncio' and 'trio'.

In CPython the [backend][async_kernel.typing.Backend] is started using [anyio.run][].
The type of backend can be specified at the attribute [interface.backend][async_kernel.interface.base.BaseMessageApplication.backend].
The backend_options can be specified at the attribute [interface.backend_options][async_kernel.interface.base.BaseMessageApplication.backend_options].
Options can be written as a literal python string.

```console
async-kernel install --name=async-trio --backend=trio
```

#### Kernel spec location

See [here](https://jupyter-client.readthedocs.io/en/latest/kernels.html#kernel-specs) for a list of locations where Jupyter/IPython
searches for the kernel specs.

The path where the kernel spec is installed/deleted can also be specified by either `prefix` or `folder`.

**Options**

- 'user' (optional): Use the user directory.
- 'prefix' (optional): the prefix to use with `PREFIX/share/jupyter/kernels` (defaults is [sys.prefix][]).
- 'folder' (optional) the full path to the `kernels` folder.

#### Examples

```bash
# Install for a user
async-kernel install --name=async --user
```

#### Host loop (gui event loops - tk, qt)

Typically event loops don't like to share the thread with any other event loop.
Trio provides the function [trio.lowlevel.start_guest_run][] which allows it to run
as a guest alongside the host event loop by means of callbacks. The author of
aiologic has provided an (experimental) asyncio equivalent ([gist](https://gist.github.com/x42005e1f/857dcc8b6865a11f1ffc7767bb602779)).

async-kernel supports configuration of one host and one backend for the kernel.

```console
async-kernel install --name=async-with-host --host=<hostname>
```

##### Host options

Options can be provided to configure how a host loads. There are only a few options available
at present.

- `host_class' `[type[Host| str]]` : A customised subclass of a [Host][async_kernel.event_loop.run.Host]
  or a dotted import path to the customised Host.
- `'module': The module name on which to base the event loop. (Only applies to [qt][async_kernel.event_loop.qt_host.QtHost]).

````console

# PySide6 is default.  You can specify a different module via `host_options`
async-kernel install --name=async-qt --host=qt --host_options module=PySide2


### Backend options

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
async-kernel install --name=async --backend_options use_uvloop=False
````

### Uninstall

Uninstall is essentially the reverse of install but without configuration options.

```bash
# default
async-kernel install

# Install the user environment
async-kernel install -user

# The main options are
async-kernel install --name=<kernel name> --host=<host name> --backend=<backend name>
```

### Start a kernel

Use the command `async-kernel start <options ...>`. All configuration options accepted when starting a kernel.

Note: Kernel spec files are not used when starting a kernel.
