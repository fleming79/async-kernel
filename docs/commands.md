# Command line

`async-kernel` (and alias `async_kernel`) is provided on the command line. The main options are:

- [Add kernel spec](#add-a-kernel-spec)
- [Remove a kernel spec](#remove-a-kernel-spec)
- [Start a kernel](#start-a-kernel)

## Add a kernel spec

Use the argument `-a` followed by the kernel name to add a new kernel spec.
Include 'trio' in the kernel name to use a 'trio' backend. Any valid kernel name is
allowed (whitespace is not allowed).

Recommended kernel names are:

- 'async': Default kernel that is installed that provides a the default 'asyncio' backend.
- 'async-trio': A trio backend. Note: trio must be installed separately.

Add a trio kernel spec.

```console
async-kernel -a async-trio
```

### Custom arguments

Additional arguments can be included when defining the kernel spec, these include:

- Arguments for [async_kernel.kernelspec.write_kernel_spec][]
    - `--kernel_factory`
    - `--fullpath=False`
    - `--display_name`
    - `--prefix`
- Nested attributes on the kernel via \`kernel.\<nested.attribute.name>'

Each parameter should be specified as if it were a 'flag' as follows.

Prefix with "--" and join with the delimiter "=".

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

    **kernel_factory**

    To specify an alternate kernel factory.

    ```console
    --kernel_factory=my_module.my_kernel_factory
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
    --shell.execute_request_timeout=0.1
    ```

=== "Kernel Boolean attribute as a flag"

    Set `kernel.quiet=True`:

    ```console
    --quiet 
    ```

    Set `kernel.quiet=False`:

    ```bash
    --no=quiet 
    ```

## Remove a kernel spec

Use the flag `-r` or `--remove` to remove a kernelspec.

```bash
async-kernel
```

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
