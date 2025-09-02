# Contributing

This project is under active development. Feel free to create an issue to provide feedback.

## Development

The development environment is provided by [uv](https://docs.astral.sh/uv/) using [locking and syncing](https://docs.astral.sh/uv/concepts/projects/sync/#locking-and-syncing).

If you are working on a pull request [make a fork](https://github.com/fleming79/async-kernel/fork) of the project and work on the fork.

```bash
git clone <your fork repository>
cd async-kernel
```

Synchronise the environment.

```bash
uv venv --python 3.11 # or whichever environment you are targeting.
uv sync
# Activate the environment
```

Additional steps to build documentation (optional):

```bash
uv sync --group docs
uv run async-kernel -a async-docs --shell.execute_request_timeout=0.1
```

### Running tests

```bash
uv run pytest
```

#### Running tests with coverage

We intend to maintain 100% code coverage on CI (Linux). The [coverage report](https://app.codecov.io/github/fleming79/async-kernel)
and badge [![codecov](https://codecov.io/github/fleming79/async-kernel/graph/badge.svg?token=PX0RWNKT85)](https://codecov.io/github/fleming79/async-kernel)
are generated with [Codecov](https://about.codecov.io/).

You can run tests locally with coverage to see if the test will pass on CI using:

```bash
uv run pytest -vv --cov --cov-fail-under=100
```

??? info

    We are only targeting 100% on linux for >= 3.12 for the following reasons:

    1. linux is the only platform that reliably supports the `transport` type `ipc` for zmq sockets which is supported by async kerenel.
    1. Coverage on Python 3.11 doesn't correctly gather data for subprocesses giving invalid coverage reports.

### Pre-commit

Pre-commit runs a number of checks on the code and will also re-format it.

Pre-commit will run automatically on submission of a PR but you can also run it locally as a tool with:

=== "Changed files"

    ```bash
    uvx pre-commit run
    ```

=== "All files"

    ```bash
    uvx pre-commit run -a
    ```

### Type checking

Type checking is performed separately to pre-commit checks. Currently type checking is done
using [basedpyright](https://docs.basedpyright.com/). Other type checkers might be added
in the future.

```bash
uv run basedpyright
```

### Update packages

To upgrade all packages use the command:

```bash
uv lock --upgrade
```

### Documentation

Documentation is generated from markdown files and the source using [Material for MkDocs ](https://squidfunk.github.io/mkdocs-material/) and
[mike](https://pypi.org/project/mike/) for versioning. Publishing of documentation is handled by the automation workflow 'publish-docs.yml'.

The 'docs' group specified extra packages are required to build documentation.

#### Sync 'docs' group

```bash
uv sync --group docs
uv run async-kernel -a async-docs --shell.execute_request_timeout=0.1
```

#### Test the docs

```bash
uv run mkdocs build -s
```

??? info

    The command:

    ```bash
    uv run async-kernel -a async-docs --shell.execute_request_timeout=0.1
    ```

    defines a new kernel spec with the name "async-docs" that sets the `shell.execute_request_timeout` to 100ms.

    The "async-docs" named kernel spec is used by [mkdocs-jupyter](#notebooks) to convert the notebooks
    for inclusion in the usage section of the documentation.

### Serve locally

```bash
mkdocs serve 
```

### API / Docstrings

API documentation is included using [mkdocstrings](https://mkdocstrings.github.io/).

Docstrings are written in docstring format [google-notypes](https://mkdocstrings.github.io/griffe/reference/docstrings/?h=google#google-style).
Typing information is included automatically by [griff](https://mkdocstrings.github.io/griffe).

#### See also

- [cross-referencing](https://mkdocstrings.github.io/usage/#cross-references)

### Notebooks

Notebooks are included in the documentation by the plugin [mkdocs-jupyter](https://github.com/danielfrg/mkdocs-jupyter).

!!! info

    We use the kernel spec named 'async-docs' which has a cell execute timeout of 100ms. This is used
    to advance execution through long running cells.

    The [suppress-error][async_kernel.typing.Tags.suppress_error] tag is inserted in code cells to enable
    with generating documentation. The symbol '⚠' is an indicator that the error was suppressed. Normally
    this is due to the timeout but there is no distinction on the type of error.

#### Useful links

These links are not relevant for docstrings.

- [footnotes](https://squidfunk.github.io/mkdocs-material/reference/footnotes/#usage)
- [tooltips](https://squidfunk.github.io/mkdocs-material/reference/tooltips/#usage)

## Releasing Async kernel

To make a new release go to the [new_release.yml](https://github.com/fleming79/async-kernel/actions/workflows/new_release.yml) action
and click 'Run workflow'.

### new_release.yml

The workflow does the following:

1. Creates and merges a PR with the updated changelog generated with [git-cliff](https://git-cliff.org/).
1. Starts a new Github release which adds a tag 'v<release version>' to the head of the main branch.

### publish-to-pypi.yml

The publish-to-pypi[^test-pypi] workflow will start automatically on completion of the "new_release.yml".
It performs the following steps.

1. Builds the distribution.
1. Waits for manual approval to release.
1. Uploads the release files to [PyPi](https://pypi.org/project/async-kernel/).
1. Uploads the release files to the [Github release](https://github.com/fleming79/async-kernel/releases).

Once the new PR is available merge the PR into the main branch.
Normally this will also trigger publication of the new release.

### Publish

[publish-to-pypi.yml](https://github.com/fleming79/async-kernel/actions/workflows/publish-to-pypi.yml) is
the workflow that publishes the release. It starts on a push to the main branch but can also be manually triggered.
It will always publish to TestPyPI on a push. If the git head has a tag starting with 'v' it will also publish
to PyPi. If it is published to PyPI successfully, it will also create a Github release.

### Run ci checks locally

You can run tests locally to see if there is anything that might be caught by CI.

```bash
uvx pre-commit run -a
uv run pytest -vv --cov --cov-fail-under=100
uv run basedpyright
uv run mkdocs build -s
```

!!! note

    CI checks also run for a matrix of OS's and python versions. So even if all tests pass locally,
    tests can still fail for another os or python version.

[^test-pypi]: This workflow also runs on push to the main branch, but will instead publish to [TestPyPI](https://test.pypi.org/project/async-kernel/).
