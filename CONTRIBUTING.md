# Contributing

This project is under active development. Feel free to create an issue to provide feedback.

## Development

Development is done using [uv](https://docs.astral.sh/uv/) to provide the virtual environment.

### Installation from source

```shell
git clone https://github.com/fleming79/async-kernel.git
cd async-kernel
uv venv -p python@311 # or whichever environment you are targeting.
uv sync
# Activate the environment
```

### Update packages

```shell
uv lock --upgrade
```

### Running tests

```shell
uv run pytest
```

### Running tests with coverage

We are aiming for 100% code coverage on CI (Linux). Any new code should also update tests to maintain coverage.

```shell
uv run pytest -vv --cov
```

!!! note

    We are only targeting 100% on linux for >= 3.12 for the following reasons:

    1. `transport` type `ipc` is only supported linux which has special handling.
    1. Coverage on Python 3.11 doesn't correctly gather data for subprocesses giving invalid coverage reports.

### Code Styling

`Async kernel` uses ruff for code formatting. The pre-commit hook should take care of how it should look.

To install `pre-commit`, run the following:

```shell
pre-commit install
```

You can invoke the pre-commit hook by hand at any time with:

```shell
pre-commit run
```

### Type checking

Type checking is performed using [basedpyright](https://docs.basedpyright.com/).

```shell
basedpyright
```

### Documentation

Documentation is provided my [Material for MkDocs ](https://squidfunk.github.io/mkdocs-material/). To start up a server for editing locally:

#### Install

```shell
uv sync --group docs
uv run async-kernel -a async-docs --shell.execute_request_timeout 0.1
```

### Serve locally

```shell
mkdocs serve 
```

### API / Docstrings

API documentation is included using [mkdocstrings](https://mkdocstrings.github.io/).

Docstrings are written in docstring format [google-notypes](https://mkdocstrings.github.io/griffe/reference/docstrings/?h=google#google-style).
Typing information is included automatically by [griff](https://mkdocstrings.github.io/griffe).

#### See also

- [cross-referencing](https://mkdocstrings.github.io/usage/#cross-references)

### Notebooks

Notebooks are included in the documentation with the plugin [mkdocs-jupyter](https://github.com/danielfrg/mkdocs-jupyter).

#### Useful links

These links are not relevant for docstrings.

- [footnotes](https://squidfunk.github.io/mkdocs-material/reference/footnotes/#usage)
- [tooltips](https://squidfunk.github.io/mkdocs-material/reference/tooltips/#usage)

### Deploy manually

```shell
mkdocs gh-deploy --force
```

## Releasing Async kernel

Releasing is performed using the Github action [new_release.yml](https://github.com/fleming79/async-kernel/actions/workflows/new_release.yml).
action creates a new tagged pull request that is assigned to the user who started the workflow.
The PR is contains the revised changelog and latest change info, both generated using [git-cliff](https://git-cliff.org/).

### Publish

The workflow that publishes the release is [publish-to-pypi.yml](https://github.com/fleming79/async-kernel/actions/workflows/publish-to-pypi.yml).
starts on push to the main branch but can also be manually started. If the git head is tagged it will publish to PyPI.
If the `github.event_name` is push it will publish to TestPyPI.

The changelog is inserted in the tag which is used to generate the Github release notes.
A corresponding Github release is also created. Manually update the release page as required.

#### Manual

To manually publish create a tag on the head of the main branch and push the tags.

```
git checkout
git tag v0.1.0 -m "v0.1.0"
git push --tags
```

If the publish workflow doesn't start automatically. Run the workflow manually.

!!! note

    Where possible use the workflows to publish releases.
