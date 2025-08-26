# Contributing

This project is under active development. Feel free to create an issue to provide feedback.

## Development

[uv](https://docs.astral.sh/uv/) is used for development. By default it will install all
relevant versions of the required packages when it is synchronised.

### Installation from source

If you are working on a pull request, [make a fork] of the project and install from your fork.

```shell
git clone <repository>
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

![Coverage grid](https://codecov.io/github/fleming79/async-kernel/graphs/tree.svg?token=PX0RWNKT85)

### Code Styling

`Async kernel` uses ruff for code formatting. The pre-commit hook should take care of how it should look.

To install `pre-commit` to run prior commits with the following:

```shell
pre-commit install
```

If you prefer not to install the hook, you can invoke the pre-commit hook by hand at any time with:

```shell
pre-commit run # append -a to run against all files.
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

To start a new release manually trigger the Github action [new_release.yml](https://github.com/fleming79/async-kernel/actions/workflows/new_release.yml).

The action does the following:

1. Creates a new branch using the version number.
1. Updates the changelog for the new version using [git-cliff](https://git-cliff.org/).
1. Commits the revised changelog.
1. Adds a tag against the commit with the version.
1. Starts a new PR assigning the actor who triggered the workflow.

Once the new PR is available merge the PR into the main branch.
Normally this will also trigger publication of the new release.

### Publish

[publish-to-pypi.yml](https://github.com/fleming79/async-kernel/actions/workflows/publish-to-pypi.yml) is
the workflow that publishes the release. It starts on a push to the main branch but can also be manually triggered.
It will always publish to TestPyPI on a push. If the git head has a tag starting with 'v' it will also publish
to PyPi. If it is published to PyPI successfully, it will also create a Github release.

#### Manual

To manually publish create a tag on the head of the main branch and push the tags.

```
git checkout
git tag v0.1.0 -m "v0.1.0"
git push --tags
```

If the publish workflow doesn't start automatically. Run the [publish-to-pypi](https://github.com/fleming79/async-kernel/actions/workflows/publish-to-pypi.yml)
workflow manually.

!!! note

    Where possible use the workflows to publish releases.
