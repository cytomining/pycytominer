# Contributing to pycytominer

First of all, thank you for contributing to pycytominer! :tada: :100:

This document contains guidelines on how to most effectively contribute to the pycytominer codebase.

If you are stuck, please feel free to ask any questions or ask for help.

## Table of contents

[Code of conduct](#code-of-conduct)

[Quick links](#quick-links)

[How can I contribute?](#how-can-i-contribute)

- [Bug reporting](#bug-reporting)
- [Suggesting enhancements](#suggesting-enhancements)
- [Your first code contribution](#your-first-code-contribution)
- [Pull requests](#pull-requests)
- [Documentation](#documentation)
- [Poetry](#poetry)
- [Dev environments](#dev-environments)
- [Releases](#releases)

[Style guides](#style-guides)

- [Git commit messages](#git-commit-messages)
- [Python style guide](#python-style-guide)
- [Documentation style guide](#documentation-style-guide)

## Code of conduct

This project and everyone participating in it is governed by our [code of conduct](CODE_OF_CONDUCT.md).
By participating, you are expected to uphold this code.
Please report unacceptable behavior to cytodata.info@gmail.com.

## Quick links

- Documentation: https://pycytominer.readthedocs.io/en/latest/
- Issue tracker: https://github.com/cytomining/pycytominer/issues
- Code coverage: https://app.codecov.io/gh/cytomining/pycytominer
- Package requirements (using Poetry): https://github.com/cytomining/pycytominer/blob/main/pyproject.toml

## How can I contribute?

### Bug reporting

We love hearing about use-cases when our software does not work.
This provides us an opportunity to improve.
However, in order for us to fix a bug, you need to tell us exactly what went wrong.

When you report a bug, please be prepared to tell us as much pertinent information as possible.
This information includes:

- The pycytominer version you’re using
- The format of input data
- Copy and paste two pieces of information: 1) your command and 2) the specific error message
- What you’ve tried to overcome the bug

Please provide this information as an issue in the repository: https://github.com/cytomining/pycytominer/issues

Please also search the issues (and documentation) for an existing solution.
It’s possible we solved your bug already!
If you find an issue already describing your bug, please add a comment to the issue instead of opening a new one.

### Suggesting enhancements

We’re deeply committed to a simple, intuitive user experience, and to support core profiling pipeline data processing.
This commitment requires a good relationship, and open communication, with our users.

We encourage you to propose enhancements to improve the pycytominer package.

First, figure out if your proposal is already implemented, by reading the documentation!
Next, check the issues (https://github.com/cytomining/pycytominer/issues) to see if someone else has already proposed the enhancement you have in mind.
If you do find the suggestion, please comment on the existing issue noting that you are also interested in this functionality.
If you do not find the suggestion, please open a new issue and clearly document the specific enhancement and why it would be helpful for your particular use case.

Please provide your enhancement suggestions as an issue in the repository:

### Your first code contribution

Contributing code for the first time can be a daunting task.
However, in our community, we strive to be as welcoming as possible to newcomers, while ensuring rigorous software development practices.

The first thing to figure out is exactly what you’re going to contribute!
We have specifically tagged [beginner issues](https://github.com/cytomining/pycytominer/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22), but we describe all future work as individual [github issues](https://github.com/cytomining/pycytominer/issues).

If you want to contribute code that we haven’t already outlined, please start a discussion in a new issue before actually writing any code.
A discussion will clarify the new code and reduce merge time.
Plus, it’s possible that your contribution belongs in a different code base, and we do not want to waste your time (or ours)!

### Pull requests

After you’ve decided to contribute code and have written it up, now it is time to file a pull request.
We specifically follow a [forked pull request model](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork).
Please create a fork of the pycytominer repository, clone the fork, and then create a new, feature-specific branch.
Once you make the necessary changes on this branch, you should file a pull request to incorporate your changes into the main pycytominer repository.

The content and description of your pull request are directly related to the speed at which we are able to review, approve, and merge your contribution into pycytominer.
To ensure an efficient review process please perform the following steps:

1. Follow all instructions in the [pull request template](.github/PULL_REQUEST_TEMPLATE.md)
2. Triple check that your pull request is only adding _one_ specific feature. Small, bite-sized pull requests move so much faster than large pull requests.
3. After submitting your pull request, ensure that your contribution passes all status checks (e.g. passes all tests)

All pull requests must be reviewed and approved by at least one project maintainer in order to be merged.
We will do our best to review the code addition in a timely fashion.
Ensuring that you follow all steps above will increase our speed and ability to review.
We will check for accuracy, style, code coverage, and scope.

### Documentation

We use [sphinx](https://www.sphinx-doc.org/en/master/index.html) for autodocumentation of docstrings, using the [napoleon extenstion](https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html) to parse [NumPy style docstrings](https://numpydoc.readthedocs.io/en/latest/format.html), implemented with a [furo](https://pradyunsg.me/furo/) theme.
We host our documentation on [readthedocs.org](https://readthedocs.org/) at [https://pycytominer.readthedocs.io/en/latest/](https://pycytominer.readthedocs.io/en/latest/).

To build and test changes to the docs locally, run the following command:

```bash
sphinx-build -b html docs build
```

See [`docs/conf.py`](docs/conf.py) for full documentation configuration.

### Poetry

We use [Poetry](https://python-poetry.org/) to manage dependencies and packaging.
Changes in dependencies are managed by Poetry's `pyproject.toml` file.
Poetry installs all dependencies in a virtual environment, which is activated automatically when you run `poetry shell`.
Poetry also provides a `poetry run` command to run commands in the virtual environment without activating it.
For example, to run the test suite, you can use `poetry run pytest`.

### Dev environments

#### Local devcontainer

Instructions for setting up a local development environment using VSCode DevContainers:

1. Install [VSCode](https://code.visualstudio.com/download)
2. Install the [Remote - Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension
3. Open the repository in VSCode
4. Click on the green "Reopen in Container" button in the lower left corner of the window
5. Wait for the container to build and install the required dependencies

#### Cloud environment

We've set up cloud development configurations with [Github Codespaces](https://github.com/codespaces).
These development environments include the project dependencies pre-installed via [Poetry](https://python-poetry.org/).
Prior to commit, pre-installed git hooks auto-format any changed code.
When you are ready to make a pull request, use the pre-configured test suite in VSCode or run `poetry run pytest` to ensure that your changes pass all tests.
You can create a codespace by clicking on the following link:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new/?repo=195106954&ref=main&devcontainer_path=.devcontainer%2Fdevcontainer.json)

[Beginner's Guide to Codespaces](https://github.blog/2023-02-22-a-beginners-guide-to-learning-to-code-with-github-codespaces/)

#### Manual setup

We recommend using either the local devcontainer or cloud dev environment approaches above.
However, we also provide general guidance for setting up a dev environment in Linux, MacOS, or Windows (WSL) below.

```bash
# Install Poetry (Linux, MacOS, Windows - WSL)
curl -sSL https://install.python-poetry.org | python3 -
# Checkout the repository
git clone https://github.com/cytomining/pycytominer.git
cd pycytominer
# Install pycytominer, dev dependencies, and pre-commit hooks
bash .devcontainer/postCreateCommand.sh
```

### Releases

Project maintainers are responsible for releasing new versions of pycytominer.
Creating a new release includes the following steps:

1. Create a new branch from `main` for the release (e.g. `release-v1.0.0`)
2. Review the [commit history](https://github.com/cytomining/pycytominer/compare) from the last release and check whether it includes commits that don't follow the [conventional commit standard](https://www.conventionalcommits.org/en/v1.0.0/#summary).
   If all changes follow conventional commits, skip to step 5.
3. Run the command `cz bump --files-only` to update the version number in `CITATION.cff` and `pyproject.toml:tool.commitizen` and generate the draft changelog.
4. Review the changes to `CHANGELOG.md`. If necessary, add descriptions of missing changes and modify descriptions to match conventional commits standard.
5. `git add` any manual changes and run `cz bump` to create the release commit.
   Push the changes to the release branch.
6. Create a pull request for the release branch into `main`.
7. Request a review from another maintainer.
8. Once the pull request is approved, merge it into `main`.
9. Create a new release on GitHub using the [release draft](https://github.com/cytomining/pycytominer/releases/new) feature.
10. Publish the release.
11. The release will be automatically published to [PyPI](https://pypi.org/project/pycytominer/) via Github Actions.
12. Manually create the release at [conda-forge](https://anaconda.org/conda-forge/pycytominer).

## Style guides

Please follow all style guides to the best of your abilities.

### Git commit messages

Pycytominer uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) standard for commit messages to aid in automatic changelog generation.
We prepare commit messages that follow this standard using [commitizen](https://commitizen-tools.github.io/commitizen/), which comes with the poetry dev dependencies.

### Python style guide

For python code style, we use [black](https://github.com/psf/black).
Please use black before committing any code.
We will not accept code contributions that do not use black.
Configuring your [dev environment](#dev-environments) as described above will ensure your code is formatted correctly automatically (using a tool called [pre-commit](https://pre-commit.com/)).

### Documentation style guide

We use the [numpy documentation style guide](https://numpydoc.readthedocs.io/en/latest/format.html).
We also use [prettier](https://prettier.io/) for automatic formatting of markdown, json and yaml files.
