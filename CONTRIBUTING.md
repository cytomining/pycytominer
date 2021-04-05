# Contributing to pycytominer

First of all, thank you for contributing to pycytominer! :tada: :100:

This document contains guidelines on how to most effectively contribute to the pycytominer codebase.

If you are stuck, please feel free to ask any questions or ask for help.

## Table Of contents

[Code of conduct](#code-of-conduct)

[Quick links](#quick-links)

[How can I contribute?](#how-can-i-contribute)
  * [Bug reporting](#bug-reporting)
  * [Suggesting enhancements](#suggesting-enhancements)
  * [Your first code contribution](#your-first-code-contribution)
  * [Pull requests](#pull-requests)

[Style guides](#style-guides)
  * [Git commit messages](#git-commit-messages)
  * [Python style guide](#python-style-guide)
  * [Documentation style guide](#documentation-style-guide)

## Code of conduct

This project and everyone participating in it is governed by our [code of conduct](CODE_OF_CONDUCT.md).
By participating, you are expected to uphold this code.
Please report unacceptable behavior to cytodata.info@gmail.com.

## Quick links

* Documentation: https://pycytominer.readthedocs.io/en/latest/
* Issue tracker: https://github.com/cytomining/pycytominer/issues
* Code coverage: https://app.codecov.io/gh/cytomining/pycytominer
* Package requirements: https://github.com/cytomining/pycytominer/blob/master/requirements.txt

## How can I contribute?

### Bug reporting

We love hearing about use-cases when our software does not work.
This provides us an opportunity to improve.
However, in order for us to fix a bug, you need to tell us exactly what went wrong.

When you report a bug, please be prepared to tell us as much pertinent information as possible.
This information includes:

* The pycytominer version you’re using
* The format of input data
* Copy and paste two pieces of information: 1) your command and 2) the specific error message
* What you’ve tried to overcome the bug

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

The content and description of your pull request are directly related to the speed at which we are able to review, approve, and merge your contribution into pycytominer.
To ensure an efficient review process please perform the following steps:

1. Follow all instructions in the [pull request template](.github/PULL_REQUEST_TEMPLATE.md)
2. Triple check that your pull request is only adding _one_ specific feature. Small, bite-sized pull requests move so much faster than large pull requests.
3. After submitting your pull request, ensure that your contribution passes all status checks (e.g. passes all tests)

## Style guides

Please follow all style guides to the best of your abilities.

### Git commit messages

For all commit messages, please use a short phrase that describes the specific change.
For example, “Add feature to check normalization method string” is much preferred to “change code”.
When appropriate, reference issues (via `#` plus number) .

### Python style guide

For python code style, we use [black](https://github.com/psf/black).
Please use black before committing any code.
We will not accept code contributions that do not use black.

### Documentation style guide

We use the [numpy documentation style guide](https://numpydoc.readthedocs.io/en/latest/format.html).
