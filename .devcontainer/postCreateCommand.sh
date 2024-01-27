#! /usr/bin/env bash
# PDV will already have been installed in a devcontainer, but is 
# re-included here for the benefit of manual dev environments.
poetry self add "poetry-dynamic-versioning[plugin]"
poetry config virtualenvs.in-project true --local
poetry install --with dev --all-extras
poetry run pre-commit install --install-hooks
