#! /usr/bin/env bash
# Specify current directory as safe to avoid the error:
#  Detected Git repository, but failed because of dubious ownership
# See https://sam.hooke.me/note/2023/08/poetry-fixing-dubious-ownership-error/
git config --global --add safe.directory $(pwd)
# PDV will already have been installed in a devcontainer, but is 
# re-included here for the benefit of manual dev environments.
poetry self add "poetry-dynamic-versioning[plugin]"
poetry config virtualenvs.in-project true --local
poetry install --with dev --all-extras
poetry run pre-commit install --install-hooks
