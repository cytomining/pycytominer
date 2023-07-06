#! /usr/bin/env bash
python -m pip install poetry
poetry config virtualenvs.in-project true --local
poetry install --all-extras
poetry run pre-commit install --install-hooks
