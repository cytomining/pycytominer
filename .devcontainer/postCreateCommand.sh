#! /usr/bin/env bash
python -m pip install poetry
poetry config virtualenvs.in-project true --local
poetry install --with cell_locations,collate,dev
poetry run pre-commit install --install-hooks
