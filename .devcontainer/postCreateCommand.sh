#! /usr/bin/env bash
poetry config virtualenvs.in-project true --local
poetry install --with cell_locations,collate,dev
poetry run pre-commit install --install-hooks
