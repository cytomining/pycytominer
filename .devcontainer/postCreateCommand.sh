#! /usr/bin/env bash
python -m pip install uv
uv sync --all-extras --group dev --group docs
uv run pre-commit install --install-hooks
