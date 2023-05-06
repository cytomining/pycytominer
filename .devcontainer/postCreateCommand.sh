#! /usr/bin/env bash

python -m pip install -r requirements-dev.txt
python -m pip install -e .
pre-commit install --install-hooks
