FROM python:3.11 AS base

# set various metadata, loosely following biocontainers standards from:
# https://biocontainers-edu.readthedocs.io/en/latest/what_is_biocontainers.html
LABEL base_image="python:3.11"
LABEL software="pycytominer"
LABEL about.summary="Python package for processing image-based profiling data"
LABEL about.home="https://github.com/cytomining/pycytominer"
LABEL about.documentation="https://pycytominer.readthedocs.io/en/stable/"
LABEL about.license_file="https://github.com/cytomining/pycytominer/blob/main/LICENSE.md"
LABEL about.license="SPDX:BSD-3-Clause"

# set the workdir to /app
WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

# install uv
# hadolint ignore=DL3013
RUN pip install --no-cache-dir uv

# create stage for production build (no dev or non-essential deps)
FROM base AS production

# copy project metadata and built artifacts into image
COPY pyproject.toml uv.lock ./
COPY dist ./dist

# install pycytominer dependencies from the lockfile, then install the built wheel
RUN uv sync --frozen --all-extras --no-dev --no-install-project && \
    uv pip install --python .venv/bin/python dist/*.whl

# create stage for production build (no dev or non-essential deps)
FROM production AS testing

# copy the source tree for tests that read package data via repository paths
COPY pycytominer ./pycytominer

# copy the tests dir for testing purposes
COPY tests ./tests

# install dev dependencies without removing the wheel-installed pycytominer
RUN uv sync --frozen --all-extras --group dev --no-install-project --inexact
