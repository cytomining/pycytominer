FROM python:3.11 as base

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

# configures poetry to install dependencies without virtualenvs
# see: https://python-poetry.org/docs/configuration/#virtualenvscreate
ENV POETRY_VIRTUALENVS_CREATE=false

# install poetry and poetry dynamic versioning
# hadolint ignore=DL3013
RUN pip install --no-cache-dir poetry

# create stage for production build (no dev or non-essential deps)
FROM base as production

# copy the pycytominer source into image
COPY README.md pyproject.toml poetry.lock ./
COPY pycytominer ./pycytominer

# install pycytominer using poetry root dependencies and all extras
RUN poetry install --all-extras -v --no-interaction

# create stage for production build (no dev or non-essential deps)
FROM production as testing

# copy the tests dir for testing purposes
COPY tests ./tests

# install pycytominer from poetry with group deps and all extras for testing
RUN poetry install --with dev --all-extras -v --no-interaction
