FROM python:3.11

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

# copy pyproject and poetry lockfile for stepped installation
COPY pyproject.toml poetry.lock ./

# install poetry and poetry dynamic versioning
# hadolint ignore=DL3013
RUN pip install --no-cache-dir poetry poetry-dynamic-versioning

# copy the rest of the repository
COPY . .

# install pycytominer from poetry with group deps and all extras
RUN poetry install --with dev --all-extras -v --no-interaction

# set an alias for running python through the poetry env of pycytominer
# hadolint ignore=DL3059
RUN echo 'alias python="poetry run python"' >> ~/.bashrc
