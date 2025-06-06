## v1.2.3 (2025-05-08)

[Detailed release notes](https://github.com/cytomining/pycytominer/releases/tag/v1.2.3)

### Fix

- **cell_locations**: modify tests for data updates (#505)
- **tests**: Organize DeepProfiler test data in one spot (#520)
- **docs**: Update docs configuration for virtualenv and poetry bug (#523)

### Docs

- **citations** update doi for nature methods pub (#514)
- **citations** Remove citation info (keep just pointer) in readme (#515)

### Refactor

- **poetry**: add plugin configuration to pyproject (#509)

### Build

- **python**: Enable Python 3.13 compatibility (#526)
- **deps**: various dependency updates automated by dependabot

### CI

- **runners**: bump macos gh actions runner version (#506)
- **pre-commit**: add pre-commit-update and related updates (#507)
- **cache**: use os with arch for cache naming (#508)

## v1.2.2 (2025-01-23)

[Detailed release notes](https://github.com/cytomining/pycytominer/releases/tag/v1.2.2)

### Fix

- **feature_select**: Fix Bug in `sample` Parameter in `feature_select()` (#495)
- **cell_locations**: Check if downloaded file is empty (#385)

### Build

- **deps**: various dependency updates automated by dependabot

### Docs

- **maintainer**: add maintainer governance docs (#487)

### CI

- **pre-commit**: add pre-commit lite for auto fixes (#488)
- **python**: bump target python for ci workflows (#486)

## v1.2.1 (2024-11-20)

[Detailed release notes](https://github.com/cytomining/pycytominer/releases/tag/v1.2.1)

### Fix

- **citation**: fix citation.cff file formatting (#460)

### Refactor

- **collate**: deprecationwarning for `collate` (#462)

### Build

- **python**: add python 3.12 compatibility (#475)
- **python**: bump minimum python version to 3.9 (#464)

### CI

- **pre-commit**: format pyproject.toml with pyproject-fmt (#453)
- **releases**: constrain PyPI GitHub Actions release trigger types (#458)
- **dependencies**: update setup python action dependencies and dependabot settings (#457)
- **dependabot**: enable auto poetry updates (#463)

## v1.2.0 (2024-09-30)

[Detailed release notes](https://github.com/cytomining/pycytominer/releases/tag/v1.2.0)

### Feat

- **dev**: add improved makefile with additional helper commands (#391)

### Fix

- **pandas**: add condition for pandas config (#415)
- **poetry**: add tool.setuptools_scm section (#402)
- **ci**: docker image push readme updates (#398, #395)
- **ci**: fix errors with automated coverage (#432)
- **ci**: add workspaceDir to git safe directories for devcontainers (#379)
- **SQLite**: joins should be on ImageNumber, TableNumber, and not ImageNumber (#378)
- **compartments**: avoid lowercase compartment strings (#421)

### Refactor

- **pd**: avoid dataframe fragmentation in agg (#407)
- **pandas**: enable copy_on_write for pandas (#401)
- **bandit**: apply bandit checks (#387)
- **isort**: apply isort linting checks (#389)
- **ruff**: update ruff to 0.3.4 (#386)
- **dev**: add docker-in-docker feature to devcontainer (#381)

### Build

- **docker**: add Dockerfile and container image build tests (#362)
- **docker**: add docker hub push capabilities (#377)
- **deps**: various dependency updates automated by dependabot

### Docs

- **docker**: add docker installation instructions (#409)
- **docs**: improve documentation for non-CellProfiler datasets in Pycytominer (#430)
- **docs**: update error message and docs for features argument to clarify CellProfiler default expectations (#448)

### CI

- **ci**: update macOS version for Python (#408)
- **ci**: specify GitHub Actions Ubuntu runner image (#411)
- **ci**: enable GitHub Actions updates (#438)
- **ci**: add mypy check and adjust code for types (#439)

## v1.1.0 (2024-03-05)

[Detailed release notes](https://github.com/cytomining/pycytominer/releases/tag/v1.1.0)

### Fix

- **build**: fix build versioning
- simplify Spherize transform – epsilon to regularize instead of clip, add additional checks (see #320)

### Refactor

- **docs**: apply flake8-builtins checks
- **dev**: apply pyflakes checks
- **dev**: apply flake8-simplify checks

### Test

- add flake8-bandit ignores
- add clarifying comments for cell_loc test
- update cell_loc s3 paths and testing

### Docs

- update Readme with Citation section
- **template**: PR template attribution to comment
- **changelog**: add commitizen template
- add description of ruff linting/formatting
- reorganize style guide

### CI

- add versioned artifact build action
- **integration-test**: add explicit artifact retention time
- add pygrep-hooks and flake8-20202 checks

### Style

- swap out black for ruff-format
- apply pyupgrade checks
- **devcontainer**: add ruff extension
- apply ruff native checks
- apply pycodestyle checks
- add flake8-comprehensions checks

### Build

- **poetry**: make dev dep group optional

## v1.0.1 (2023-11-07)

[Detailed Release Notes](https://github.com/cytomining/pycytominer/releases/tag/v1.0.1)

### Fix

- **docs**: add dynamic versioning to docs build
- **collate**: move optional dep import to function

## v1.0.0 (2023-10-29)

[Detailed Release Notes](https://github.com/cytomining/pycytominer/releases/tag/v1.0.0)

### BREAKING CHANGE

- functions now use expect None type as argument where they previously expected a "none" string
- **drop_outliers**:: change default outlier threshold to 500
- **variance_threshold** change variance_threshold default to 0.01

### Feat

- add parquet support for all i/o functions
- **annotate**: add function to convert SQLite to pandas DataFrame and merge additional metadata
- **build**: switch to poetry for dependency management and builds
- **cells**: Enable custom image table name for SQLite queries in cells.py
- **deepprofiler**: add single cell output and normalization
- **dev**: standardized dev process and workflows (See [CONTRIBUTING.md](CONTRIBUTING.md)):
  - add vscode devcontainer and codespaces config
  - add conventional commit standards and commitizen cli
  - add pre-commit hooks for black, prettier, actionlint
- **docs**: Add detailed single-cell profiling [walkthrough](https://pycytominer.readthedocs.io/en/latest/walkthroughs/single_cell_usage.html)
- **utils**: cell_locations script to append X,Y locations to LoadData

### Fix

- **annotate**: add informative merge suffixes to platemap and external_metadata
- **annotate**: add check before dropping well columns
- **build**: remove python 3.7 support
- **correlation**: Use numpy to calculate pearson corr of non NaN matrices
- **load_profiles**: now works with PurePath types

## v0.2.0 (2022-06-17)

[Detailed Release Notes](https://github.com/cytomining/pycytominer/releases/tag/v0.2.0)

### Feat

- **collate**: support CellProfiler analyses directly from CellProfiler output files

## v0.1.5 (2022-06-17)

[Detailed Release Notes](https://github.com/cytomining/pycytominer/releases/tag/v0.1.5)

### Fix

- **merge_single_cells** - improve memory performance

## v0.1 (2022-02-08)

[Detailed Release Notes](https://github.com/cytomining/pycytominer/releases/tag/v0.1)
