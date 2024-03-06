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
