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
