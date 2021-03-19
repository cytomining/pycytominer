# Pycytominer: Data processing functions for profiling perturbations

[![Build Status](https://travis-ci.org/cytomining/pycytominer.svg?branch=master)](https://travis-ci.org/cytomining/pycytominer)
[![Coverage Status](https://codecov.io/gh/cytomining/pycytominer/branch/master/graph/badge.svg)](https://codecov.io/github/cytomining/pycytominer?branch=master)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Pycytominer is a suite of common functions used to process high dimensional readouts from high-throughput cell experiments.

## Installation

Pycytominer is still in beta, and can only be installed from GitHub:

```bash
pip install git+git://github.com/cytomining/pycytominer
```

Since the project is actively being developed, with new features added regularly, we recommend installation using a hash:

```bash
# Example:
pip install git+git://github.com/cytomining/pycytominer@1806088bfd7f9be961a320635a0ddc66a12b5fb0
```

## Usage

Using pycytominer is simple and fun.

```python
# Real world example
import pandas as pd
import pycytominer

commit = "da8ae6a3bc103346095d61b4ee02f08fc85a5d98"
url = f"https://media.githubusercontent.com/media/broadinstitute/lincs-cell-painting/{commit}/profiles/2016_04_01_a549_48hr_batch1/SQ00014812/SQ00014812_augmented.csv.gz"

df = pd.read_csv(url)

normalized_df = pycytominer.normalize(
    profiles=df,
    method="standardize",
    samples="Metadata_broad_sample == 'DMSO'"
)
```

The API is consistent for the five major processing functions:

1. Aggregate
2. Annotate
3. Normalize
4. Feature Select
5. Consensus

Each processing function has unique arguments, more specific documentation coming soon.

### Customized usage

Pycytominer was written with a goal of processing any high-throughput profiling data.
However, the initial use case was developed for processing image-based profiling experiments specifically.
And, more specifically than that, image-based profiling readouts from [CellProfiler](https://github.com/CellProfiler) measurements from Cell Painting data.

Therefore, we have included some custom tools in `pycytominer/cyto_utils`.
