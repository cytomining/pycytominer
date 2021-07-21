# Pycytominer: Data processing functions for profiling perturbations

[![Build Status](https://travis-ci.org/cytomining/pycytominer.svg?branch=master)](https://travis-ci.org/cytomining/pycytominer)
[![Coverage Status](https://codecov.io/gh/cytomining/pycytominer/branch/master/graph/badge.svg)](https://codecov.io/github/cytomining/pycytominer?branch=master)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![RTD](https://readthedocs.org/projects/pycytominer/badge/?version=latest&style=flat)](https://pycytominer.readthedocs.io/)

Pycytominer is a suite of common functions used to process high dimensional readouts from high-throughput cell experiments.
The tool is most often used for processing data through the following pipeline:

![pipeline](/media/pipeline.png)

Image data flow from the microscope to segmentation and feature extraction tools (e.g. CellProfiler or DeepProfiler).
From here, additional single cell processing tools curate the single cell readouts into a form manageable for pycytominer input.
For CellProfiler, we use [cytominer-database](https://github.com/cytomining/cytominer-database) or [cytominer-transport](https://github.com/cytomining/cytominer-transport).
For DeepProfiler, we include single cell processing tools in [pycytominer.cyto_utils](pycytominer/cyto_utils/).

From the single cell output, we perform five steps using a simple API (described below), before passing along our data to [cytominer-eval](https://github.com/cytomining/cytominer-eval) for quality and perturbation strength evaluation.

## API

The API is consistent for the five major processing functions:

1. Aggregate
2. Annotate
3. Normalize
4. Feature select
5. Consensus

Each processing function has unique arguments, see our [documentation](https://pycytominer.readthedocs.io/) for more details.

## Installation

Pycytominer is still in beta, and can only be installed from GitHub:

```bash
pip install git+git://github.com/cytomining/pycytominer
```

Since the project is actively being developed, with new features added regularly, we recommend installation using a hash:

```bash
# Example:
pip install git+git://github.com/cytomining/pycytominer@2aa8638d7e505ab510f1d5282098dd59bb2cb470
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

### Customized usage

Pycytominer was written with a goal of processing any high-throughput profiling data.
However, the initial use case was developed for processing image-based profiling experiments specifically.
And, more specifically than that, image-based profiling readouts from [CellProfiler](https://github.com/CellProfiler) measurements from [Cell Painting](https://www.nature.com/articles/nprot.2016.105) data.

Therefore, we have included some custom tools in `pycytominer/cyto_utils`.

## Citation

Please support computational biology by citing software.
If you have used pycytominer in your project, please cite us as:

> @software{pycytominer,
  author = {Way, G.P., Chandrasekaran, N., Bornholdt, M., Tsang, H., Adeboye, A., Cimini, B., Weisbart, E., Jamali, N., Ryder, P., Singh, S., Carpenter, A.E.},
  title = {Pycytominer: Data processing functions for profiling perturbations},
  url = {https://github.com/cytomining/pycytominer},
  version = {0.1},
  date = {2021},
}
