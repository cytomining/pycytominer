<img height="200" src="https://raw.githubusercontent.com/cytomining/pycytominer/master/logo/with-text-for-light-bg.png?raw=true">

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

### CSV collation

If running your images on a cluster, unless you have a MySQL or similar large database set up then you will likely end up with lots of different folders from the different cluster runs (often one per well or one per site), each one containing an `Image.csv`, `Nuclei.csv`, etc.
In order to look at full plates, therefore, we first need to collate all of these CSVs into a single file (currently SQLite) per plate.
We currently do this with a library called [cytominer-database](https://github.com/cytomining/cytominer-database).

If you want to perform this data collation inside pycytominer using the `cyto_utils` function `collate` (and/or you want to be able to run the tests and have them all pass!), you will need `cytominer-database==0.3.4`; this will change your installation commands slightly:

```bash
# Example for general case commit:
pip install "pycytominer[collate] @ git+git://github.com/cytomining/pycytominer"
# Example for specific commit:
pip install "pycytominer[collate] @ git+git://github.com/cytomining/pycytominer@2aa8638d7e505ab510f1d5282098dd59bb2cb470"
```

If using `pycytominer` in a conda environment, in order to run `collate.py`, you will also want to make sure to add `cytominer-database=0.3.4` to your list of dependencies.

## Creating a cell locations lookup table

The `CellLocation` class offers a convenient way to augment a [LoadData](https://cellprofiler-manual.s3.amazonaws.com/CPmanual/LoadData.html) file with X,Y locations of cells in each image.
The locations information is obtained from a single cell SQLite file.

To use this functionality, you will need to modify your installation command, similar to above:

```bash
# Example for general case commit:
pip install "pycytominer[cell_locations] @ git+git://github.com/cytomining/pycytominer"
```

Example using this functionality:

```bash
metadata_input="s3://cellpainting-gallery/test-cpg0016-jump/source_4/workspace/load_data_csv/2021_08_23_Batch12/BR00126114/test_BR00126114_load_data_with_illum.parquet"
single_single_cell_input="s3://cellpainting-gallery/test-cpg0016-jump/source_4/workspace/backend/2021_08_23_Batch12/BR00126114/test_BR00126114.sqlite"
augmented_metadata_output="~/Desktop/load_data_with_illum_and_cell_location_subset.parquet"

python \
    -m pycytominer.cyto_utils.cell_locations_cmd \
    --metadata_input ${metadata_input} \
    --single_cell_input ${single_single_cell_input}   \
    --augmented_metadata_output ${augmented_metadata_output} \
    add_cell_location

# Check the output

python -c "import pandas as pd; print(pd.read_parquet('${augmented_metadata_output}').head())"

# It should look something like this (depends on the width of your terminal):

#   Metadata_Plate Metadata_Well Metadata_Site  ...                                   PathName_OrigRNA ImageNumber                                        CellCenters
# 0     BR00126114           A01             1  ...  s3://cellpainting-gallery/cpg0016-jump/source_...           1  [{'Nuclei_Location_Center_X': 943.512129380054...
# 1     BR00126114           A01             2  ...  s3://cellpainting-gallery/cpg0016-jump/source_...           2  [{'Nuclei_Location_Center_X': 29.9516027655562...
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
