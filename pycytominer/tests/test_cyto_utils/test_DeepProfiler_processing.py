import os
import random
import pytest
import tempfile
import numpy as np
import pandas as pd

import sys
sys.path.insert(0, "../../../")
print(os.getcwd())

from pycytominer.cyto_utils import load_profiles, load_platemap, load_npz
from pycytominer.cyto_utils.load import infer_delim
from pycytominer.cyto_utils.DeepProfiler_processing import AggregateDeepProfiler


example_project_dir = os.path.join(
    os.path.dirname(__file__),
    "..",
    "test_data",
    "DeepProfiler_example_data_2"
)

profile_dir = os.path.join(
    example_project_dir,
    "outputs",
    "results",
    "features"
)

index_file = os.path.join(
    example_project_dir,
    "inputs",
    "metadata",
    "test_index.csv"
)

annotate_cols = ['feat0', 'feat1']

test_class = AggregateDeepProfiler(index_file=index_file, profile_dir=profile_dir, aggregate_on="well")

df = test_class.annotate_deep(annotate_cols=annotate_cols)

