import os
import random
import pytest
import tempfile
import warnings
import pandas as pd
from pycytominer.cyto_utils.features import get_blocklist_features

blocklist_file = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "blocklist_features.txt"
)

blocklist = pd.read_csv(blocklist_file).blocklist.tolist()

data_blocklist_df = pd.DataFrame(
    {
        "Nuclei_Correlation_Manders_AGP_DNA": [1, 3, 8, 5, 2, 2],
        "Nuclei_Correlation_RWC_ER_RNA": [9, 3, 8, 9, 2, 9],
    }
).reset_index(drop=True)


def test_blocklist():
    blocklist_from_func = get_blocklist_features()
    assert blocklist == blocklist_from_func


def test_blocklist_df():
    blocklist_from_func = get_blocklist_features(population_df=data_blocklist_df)
    assert data_blocklist_df.columns.tolist() == blocklist_from_func
