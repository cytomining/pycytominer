import os
import random
import pytest
import tempfile
import warnings
import pandas as pd
from pycytominer.cyto_utils.features import get_blacklist_features

blacklist_file = os.path.join(
    os.path.dirname(__file__), "..", "data", "blacklist_features.txt"
)

blacklist = pd.read_csv(blacklist_file).blacklist.tolist()

data_blacklist_df = pd.DataFrame(
    {
        "Nuclei_Correlation_Manders_AGP_DNA": [1, 3, 8, 5, 2, 2],
        "Nuclei_Correlation_RWC_ER_RNA": [9, 3, 8, 9, 2, 9],
    }
).reset_index(drop=True)


def test_blacklist():
    blacklist_from_func = get_blacklist_features()
    assert blacklist == blacklist_from_func


def test_blacklist_df():
    blacklist_from_func = get_blacklist_features(population_df=data_blacklist_df)
    assert data_blacklist_df.columns.tolist() == blacklist_from_func
