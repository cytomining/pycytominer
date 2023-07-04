import os
import random
import pytest
import tempfile
import warnings
import pandas as pd
from pycytominer.cyto_utils.features import drop_outlier_features

# Build data to use in tests
data_df = pd.DataFrame(
    {
        "Metadata_plate": ["a", "a", "a", "a", "b", "b", "b", "b"],
        "Metadata_treatment": [
            "drug",
            "drug",
            "control",
            "control",
            "drug",
            "drug",
            "control",
            "control",
        ],
        "Cells_x": [1, 2, -8, 2, 5, 5, 5, -1],
        "Cytoplasm_y": [3, -1, 7, 4, 5, -9, 6, 1],
        "Nuclei_z": [-1, 8, 2, 5, -6, 20, 2, -2],
        "Cells_zz": [14, -46, 1, 60, -30, -10000, 2, 2],
    }
).reset_index(drop=True)


def test_outlier_default():
    result = drop_outlier_features(data_df)
    expected_result = ["Cells_zz"]
    assert sorted(result) == sorted(expected_result)


def test_outlier_15_cutoff():
    result = drop_outlier_features(data_df, outlier_cutoff=15)
    expected_result = ["Cells_zz", "Nuclei_z"]
    assert sorted(result) == sorted(expected_result)


def test_outlier_samples_15():
    result = drop_outlier_features(data_df, samples=[0, 1, 2, 3, 5], outlier_cutoff=15)
    expected_result = ["Cells_zz", "Nuclei_z"]
    assert sorted(result) == sorted(expected_result)

    result = drop_outlier_features(data_df, samples=[0, 1, 2, 3], outlier_cutoff=15)
    expected_result = ["Cells_zz"]
    assert result == expected_result


def test_outlier_features():
    result = drop_outlier_features(data_df, features=["Cells_x", "Cytoplasm_y"])
    assert len(result) == 0
