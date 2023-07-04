import os
import random
import pytest
import pandas as pd
from pycytominer.cyto_utils.features import infer_cp_features


data_df = pd.DataFrame(
    {
        "Cells_Something_Something": [1, 3, 8, 5, 2, 2],
        "Cytoplasm_Something_Something": [1, 3, 8, 5, 2, 2],
        "Metadata_Something_Something": [1, 3, 8, 5, 2, 2],
        "Nuclei_Correlation_Manders_AGP_DNA": [1, 3, 8, 5, 2, 2],
        "Nuclei_Correlation_RWC_ER_RNA": [9, 3, 8, 9, 2, 9],
        "CElls_somethingwrong": [9, 3, 8, 9, 2, 9],
        "Nothing_somethingwrong": [9, 3, 8, 9, 2, 9],
        "": [9, 3, 8, 9, 2, 9],
        "dont pick me": [9, 3, 8, 9, 2, 9],
        "Image_Feature_1": [4, 7, 9, 2, 3, 1],
        "Image_Feature_2": [10, 4, 6, 1, 4, 5],
    }
).reset_index(drop=True)


non_cp_data_df = pd.DataFrame(
    {
        "x": [1, 3, 8, 5, 2, 2],
        "y": [1, 2, 8, 5, 2, 1],
        "z": [9, 3, 8, 9, 2, 9],
        "zz": [0, -3, 8, 9, 6, 9],
    }
).reset_index(drop=True)


def test_feature_infer():
    features = infer_cp_features(population_df=data_df)
    expected = [
        "Cells_Something_Something",
        "Cytoplasm_Something_Something",
        "Nuclei_Correlation_Manders_AGP_DNA",
        "Nuclei_Correlation_RWC_ER_RNA",
    ]

    assert features == expected


def test_feature_infer_nocp():
    with pytest.raises(AssertionError) as nocp:
        features = infer_cp_features(population_df=non_cp_data_df)

    assert "No CP features found." in str(nocp.value)


def test_metadata_feature_infer():
    features = infer_cp_features(population_df=data_df, metadata=True)
    expected = ["Metadata_Something_Something"]

    assert features == expected


def test_feature_infer_compartments():
    features = infer_cp_features(population_df=data_df, compartments=["CElls"])
    expected = ["Cells_Something_Something"]

    features2 = infer_cp_features(population_df=data_df, compartments=["nothing"])
    expected2 = ["Nothing_somethingwrong"]

    assert features == expected
    assert features2 == expected2


def test_feature_infer_image():
    features = infer_cp_features(population_df=data_df, image_features=True)
    expected = [
        "Cells_Something_Something",
        "Cytoplasm_Something_Something",
        "Nuclei_Correlation_Manders_AGP_DNA",
        "Nuclei_Correlation_RWC_ER_RNA",
        "Image_Feature_1",
        "Image_Feature_2",
    ]

    assert features == expected
