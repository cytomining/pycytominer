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
