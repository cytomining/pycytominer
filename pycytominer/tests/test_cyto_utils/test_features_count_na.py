import numpy as np
import pandas as pd
from pycytominer.cyto_utils import count_na_features

data_df = pd.DataFrame(
    {
        "x": [np.nan, 3, 8, 5, 2, 2],
        "y": [1, 2, 8, np.nan, 2, np.nan],
        "z": [9, 3, 8, 9, 2, 9],
        "zz": [np.nan, np.nan, 8, np.nan, 6, 9],
    }
).reset_index(drop=True)


def test_count_na_features():
    """
    Testing count_na_features pycytominer function
    """
    count_na_features_result = count_na_features(
        population_df=data_df, features=["x", "zz"]
    )

    expected_result = pd.DataFrame({"num_na": [1, 3]})
    expected_result.index = ["x", "zz"]

    assert count_na_features_result.equals(expected_result)
