import numpy as np
import pandas as pd
from pycytominer.get_na_columns import get_na_columns

data_df = pd.DataFrame(
    {
        "x": [np.nan, 3, 8, 5, 2, 2],
        "y": [1, 2, 8, np.nan, 2, np.nan],
        "z": [9, 3, 8, 9, 2, 9],
        "zz": [np.nan, np.nan, 8, np.nan, 6, 9],
    }
).reset_index(drop=True)


def test_get_na_columns():
    """
    Testing get_na_columns pycytominer function
    """
    get_na_columns_result = get_na_columns(
        population_df=data_df, variables=["x", "y", "zz"], cutoff=0.4
    )

    expected_result = ["zz"]

    assert get_na_columns_result == expected_result
