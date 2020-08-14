import numpy as np
import pandas as pd
from pycytominer.operations import sparse_random_projection

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
        "Metadata_batch": [
            "day1",
            "day1",
            "day1",
            "day1",
            "day1",
            "day1",
            "day1",
            "day1",
        ],
        "x": [1, 2, 8, 2, 5, 5, 5, 1],
        "y": [3, 1, 7, 4, 5, 9, 6, 1],
        "z": [1, 8, 2, 5, 6, 22, 2, 2],
        "zz": [14, 46, 1, 6, 30, 100, 2, 2],
    }
)


def test_sparse_random_projection():
    """
    Testing the base covariance pycytominer function
    """
    n_components = 2
    cp_features = ["x", "y", "z"]
    seed = 123

    sparse_result = sparse_random_projection(
        population_df=data_df,
        variables=cp_features,
        n_components=n_components,
        seed=seed,
    ).round(2)

    expected_result = pd.DataFrame(
        {
            0: [2.79, 1.86],
            1: [0.93, -0.93],
            2: [6.51, -0.93],
            3: [3.72, 1.86],
            4: [4.65, 0.00],
            5: [8.38, 3.72],
            6: [5.58, 0.93],
            7: [0.93, 0.00],
        }
    ).transpose()

    expected_result.columns = ["sparse_comp_0", "sparse_comp_1"]

    assert sparse_result.equals(expected_result)


def test_sparse_random_projection_allvar():
    """
    Testing the base covariance pycytominer function
    """
    n_components = 2
    cp_features = "all"
    seed = 123

    input_data_df = data_df.loc[:, ["x", "y", "z", "zz"]]

    sparse_result = sparse_random_projection(
        population_df=input_data_df,
        variables=cp_features,
        n_components=n_components,
        seed=seed,
    ).round(2)

    expected_result = pd.DataFrame(
        {
            0: [16.0, -14.0],
            1: [45.0, -40.0],
            2: [0.0, -7.0],
            3: [8.0, -3.0],
            4: [30.0, -29.0],
            5: [104.0, -83.0],
            6: [3.0, -5.0],
            7: [2.0, -1.0],
        }
    ).transpose()

    expected_result.columns = ["sparse_comp_0", "sparse_comp_1"]

    assert sparse_result.equals(expected_result)
