from scipy.cluster.vq import whiten
import numpy as np
import pandas as pd
from pycytominer.transform import transform
from pycytominer.transform import whiten_transform

data_one_df = pd.DataFrame(
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

data_two_df = pd.DataFrame(
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
            "day2",
            "day2",
            "day2",
            "day2",
            "day2",
            "day2",
            "day2",
            "day2",
        ],
        "x": [x * 0.5 for x in [1, 2, 8, 2, 5, 5, 5, 1]],
        "y": [x - (1 * 0.2) for x in [3, 1, 7, 4, 5, 9, 6, 1]],
        "z": [x * 0.1 for x in [1, 8, 2, 5, 6, 22, 2, 2]],
        "zz": [x * 1.1 for x in [14, 46, 1, 6, 30, 100, 2, 2]],
    }
)

data_df = pd.concat([data_one_df, data_two_df]).reset_index(drop=True)


def test_transform():
    """
    Testing the base covariance pycytominer function
    """
    cp_features = ["x", "y", "z"]

    transform_result = (
        transform(
            population_df=data_df,
            variables=cp_features,
            strata="none",
            operation="whiten",
        )
        .astype(np.float)
        .round(2)
    )

    expected_result = pd.DataFrame(
        {
            "x": [
                0.49,
                0.97,
                3.88,
                0.97,
                2.43,
                2.43,
                2.43,
                0.49,
                0.24,
                0.49,
                1.94,
                0.49,
                1.21,
                1.21,
                1.21,
                0.24,
            ],
            "y": [
                1.13,
                0.38,
                2.64,
                1.51,
                1.89,
                3.4,
                2.27,
                0.38,
                1.06,
                0.3,
                2.57,
                1.44,
                1.81,
                3.32,
                2.19,
                0.3,
            ],
            "z": [
                0.19,
                1.5,
                0.38,
                0.94,
                1.13,
                4.13,
                0.38,
                0.38,
                0.02,
                0.15,
                0.04,
                0.09,
                0.11,
                0.41,
                0.04,
                0.04,
            ],
        }
    )

    assert transform_result.equals(expected_result)


def test_transform_all_vars():
    """
    Testing the base covariance pycytominer function
    """
    cp_features = "all"
    input_df = data_df.loc[:, ["x", "y", "z", "zz"]]

    transform_result = (
        transform(
            population_df=input_df,
            variables=cp_features,
            strata="none",
            operation="whiten",
        )
        .astype(np.float)
        .round(2)
    )

    expected_result = pd.DataFrame(
        {
            "x": [
                0.49,
                0.97,
                3.88,
                0.97,
                2.43,
                2.43,
                2.43,
                0.49,
                0.24,
                0.49,
                1.94,
                0.49,
                1.21,
                1.21,
                1.21,
                0.24,
            ],
            "y": [
                1.13,
                0.38,
                2.64,
                1.51,
                1.89,
                3.4,
                2.27,
                0.38,
                1.06,
                0.3,
                2.57,
                1.44,
                1.81,
                3.32,
                2.19,
                0.3,
            ],
            "z": [
                0.19,
                1.5,
                0.38,
                0.94,
                1.13,
                4.13,
                0.38,
                0.38,
                0.02,
                0.15,
                0.04,
                0.09,
                0.11,
                0.41,
                0.04,
                0.04,
            ],
            "zz": [
                0.42,
                1.37,
                0.03,
                0.18,
                0.89,
                2.97,
                0.06,
                0.06,
                0.46,
                1.5,
                0.03,
                0.2,
                0.98,
                3.26,
                0.07,
                0.07,
            ],
        }
    )

    assert transform_result.equals(expected_result)


def test_transform_strata():
    """
    Testing the base covariance pycytominer function
    """
    cp_features = ["x", "y", "z"]
    strata = ["Metadata_batch", "Metadata_treatment"]

    transform_result = transform(
        population_df=data_df,
        variables=cp_features,
        strata=strata,
        operation="whiten",
    ).round(2)

    expected_result = pd.DataFrame(
        {
            "Metadata_batch": ["day1"] * 8 + ["day2"] * 8,
            "Metadata_treatment": ["control"] * 4
            + ["drug"] * 4
            + ["control"] * 4
            + ["drug"] * 4,
            "x": [
                2.92,
                0.73,
                1.83,
                0.37,
                0.56,
                1.12,
                2.8,
                2.8,
                2.92,
                0.73,
                1.83,
                0.37,
                0.56,
                1.12,
                2.8,
                2.8,
            ],
            "y": [
                3.06,
                1.75,
                2.62,
                0.44,
                1.01,
                0.34,
                1.69,
                3.04,
                2.97,
                1.66,
                2.53,
                0.35,
                0.95,
                0.27,
                1.62,
                2.97,
            ],
            "z": [
                1.54,
                3.85,
                1.54,
                1.54,
                0.13,
                1.03,
                0.77,
                2.82,
                1.54,
                3.85,
                1.54,
                1.54,
                0.13,
                1.03,
                0.77,
                2.82,
            ],
        }
    )

    assert transform_result.equals(expected_result)
