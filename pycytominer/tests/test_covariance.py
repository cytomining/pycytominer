import numpy as np
import pandas as pd
from pycytominer.covariance import covariance_base
from pycytominer.covariance import covariance

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


def test_covariance_base():
    """
    Testing the base covariance pycytominer function
    """
    cp_features = ["x", "y", "z"]
    cov_result = (
        covariance_base(population_df=data_df, variables=cp_features)
        .astype(np.float)
        .round(2)
    )

    expected_result = pd.DataFrame(
        {
            "x__x": [4.53],
            "y__x": [4.05],
            "y__y": [7.48],
            "z__x": [4.08],
            "z__y": [5.49],
            "z__z": [30.27],
        }
    )

    expected_result.index = ["covar"]
    expected_result.index.name = "covar_feature"

    assert cov_result.equals(expected_result)


def test_covariance_base_allvar():
    """
    Testing the base covariance pycytominer function
    """
    cp_features = "all"
    cov_result = (
        covariance_base(population_df=data_df, variables=cp_features)
        .astype(np.float)
        .round(2)
    )

    expected_result = pd.DataFrame(
        {
            "x__x": [4.53],
            "y__x": [4.05],
            "y__y": [7.48],
            "z__x": [4.08],
            "z__y": [5.49],
            "z__z": [30.27],
            "zz__x": [4.92],
            "zz__y": [38.86],
            "zz__z": [114.49],
            "zz__zz": [1210.83]
        }
    )

    expected_result.index = ["covar"]
    expected_result.index.name = "covar_feature"

    assert cov_result.equals(expected_result)


def test_covariance_sameresultexpected():
    """
    Testing the base covariance pycytominer function
    """
    cp_features = ["x", "y", "z"]
    cov_result = (
        covariance(population_df=data_df, variables=cp_features)
        .astype(np.float)
        .round(2)
    )

    expected_result = pd.DataFrame(
        {
            "x__x": [4.53],
            "y__x": [4.05],
            "y__y": [7.48],
            "z__x": [4.08],
            "z__y": [5.49],
            "z__z": [30.27],
        }
    )

    expected_result.index = ["covar"]
    expected_result.index.name = "covar_feature"

    assert cov_result.equals(expected_result)


def test_covariance_withgroups():
    """
    Testing the base covariance pycytominer function
    """
    cp_features = ["x", "y", "z"]
    strata = ["Metadata_batch"]
    cov_result = (
        covariance(population_df=data_df, variables=cp_features, strata=strata)
        .astype(np.float)
        .round(2)
    )

    expected_result = pd.concat(
        [
            pd.DataFrame(
                {
                    "Metadata_batch": ["day1"],
                    "covar_feature": ["covar"],
                    "x__x": [6.27],
                    "y__x": [5.64],
                    "y__y": [8.0],
                    "z__x": [3.00],
                    "z__y": [10.14],
                    "z__z": [47.71],
                }
            ),
            pd.DataFrame(
                {
                    "Metadata_batch": ["day2"],
                    "covar_feature": ["covar"],
                    "x__x": [1.57],
                    "y__x": [2.82],
                    "y__y": [8.0],
                    "z__x": [0.15],
                    "z__y": [1.01],
                    "z__z": [0.48],
                }
            ),
        ]
    )

    expected_result = expected_result.set_index(["Metadata_batch", "covar_feature"])

    assert cov_result.equals(expected_result)
