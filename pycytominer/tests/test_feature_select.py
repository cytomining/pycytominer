import random
import numpy as np
import pandas as pd
from pycytominer.feature_select import feature_select

random.seed(123)

data_df = pd.DataFrame(
    {
        "x": [1, 3, 8, 5, 2, 2],
        "y": [1, 2, 8, 5, 2, 1],
        "z": [9, 3, 8, 9, 2, 9],
        "zz": [0, -3, 8, 9, 6, 9],
    }
).reset_index(drop=True)

data_na_df = pd.DataFrame(
    {
        "x": [np.nan, 3, 8, 5, 2, 2],
        "xx": [np.nan, 3, 8, 5, 2, 2],
        "y": [1, 2, 8, np.nan, 2, np.nan],
        "yy": [1, 2, 8, 10, 2, 100],
        "z": [9, 3, 8, 9, 2, np.nan],
        "zz": [np.nan, np.nan, 8, np.nan, 6, 9],
    }
).reset_index(drop=True)

a_feature = [1] * 99 + [2]
b_feature = [1, 2] * 50
c_feature = [1, 2] * 25 + random.sample(range(1, 1000), 50)
d_feature = random.sample(range(1, 1000), 100)

data_unique_test_df = pd.DataFrame(
    {"a": a_feature, "b": b_feature, "c": c_feature, "d": d_feature}
).reset_index(drop=True)


def test_feature_select_get_na_columns():
    """
    Testing feature_select and get_na_columns pycytominer function
    """
    features = data_na_df.columns.tolist()
    result = feature_select(data_na_df, features=features, operation="drop_na_columns")
    expected_result = pd.DataFrame({"yy": [1, 2, 8, 10, 2, 100]})
    pd.testing.assert_frame_equal(result, expected_result)

    result = feature_select(data_na_df, features=features, operation="drop_na_columns", na_cutoff=0.3)
    expected_result = pd.DataFrame(
        {
            "x": [np.nan, 3, 8, 5, 2, 2],
            "xx": [np.nan, 3, 8, 5, 2, 2],
            "yy": [1, 2, 8, 10, 2, 100],
            "z": [9, 3, 8, 9, 2, np.nan],
        }
    )
    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_variance_threshold():
    """
    Testing feature_select and variance_threshold pycytominer function
    """
    features = data_unique_test_df.columns.tolist()
    result = feature_select(
        data_unique_test_df, features=features, operation="variance_threshold", unique_cut=0.01
    )
    expected_result = pd.DataFrame(
        {"b": b_feature, "c": c_feature, "d": d_feature}
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, expected_result)

    na_data_unique_test_df = data_unique_test_df.copy()
    na_data_unique_test_df.iloc[[x for x in range(0, 50)], 1] = np.nan
    features = na_data_unique_test_df.columns.tolist()
    result = feature_select(
        na_data_unique_test_df, features=features, operation=["drop_na_columns", "variance_threshold"]
    )
    expected_result = pd.DataFrame({"c": c_feature, "d": d_feature}).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(result, expected_result)

    na_data_unique_test_df = data_unique_test_df.copy()
    na_data_unique_test_df.iloc[[x for x in range(0, 50)], 1] = np.nan
    features = na_data_unique_test_df.columns.tolist()
    result = feature_select(
        na_data_unique_test_df, features=features, operation=["variance_threshold", "drop_na_columns"]
    )
    expected_result = pd.DataFrame({"c": c_feature, "d": d_feature}).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_correlation_threshold():
    """
    Testing feature_select and correlation_threshold pycytominer function
    """

    result = feature_select(data_df, operation="correlation_threshold")
    expected_result = data_df.drop(["y"], axis="columns")
    pd.testing.assert_frame_equal(result, expected_result)

    data_cor_thresh_na_df = data_df.copy()
    data_cor_thresh_na_df.iloc[0, 2] = np.nan
    features = data_cor_thresh_na_df.columns.tolist()
    result = feature_select(
        data_cor_thresh_na_df, features=features, operation=["drop_na_columns", "correlation_threshold"]
    )
    expected_result = data_df.drop(["z", "x"], axis="columns")
    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_all():
    data_all_test_df = data_unique_test_df.assign(zz=a_feature)
    data_all_test_df.iloc[1, 4] = 2
    data_all_test_df.iloc[[x for x in range(0, 50)], 1] = np.nan
    features = data_all_test_df.columns.tolist()
    result = feature_select(
        population_df=data_all_test_df,
        features=features,
        operation=["drop_na_columns", "correlation_threshold"],
        corr_threshold=0.7,
    )
    expected_result = pd.DataFrame(
        {"c": c_feature, "d": d_feature, "zz": a_feature}
    ).reset_index(drop=True)
    expected_result.iloc[1, 2] = 2
    pd.testing.assert_frame_equal(result, expected_result)

    result = feature_select(
        population_df=data_all_test_df,
        features=features,
        operation=["drop_na_columns", "correlation_threshold", "variance_threshold"],
        corr_threshold=0.7,
    )
    expected_result = pd.DataFrame({"c": c_feature, "d": d_feature}).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(result, expected_result)
