import random
import pytest
import numpy as np
import pandas as pd
from pycytominer.operations import variance_threshold, calculate_frequency

random.seed(123)

# Build data to use in tests
data_df = pd.DataFrame(
    {
        "a": [1, 1, 1, 1, 1, 1],
        "b": [1, 1, 1, 1, 1, 2],
        "c": [1, 1, 1, 1, 2, 3],
        "x": [1, 3, 8, 5, 2, 2],
        "y": [1, 2, 8, 5, 2, 1],
        "z": [9, 3, 8, 9, 2, 9],
        "zz": [0, -3, 8, 9, 6, 9],
    }
).reset_index(drop=True)

data_unique_test_df = pd.DataFrame(
    {
        "a": [1] * 99 + [2],
        "b": [1, 2] * 50,
        "c": [1, 2] * 25 + random.sample(range(1, 1000), 50),
        "d": random.sample(range(1, 1000), 100),
    }
).reset_index(drop=True)


def test_calculate_frequency():
    """
    Testing calculate_frequency pycytominer function for variance threshold calculation
    """
    freq_cut = 0.05

    excluded_features_freq = data_df.apply(
        lambda x: calculate_frequency(x, freq_cut), axis="rows"
    )

    expect_names = ["a", "b", "c", "x", "y", "z", "zz"]
    expected_result = pd.Series(expect_names)
    expected_result.index = expect_names
    expected_result.a = np.nan

    pd.testing.assert_series_equal(
        excluded_features_freq.fillna("found me!"), expected_result.fillna("found me!")
    )

    freq_cut = 0.25
    excluded_features_freq = data_df.apply(
        lambda x: calculate_frequency(x, freq_cut), axis="rows"
    )
    expected_result = pd.Series(expect_names)
    expected_result.index = expect_names
    expected_result.a = np.nan
    expected_result.b = np.nan

    pd.testing.assert_series_equal(
        excluded_features_freq.fillna("found me!"), expected_result.fillna("found me!")
    )

    # Test missing value (see issue #69)
    missing_data = data_df.assign(missing=np.nan).apply(
        lambda x: calculate_frequency(x, freq_cut), axis="rows"
    )

    assert missing_data.isna().sum() == 3


def test_variance_threshold():
    unique_cut = 0.01
    excluded_features = variance_threshold(
        population_df=data_unique_test_df,
        features=data_unique_test_df.columns.tolist(),
        unique_cut=unique_cut,
    )
    expected_result = ["a"]

    assert sorted(excluded_features) == sorted(expected_result)

    unique_cut = 0.03
    excluded_features = variance_threshold(
        population_df=data_unique_test_df,
        features=data_unique_test_df.columns.tolist(),
        unique_cut=unique_cut,
    )
    expected_result = ["a", "b"]

    assert sorted(excluded_features) == sorted(expected_result)

    freq_cut = -1
    excluded_features_freq = data_unique_test_df.apply(
        lambda x: calculate_frequency(x, freq_cut), axis="rows"
    )

    excluded_features_freq = excluded_features_freq[
        excluded_features_freq.isna()
    ].index.tolist()

    assert len(excluded_features_freq) == 0


def test_variance_threshold_featureinfer():
    unique_cut = 0.01
    with pytest.raises(AssertionError) as nocp:
        excluded_features = variance_threshold(
            population_df=data_unique_test_df, features="infer", unique_cut=unique_cut
        )

    assert "No CP features found." in str(nocp.value)

    data_cp_df = data_unique_test_df.copy()
    data_cp_df.columns = ["Cells_{}".format(x) for x in data_unique_test_df.columns]

    excluded_features = variance_threshold(
        population_df=data_cp_df, features="infer", unique_cut=unique_cut
    )

    expected_result = ["Cells_a"]

    assert excluded_features == expected_result
