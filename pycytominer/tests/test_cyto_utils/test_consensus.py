import os
import random
import numpy as np
import pandas as pd
from pycytominer.cyto_utils.consensus import modz_base, modz

# No replicate information
data_df = pd.DataFrame({"x": [1, 1, -1], "y": [5, 5, -5], "z": [2, 2, -2]})
data_df.index = ["sample_{}".format(x) for x in data_df.index]

# Include replicate information
data_replicate_df = pd.concat(
    [
        pd.DataFrame({"g": "a", "x": [1, 1, -1], "y": [5, 5, -5], "z": [2, 2, -2]}),
        pd.DataFrame({"g": "b", "x": [1, 3, 5], "y": [8, 3, 1], "z": [5, -2, 1]}),
    ]
).reset_index(drop=True)
data_replicate_df.index = ["sample_{}".format(x) for x in data_replicate_df.index]

precision = 4
replicate_columns = "g"


def test_modz_base():
    # The expected result is to completely remove influence of anticorrelated sample
    consensus_df = modz_base(data_df, min_weight=0, precision=precision)
    expected_result = pd.Series([1.0, 5.0, 2.0], index=data_df.columns)
    pd.testing.assert_series_equal(expected_result, consensus_df)

    # With the min_weight = 1, then modz is mean
    consensus_df = modz_base(data_df, min_weight=1, precision=precision)
    expected_result = data_df.mean().round(precision)
    pd.testing.assert_series_equal(
        expected_result, consensus_df, check_less_precise=True
    )


def test_modz():
    # The expected result is to completely remove influence of anticorrelated sample
    consensus_df = modz(
        data_replicate_df, replicate_columns, min_weight=0, precision=precision
    )
    expected_result = pd.DataFrame(
        {"x": [1.0, 4.0], "y": [5.0, 2.0], "z": [2.0, -0.5]}, index=["a", "b"]
    )
    expected_result.index.name = replicate_columns
    pd.testing.assert_frame_equal(expected_result, consensus_df)

    # With the min_weight = 1, then modz is mean
    consensus_df = modz(
        data_replicate_df, replicate_columns, min_weight=1, precision=precision
    )
    expected_result = data_replicate_df.groupby(replicate_columns).mean().round(4)
    expected_result.index.name = replicate_columns
    pd.testing.assert_frame_equal(
        expected_result, consensus_df, check_less_precise=True
    )


def test_modz_multiple_columns():
    replicate_columns = ["g", "h"]
    data_replicate_multi_df = data_replicate_df.assign(h=["c", "c", "c", "d", "d", "d"])
    # The expected result is to completely remove influence of anticorrelated sample
    consensus_df = modz(
        data_replicate_multi_df, replicate_columns, min_weight=0, precision=precision
    )
    expected_result = pd.DataFrame(
        {
            "g": ["a", "b"],
            "h": ["c", "d"],
            "x": [1.0, 4.0],
            "y": [5.0, 2.0],
            "z": [2.0, -0.5],
        }
    )
    pd.testing.assert_frame_equal(expected_result, consensus_df.reset_index())

    # With the min_weight = 1, then modz is mean
    consensus_df = modz(
        data_replicate_multi_df, replicate_columns, min_weight=1, precision=precision
    )
    expected_result = data_replicate_multi_df.groupby(replicate_columns).mean().round(4)
    expected_result.index.name = replicate_columns
    pd.testing.assert_frame_equal(
        expected_result, consensus_df, check_less_precise=True
    )
