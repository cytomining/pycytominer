import random

import numpy as np
import pandas as pd
import pytest

from pycytominer.operations import calculate_frequency, frequency_threshold

random.seed(123)

# Build data to use in tests
data_df = pd.DataFrame({
    "a": [1, 1, 1, 1, 1, 1],
    "b": [1, 1, 1, 1, 1, 2],
    "c": [1, 1, 1, 1, 2, 3],
    "x": [1, 3, 8, 5, 2, 2],
    "y": [1, 2, 8, 5, 2, 1],
    "z": [9, 3, 8, 9, 2, 9],
    "zz": [0, -3, 8, 9, 6, 9],
}).reset_index(drop=True)

data_unique_test_df = pd.DataFrame({
    "a": [1] * 99 + [2],
    "b": [1, 2] * 50,
    "c": [1, 2] * 25 + random.sample(range(1, 1000), 50),
    "d": random.sample(range(1, 1000), 100),
}).reset_index(drop=True)


def test_calculate_frequency():
    """
    Testing calculate_frequency pycytominer function for frequency threshold calculation
    """
    freq_ratios = data_df.apply(calculate_frequency, axis="rows")

    expect_names = ["a", "b", "c", "x", "y", "z", "zz"]
    expected_result = pd.Series(
        [0.0, 1 / 5, 1 / 4, 1 / 2, 1.0, 1 / 3, 1 / 2], index=expect_names
    )

    pd.testing.assert_series_equal(freq_ratios, expected_result)

    # freq_cut of 0.05 should only exclude "a"
    assert (freq_ratios < 0.05).sum() == 1
    assert freq_ratios[freq_ratios < 0.05].index.tolist() == ["a"]

    # freq_cut of 0.25 should exclude "a" and "b"
    assert sorted(freq_ratios[freq_ratios < 0.25].index.tolist()) == ["a", "b"]

    # Test missing value (see issue #69)
    missing_freq_ratios = data_df.assign(missing=np.nan).apply(
        calculate_frequency, axis="rows"
    )

    assert (missing_freq_ratios < 0.25).sum() == 3


def test_frequency_threshold():
    """Test that frequency_threshold removes low-frequency and low-unique features."""
    unique_cut = 0.01
    excluded_features = frequency_threshold(
        population_df=data_unique_test_df,
        features=data_unique_test_df.columns.tolist(),
        unique_cut=unique_cut,
    )
    expected_result = ["a"]

    assert sorted(excluded_features) == sorted(expected_result)

    unique_cut = 0.03
    excluded_features = frequency_threshold(
        population_df=data_unique_test_df,
        features=data_unique_test_df.columns.tolist(),
        unique_cut=unique_cut,
    )
    expected_result = ["a", "b"]

    assert sorted(excluded_features) == sorted(expected_result)

    freq_cut = -1
    freq_ratios = data_unique_test_df.apply(calculate_frequency, axis="rows")
    excluded_features_freq = freq_ratios[freq_ratios < freq_cut].index.tolist()

    assert len(excluded_features_freq) == 0


@pytest.mark.parametrize(
    "kwargs, expected_error",
    [
        ({"freq_cut": -0.1}, "freq_cut variable must be between"),
        ({"freq_cut": 1.1}, "freq_cut variable must be between"),
        ({"unique_cut": -0.1}, "unique_cut variable must be between"),
        ({"unique_cut": 1.1}, "unique_cut variable must be between"),
        (
            {"features": "not-infer"},
            'features must be a list of column names or "infer"',
        ),
        (
            {"features": ("a", "b")},
            'features must be a list of column names or "infer"',
        ),
        ({"samples": ["a > 1"]}, "samples must be a string"),
    ],
)
def test_frequency_threshold_invalid_inputs(kwargs, expected_error):
    """Test that frequency_threshold rejects invalid threshold and subset inputs."""
    threshold_kwargs = {
        "population_df": data_unique_test_df,
        "features": data_unique_test_df.columns.tolist(),
    }
    threshold_kwargs.update(kwargs)

    with pytest.raises(ValueError, match=expected_error):
        frequency_threshold(**threshold_kwargs)


def test_frequency_threshold_featureinfer():
    """Test that frequency_threshold supports inferred CellProfiler features."""
    unique_cut = 0.01
    with pytest.raises(ValueError) as nocp:
        frequency_threshold(
            population_df=data_unique_test_df, features="infer", unique_cut=unique_cut
        )

    assert "No features or metadata found." in str(nocp.value)

    data_cp_df = data_unique_test_df.copy()
    data_cp_df.columns = [f"Cells_{x}" for x in data_unique_test_df.columns]

    excluded_features = frequency_threshold(
        population_df=data_cp_df, features="infer", unique_cut=unique_cut
    )

    expected_result = ["Cells_a"]

    assert excluded_features == expected_result


def test_frequency_threshold_samples():
    """Test that frequency_threshold calculates exclusions from selected samples."""
    unique_cut = 0.01
    excluded_features = frequency_threshold(
        population_df=data_unique_test_df,
        features=data_unique_test_df.columns.tolist(),
        samples="all",
        unique_cut=unique_cut,
    )
    expected_result = ["a"]

    assert sorted(excluded_features) == sorted(expected_result)

    # Add metadata_sample column
    data_sample_id_df = data_df.assign(
        Metadata_sample=[f"sample_{x}" for x in range(0, data_df.shape[0])]
    )

    excluded_features = frequency_threshold(
        population_df=data_sample_id_df,
        features=data_sample_id_df.columns.tolist(),
        samples="Metadata_sample != 'sample_5'",
        unique_cut=unique_cut,
    )
    expected_result = ["a", "b"]
    assert sorted(excluded_features) == sorted(expected_result)
