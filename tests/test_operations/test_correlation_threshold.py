import pandas as pd
import pytest

from pycytominer.operations import correlation_threshold

# Build data to use in tests
data_df = pd.DataFrame({
    "x": [1, 3, 8, 5, 2, 2],
    "y": [1, 2, 8, 5, 2, 1],
    "z": [9, 3, 8, 9, 2, 9],
    "zz": [0, -3, 8, 9, 6, 9],
}).reset_index(drop=True)

data_uncorrelated_df = pd.DataFrame({
    "x": [2, 2, 2, 5, 2, 1],
    "y": [8, 1, 0, 3, -2, 0],
    "z": [-1, -6, 10, 2, 9, 10],
    "zz": [-90, 12, -8, -9, 0, -4],
}).reset_index(drop=True)


def test_correlation_threshold():
    correlation_threshold_result = correlation_threshold(
        population_df=data_df,
        features=data_df.columns.tolist(),
        samples="all",
        threshold=0.9,
        method="pearson",
    )

    expected_result = ["y"]

    assert correlation_threshold_result == expected_result

    correlation_threshold_result = correlation_threshold(
        population_df=data_df,
        features=data_df.columns.tolist(),
        samples="all",
        threshold=0.2,
        method="pearson",
    )

    expected_result = sorted(["y", "zz", "x"])

    assert sorted(correlation_threshold_result) == expected_result


def test_correlation_threshold_uncorrelated():
    correlation_threshold_result = correlation_threshold(
        population_df=data_uncorrelated_df,
        features=data_uncorrelated_df.columns.tolist(),
        samples="all",
        threshold=0.9,
        method="pearson",
    )

    assert len(correlation_threshold_result) == 0


def test_correlation_threshold_samples():
    # Add metadata_sample column
    data_sample_id_df = data_df.assign(
        Metadata_sample=[f"sample_{x}" for x in range(0, data_df.shape[0])]
    )
    correlation_threshold_result = correlation_threshold(
        population_df=data_sample_id_df,
        features=data_df.columns.tolist(),
        samples="Metadata_sample != 'sample_2'",
        threshold=0.9,
        method="pearson",
    )
    expected_result = ["y"]

    assert correlation_threshold_result == expected_result


def test_correlation_threshold_featureinfer():
    with pytest.raises(ValueError) as nocp:
        correlation_threshold_result = correlation_threshold(
            population_df=data_df,
            features="infer",
            samples="all",
            threshold=0.9,
            method="pearson",
        )

        expected_message = (
            "No features found. Pycytominer expects CellProfiler features by default. "
            "If you're using non-CellProfiler data, please specify the feature space using the `features` parameter."
        )
        assert expected_message in str(nocp.value)

    data_cp_df = data_df.copy()
    data_cp_df.columns = [f"Cells_{x}" for x in data_df.columns]

    correlation_threshold_result = correlation_threshold(
        population_df=data_cp_df,
        features="infer",
        samples="all",
        threshold=0.9,
        method="pearson",
    )

    expected_result = ["Cells_y"]

    assert correlation_threshold_result == expected_result
