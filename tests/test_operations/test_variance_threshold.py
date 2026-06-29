import pandas as pd
import pytest

from pycytominer.operations import variance_threshold


def test_variance_threshold():
    """Test that variance_threshold removes low-variance features."""
    data_var_test_df = pd.DataFrame({
        "low_var": [1, 1, 1, 1, 1.001, 1.001],
        "high_var": [0, 0, 10, 10, 20, 20],
    }).reset_index(drop=True)

    excluded_features = variance_threshold(
        population_df=data_var_test_df,
        features=data_var_test_df.columns.tolist(),
        min_variance=0.000001,
    )

    assert excluded_features == ["low_var"]


def test_variance_threshold_min_variance_zero_excludes_no_features():
    """Test that variance_threshold keeps all features when min_variance is zero."""
    data_var_test_df = pd.DataFrame({
        "low_var": [1, 1, 1, 1, 1.001, 1.001],
        "high_var": [0, 0, 10, 10, 20, 20],
    }).reset_index(drop=True)

    excluded_features = variance_threshold(
        population_df=data_var_test_df,
        features=data_var_test_df.columns.tolist(),
        min_variance=0.0,
    )

    assert excluded_features == []


@pytest.mark.parametrize("min_variance", [-0.1, "0.1", None])
def test_variance_threshold_min_variance_invalid(min_variance):
    """Test that variance_threshold rejects invalid min_variance values."""
    with pytest.raises(ValueError):
        variance_threshold(
            population_df=pd.DataFrame({"feature": [1, 2, 3]}),
            features=["feature"],
            min_variance=min_variance,
        )


def test_variance_threshold_featureinfer():
    """Test that variance_threshold supports inferred CellProfiler features."""
    data_cp_df = pd.DataFrame({
        "Cells_low_var": [1, 1, 1, 1, 1.001, 1.001],
        "Cells_high_var": [0, 0, 10, 10, 20, 20],
    }).reset_index(drop=True)

    excluded_features = variance_threshold(
        population_df=data_cp_df, features="infer", min_variance=0.000001
    )

    assert excluded_features == ["Cells_low_var"]


def test_variance_threshold_samples():
    """Test that variance_threshold calculates exclusions from selected samples."""
    data_sample_id_df = pd.DataFrame({
        "low_var": [1, 1, 1, 1, 1.001, 100],
        "high_var": [0, 0, 10, 10, 20, 20],
        "Metadata_sample": [f"sample_{x}" for x in range(0, 6)],
    }).reset_index(drop=True)

    excluded_features = variance_threshold(
        population_df=data_sample_id_df,
        features=["low_var", "high_var"],
        samples="Metadata_sample != 'sample_5'",
        min_variance=0.000001,
    )

    assert excluded_features == ["low_var"]
