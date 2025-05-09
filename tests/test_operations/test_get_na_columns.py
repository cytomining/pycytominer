import numpy as np
import pandas as pd
import pytest

from pycytominer.operations import get_na_columns

data_df = pd.DataFrame({
    "x": [np.nan, 3, 8, 5, 2, 2],
    "y": [1, 2, 8, np.nan, 2, np.nan],
    "z": [9, 3, 8, 9, 2, np.nan],
    "zz": [np.nan, np.nan, 8, np.nan, 6, 9],
}).reset_index(drop=True)


def test_get_na_columns():
    """
    Testing get_na_columns pycytominer function
    """
    get_na_columns_result = get_na_columns(
        population_df=data_df, features=["x", "y", "zz"], cutoff=0.4
    )
    expected_result = ["zz"]
    assert get_na_columns_result == expected_result

    get_na_columns_result = get_na_columns(
        population_df=data_df, features=data_df.columns.tolist(), cutoff=0.1
    )
    expected_result = ["x", "y", "z", "zz"]
    assert sorted(get_na_columns_result) == expected_result

    get_na_columns_result = get_na_columns(
        population_df=data_df, features=["x", "y", "zz"], cutoff=0.3
    )
    expected_result = ["y", "zz"]
    assert sorted(get_na_columns_result) == expected_result

    get_na_columns_result = get_na_columns(
        population_df=data_df, features=["x", "y", "zz"], cutoff=0.5
    )
    assert len(get_na_columns_result) == 0


def test_get_na_columns_sample():
    """
    Testing get_na_columns pycyominer function with samples option
    """
    data_sample_id_df = data_df.assign(
        Metadata_sample=[f"sample_{x}" for x in range(0, data_df.shape[0])]
    )
    get_na_columns_result = get_na_columns(
        population_df=data_sample_id_df,
        samples="Metadata_sample != 'sample_0'",
        features=["x", "y", "zz"],
        cutoff=0.4,
    )

    assert len(get_na_columns_result) == 0

    get_na_columns_result = get_na_columns(
        population_df=data_sample_id_df,
        samples="Metadata_sample != 'sample_0'",
        features=["x", "y", "zz"],
        cutoff=0.1,
    )
    expected_result = ["y", "zz"]
    assert sorted(get_na_columns_result) == expected_result


def test_get_na_columns_featureinfer():
    with pytest.raises(ValueError) as nocp:
        get_na_columns(
            population_df=data_df, samples="all", features="infer", cutoff=0.1
        )

        assert "No features found." in str(nocp.value)


def test_get_na_columns_cutoff_parameter():
    # Define invalid cutoff values
    invalid_cutoffs = [10, -0.1]

    # Test cutoff parameter
    for cutoff in invalid_cutoffs:
        # Test if ValueError is raised with invalid cutoff values
        with pytest.raises(
            ValueError, match="cutoff variable must be between \\(0 and 1\\)"
        ):
            get_na_columns(
                population_df=data_df, samples="all", features="infer", cutoff=cutoff
            )
