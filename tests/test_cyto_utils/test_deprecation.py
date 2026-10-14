"""
Backward-compatibility: deprecated population_df parameter
"""

import warnings

import pandas as pd
import pytest

from pycytominer.cyto_utils import (
    count_na_features,
    drop_outlier_features,
    get_blocklist_features,
    get_pairwise_correlation,
    infer_cp_features,
    modz,
)
from pycytominer.cyto_utils.modz import modz_base
from pycytominer.operations import (
    correlation_threshold,
    frequency_threshold,
    get_na_columns,
    noise_removal,
    variance_threshold,
)

data_df = pd.DataFrame({
    "Metadata_group": ["a", "a", "b", "b", "c", "c"],
    "Cells_x": [1, 3, 8, 5, 2, 2],
    "Cytoplasm_y": [1, 2, 8, 5, 2, 1],
    "Nuclei_z": [9, 3, 8, 9, 2, 9],
})
feature_df = data_df.drop(columns="Metadata_group")


def _assert_same(legacy, current):
    """Compare results from functions returning lists, pandas objects, or tuples."""
    if isinstance(current, tuple):
        for legacy_item, current_item in zip(legacy, current, strict=True):
            _assert_same(legacy_item, current_item)
    elif isinstance(current, pd.DataFrame):
        pd.testing.assert_frame_equal(legacy, current)
    elif isinstance(current, pd.Series):
        pd.testing.assert_series_equal(legacy, current)
    else:
        assert legacy == current


@pytest.mark.parametrize(
    "func, data, kwargs",
    [
        (infer_cp_features, data_df, {}),
        (count_na_features, data_df, {"features": ["Cells_x", "Nuclei_z"]}),
        (drop_outlier_features, data_df, {}),
        (get_blocklist_features, data_df, {}),
        (get_pairwise_correlation, feature_df, {}),
        (modz_base, feature_df, {}),
        (modz, data_df, {"replicate_columns": "Metadata_group"}),
        (correlation_threshold, data_df, {}),
        (frequency_threshold, data_df, {}),
        (get_na_columns, data_df, {}),
        (noise_removal, data_df, {"noise_removal_perturb_groups": "Metadata_group"}),
        (variance_threshold, data_df, {}),
    ],
)
def test_population_df_deprecated(func, data, kwargs):
    """Passing population_df warns and returns the same result as profiles."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        legacy = func(population_df=data, **kwargs)

    deprecation_warnings = [
        w
        for w in caught
        if issubclass(w.category, DeprecationWarning)
        and "population_df" in str(w.message)
    ]
    assert len(deprecation_warnings) == 1
    assert func.__name__ in str(deprecation_warnings[0].message)

    _assert_same(legacy, func(profiles=data, **kwargs))
