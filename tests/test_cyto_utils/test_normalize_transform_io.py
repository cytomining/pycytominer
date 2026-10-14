import os
import tempfile

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.preprocessing import RobustScaler, StandardScaler

from pycytominer.cyto_utils.normalize_transform_io import (
    AVAIL_METHODS,
    load_normalize_transform,
    save_normalize_transform,
)
from pycytominer.operations import InverseNormalTransform, RobustMAD, Spherize

tmpdir = tempfile.gettempdir()

features = ["x", "y", "z", "zz"]

data_df = pd.DataFrame({
    "x": [1, 2, 8, 2, 5, 5, 5, 1],
    "y": [3, 1, 7, 4, 5, 9, 6, 1],
    "z": [1, 8, 2, 5, 6, 22, 2, 2],
    "zz": [14, 46, 1, 6, 30, 100, 2, 2],
}).astype(float)


def _fitted_scaler(method):
    """Build and fit the scaler `normalize()` would build for `method`."""
    if method == "standardize":
        return StandardScaler().fit(data_df)
    if method == "robustize":
        return RobustScaler().fit(data_df)
    if method == "mad_robustize":
        return RobustMAD(epsilon=0.5).fit(data_df)
    if method == "spherize":
        return Spherize(center=True, method="ZCA-cor", epsilon=0.25).fit(data_df)
    if method == "inverse_normal":
        return InverseNormalTransform(n_quantiles=4).fit(data_df)
    raise ValueError(method)


@pytest.mark.parametrize("method", AVAIL_METHODS)
def test_save_and_load_roundtrip(method):
    """Saving and loading a fitted scaler should preserve the method, the
    feature order, and the ability to reproduce the original transform()
    output."""
    transform_file = os.path.join(tmpdir, f"test_transform_io_{method}.joblib")
    scaler = _fitted_scaler(method)
    expected = scaler.transform(data_df)

    save_normalize_transform(
        scaler=scaler, method=method, joblib_output_file=transform_file
    )

    assert os.path.exists(transform_file)

    loaded_scaler, loaded_method, loaded_features, params = load_normalize_transform(
        transform_file
    )

    assert loaded_method == method
    assert loaded_features == features
    assert isinstance(params, dict)

    np.testing.assert_allclose(loaded_scaler.transform(data_df), expected)


def test_save_normalize_transform_method_is_lowercased():
    scaler = _fitted_scaler("standardize")
    transform_file = os.path.join(tmpdir, "test_transform_io_uppercase_method.joblib")

    save_normalize_transform(
        scaler=scaler, method="STANDARDIZE", joblib_output_file=transform_file
    )

    _, loaded_method, _, _ = load_normalize_transform(transform_file)
    assert loaded_method == "standardize"


def test_save_normalize_transform_invalid_method_raises():
    scaler = _fitted_scaler("standardize")
    transform_file = os.path.join(tmpdir, "test_transform_io_invalid_method.joblib")

    with pytest.raises(ValueError):
        save_normalize_transform(
            scaler=scaler, method="not_a_real_method", joblib_output_file=transform_file
        )

    assert not os.path.exists(transform_file)


def test_save_normalize_transform_requires_feature_names_in():
    """A scaler fit on a bare numpy array (no column names) has no
    `feature_names_in_`, and can't be saved since features can't be
    recovered from it on load."""
    scaler = StandardScaler().fit(data_df.to_numpy())
    transform_file = os.path.join(tmpdir, "test_transform_io_no_feature_names.joblib")

    with pytest.raises(ValueError):
        save_normalize_transform(
            scaler=scaler, method="standardize", joblib_output_file=transform_file
        )

    assert not os.path.exists(transform_file)


@pytest.mark.parametrize(
    ("method", "extra_kwargs", "expected_params"),
    [
        ("standardize", {}, {}),
        ("robustize", {}, {}),
        (
            "mad_robustize",
            {"epsilon": 0.5},
            {"mad_robustize_epsilon": 0.5},
        ),
        (
            "spherize",
            {"center": False, "method": "PCA", "epsilon": 0.25},
            {
                "spherize_center": False,
                "spherize_method": "PCA",
                "spherize_epsilon": 0.25,
            },
        ),
        (
            "inverse_normal",
            {"n_quantiles": 4},
            {"inverse_normal_n_quantiles": 4},
        ),
    ],
)
def test_save_normalize_transform_records_method_specific_params(
    method, extra_kwargs, expected_params
):
    """The joblib file should record method-specific fitting parameters for
    provenance/inspection, not just the fitted scaler."""
    if method == "standardize":
        scaler = StandardScaler(**extra_kwargs).fit(data_df)
    elif method == "robustize":
        scaler = RobustScaler(**extra_kwargs).fit(data_df)
    elif method == "mad_robustize":
        scaler = RobustMAD(**extra_kwargs).fit(data_df)
    elif method == "spherize":
        scaler = Spherize(**extra_kwargs).fit(data_df)
    elif method == "inverse_normal":
        scaler = InverseNormalTransform(**extra_kwargs).fit(data_df)

    transform_file = os.path.join(tmpdir, f"test_transform_io_params_{method}.joblib")
    save_normalize_transform(
        scaler=scaler, method=method, joblib_output_file=transform_file
    )

    payload = joblib.load(transform_file)
    assert payload["params"] == expected_params

    _, _, _, loaded_params = load_normalize_transform(transform_file)
    assert loaded_params == expected_params


def test_save_normalize_transform_does_not_create_parent_directory():
    """Pycytominer should not be opinionated about creating directories:
    saving to a path whose parent directory doesn't exist should raise
    (matching the underlying `joblib.dump` behavior) rather than silently
    creating it."""
    missing_parent = os.path.join(tmpdir, "does_not_exist_transform_io_dir")
    transform_file = os.path.join(missing_parent, "transform.joblib")

    assert not os.path.exists(missing_parent)

    scaler = _fitted_scaler("standardize")
    with pytest.raises(OSError):
        save_normalize_transform(
            scaler=scaler, method="standardize", joblib_output_file=transform_file
        )

    assert not os.path.exists(missing_parent)


def test_save_normalize_transform_does_not_require_or_append_joblib_suffix():
    """Pycytominer should not be opinionated about the output filename
    either: a path with no ".joblib" suffix should be written to, and read
    back from, exactly as given (no suffix appended)."""
    scaler = _fitted_scaler("standardize")
    no_suffix_path = os.path.join(tmpdir, "test_transform_io_no_suffix")

    save_normalize_transform(
        scaler=scaler, method="standardize", joblib_output_file=no_suffix_path
    )

    assert os.path.exists(no_suffix_path)
    assert not os.path.exists(no_suffix_path + ".joblib")

    loaded_scaler, loaded_method, loaded_features, _ = load_normalize_transform(
        no_suffix_path
    )
    assert loaded_method == "standardize"
    assert loaded_features == features
    np.testing.assert_allclose(
        loaded_scaler.transform(data_df), scaler.transform(data_df)
    )


def test_load_normalize_transform_missing_file_raises():
    missing_path = os.path.join(tmpdir, "does_not_exist_transform.joblib")
    assert not os.path.exists(missing_path)

    with pytest.raises(FileNotFoundError):
        load_normalize_transform(missing_path)


def test_load_normalize_transform_unsupported_method_raises():
    """A joblib file recording a method outside `AVAIL_METHODS` (e.g. from a
    future/foreign version of the file) should be rejected on load."""
    transform_file = os.path.join(tmpdir, "test_transform_io_bad_method.joblib")
    scaler = _fitted_scaler("standardize")

    joblib.dump(
        {"scaler": scaler, "method": "not_a_real_method", "params": {}},
        transform_file,
    )

    with pytest.raises(ValueError):
        load_normalize_transform(transform_file)


@pytest.mark.parametrize(
    ("method", "attr_by_param"),
    [
        ("mad_robustize", {"mad_robustize_epsilon": "epsilon"}),
        (
            "spherize",
            {
                "spherize_center": "center",
                "spherize_method": "method",
                "spherize_epsilon": "epsilon",
            },
        ),
        ("inverse_normal", {"inverse_normal_n_quantiles": "n_quantiles"}),
    ],
)
def test_stored_params_match_restored_scaler_attributes(method, attr_by_param):
    """The recorded params should agree with the parameters on the scaler
    restored from the same joblib file."""
    transform_file = os.path.join(tmpdir, f"test_transform_io_attrs_{method}.joblib")
    save_normalize_transform(
        scaler=_fitted_scaler(method),
        method=method,
        joblib_output_file=transform_file,
    )

    loaded_scaler, _, _, params = load_normalize_transform(transform_file)

    assert set(params) == set(attr_by_param)
    for param_name, attr in attr_by_param.items():
        assert params[param_name] == getattr(loaded_scaler, attr)


def test_load_normalize_transform_without_params_entry_returns_empty_dict():
    """Files lacking the optional "params" entry should still load."""
    transform_file = os.path.join(tmpdir, "test_transform_io_no_params.joblib")
    joblib.dump(
        {"scaler": _fitted_scaler("standardize"), "method": "standardize"},
        transform_file,
    )

    _, _, _, params = load_normalize_transform(transform_file)
    assert params == {}
