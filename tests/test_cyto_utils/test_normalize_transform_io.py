import json
import os
import tempfile

import numpy as np
import onnx
import pandas as pd
import pytest
from sklearn.preprocessing import RobustScaler, StandardScaler

from pycytominer.cyto_utils.normalize_transform_io import (
    AVAIL_NORM_METHODS,
    OnnxNormalizeTransform,
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


def _metadata(path):
    return {e.key: e.value for e in onnx.load(path).metadata_props}


@pytest.mark.parametrize("method", AVAIL_NORM_METHODS)
def test_save_and_load_roundtrip(method):
    """Saving and loading a fitted scaler should preserve the method, the
    feature order, and the ability to reproduce the original transform()
    output."""
    transform_file = os.path.join(tmpdir, f"test_transform_io_{method}.onnx")
    scaler = _fitted_scaler(method)
    expected = scaler.transform(data_df)

    save_normalize_transform(scaler=scaler, method=method, output_file=transform_file)

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
    transform_file = os.path.join(tmpdir, "test_transform_io_uppercase_method.onnx")

    save_normalize_transform(
        scaler=scaler, method="STANDARDIZE", output_file=transform_file
    )

    _, loaded_method, _, _ = load_normalize_transform(transform_file)
    assert loaded_method == "standardize"


def test_save_normalize_transform_invalid_method_raises():
    scaler = _fitted_scaler("standardize")
    transform_file = os.path.join(tmpdir, "test_transform_io_invalid_method.onnx")

    with pytest.raises(ValueError):
        save_normalize_transform(
            scaler=scaler, method="not_a_real_method", output_file=transform_file
        )

    assert not os.path.exists(transform_file)


def test_save_normalize_transform_requires_feature_names_in():
    """A scaler fit on a bare numpy array (no column names) has no
    `feature_names_in_`, and can't be saved since features can't be
    recovered from it on load."""
    scaler = StandardScaler().fit(data_df.to_numpy())
    transform_file = os.path.join(tmpdir, "test_transform_io_no_feature_names.onnx")

    with pytest.raises(ValueError):
        save_normalize_transform(
            scaler=scaler, method="standardize", output_file=transform_file
        )

    assert not os.path.exists(transform_file)


def test_save_normalize_transform_numpy_error_message_is_not_pandas_specific():
    """The error should point at missing column names, not demand pandas."""
    scaler = StandardScaler().fit(data_df.to_numpy())

    with pytest.raises(ValueError, match="named columns"):
        save_normalize_transform(
            scaler=scaler,
            method="standardize",
            output_file=os.path.join(tmpdir, "test_transform_io_numpy_message.onnx"),
        )


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
    """The ONNX file should record method-specific fitting parameters in its
    metadata for provenance/inspection, not just the fitted weights."""
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

    transform_file = os.path.join(tmpdir, f"test_transform_io_params_{method}.onnx")
    save_normalize_transform(scaler=scaler, method=method, output_file=transform_file)

    assert json.loads(_metadata(transform_file)["pycytominer_params"]) == (
        expected_params
    )

    _, _, _, loaded_params = load_normalize_transform(transform_file)
    assert loaded_params == expected_params


def test_save_normalize_transform_does_not_create_parent_directory():
    """Pycytominer should not be opinionated about creating directories:
    saving to a path whose parent directory doesn't exist should raise
    (matching the underlying `joblib.dump` behavior) rather than silently
    creating it."""
    missing_parent = os.path.join(tmpdir, "does_not_exist_transform_io_dir")
    transform_file = os.path.join(missing_parent, "transform.onnx")

    assert not os.path.exists(missing_parent)

    scaler = _fitted_scaler("standardize")
    with pytest.raises(OSError):
        save_normalize_transform(
            scaler=scaler, method="standardize", output_file=transform_file
        )

    assert not os.path.exists(missing_parent)


def test_save_normalize_transform_does_not_require_or_append_joblib_suffix():
    """Pycytominer should not be opinionated about the output filename
    either: a path with no ".onnx" suffix should be written to, and read
    back from, exactly as given (no suffix appended)."""
    scaler = _fitted_scaler("standardize")
    no_suffix_path = os.path.join(tmpdir, "test_transform_io_no_suffix")

    save_normalize_transform(
        scaler=scaler, method="standardize", output_file=no_suffix_path
    )

    assert os.path.exists(no_suffix_path)
    assert not os.path.exists(no_suffix_path + ".onnx")

    loaded_scaler, loaded_method, loaded_features, _ = load_normalize_transform(
        no_suffix_path
    )
    assert loaded_method == "standardize"
    assert loaded_features == features
    np.testing.assert_allclose(
        loaded_scaler.transform(data_df), scaler.transform(data_df)
    )


def test_load_normalize_transform_missing_file_raises():
    missing_path = os.path.join(tmpdir, "does_not_exist_transform.onnx")
    assert not os.path.exists(missing_path)

    with pytest.raises(FileNotFoundError):
        load_normalize_transform(missing_path)


def test_load_normalize_transform_unsupported_method_raises():
    """A file recording a method outside `AVAIL_NORM_METHODS` (e.g. from a
    future/foreign version of the file) should be rejected on load."""
    transform_file = os.path.join(tmpdir, "test_transform_io_bad_method.onnx")
    save_normalize_transform(
        scaler=_fitted_scaler("standardize"),
        method="standardize",
        output_file=transform_file,
    )
    model = onnx.load(transform_file)
    for entry in model.metadata_props:
        if entry.key == "pycytominer_method":
            entry.value = json.dumps("not_a_real_method")
    onnx.save(model, transform_file)

    with pytest.raises(ValueError, match="unsupported method"):
        load_normalize_transform(transform_file)


def test_load_normalize_transform_rejects_foreign_onnx_file():
    """An ONNX file lacking pycytominer's metadata should be rejected."""
    transform_file = os.path.join(tmpdir, "test_transform_io_foreign.onnx")
    save_normalize_transform(
        scaler=_fitted_scaler("standardize"),
        method="standardize",
        output_file=transform_file,
    )
    model = onnx.load(transform_file)
    del model.metadata_props[:]
    onnx.save(model, transform_file)

    with pytest.raises(ValueError, match="not a pycytominer normalize transform"):
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
def test_stored_params_match_original_scaler_attributes(method, attr_by_param):
    """The recorded params should agree with the fitted scaler that was saved."""
    transform_file = os.path.join(tmpdir, f"test_transform_io_attrs_{method}.onnx")
    scaler = _fitted_scaler(method)
    save_normalize_transform(scaler=scaler, method=method, output_file=transform_file)

    _, _, _, params = load_normalize_transform(transform_file)

    assert set(params) == set(attr_by_param)
    for param_name, attr in attr_by_param.items():
        assert params[param_name] == getattr(scaler, attr)


@pytest.mark.parametrize(
    ("inverse_normal_kwargs", "n_rows"),
    [
        ({"method": "blom"}, 8),
        ({"method": "tukey", "ties": "random", "random_state": 3}, 8),
        ({"method": "quantile", "n_quantiles": 1000}, 8),
    ],
)
def test_inverse_normal_variants_roundtrip(inverse_normal_kwargs, n_rows):
    """Every inverse normal variant (rank-based, random ties, quantile) should
    reproduce its transform() output after a save/load cycle."""
    transform_file = os.path.join(tmpdir, "test_transform_io_inverse_normal_var.onnx")
    scaler = InverseNormalTransform(**inverse_normal_kwargs).fit(data_df)
    save_normalize_transform(
        scaler=scaler, method="inverse_normal", output_file=transform_file
    )

    loaded, _, _, _ = load_normalize_transform(transform_file)

    np.testing.assert_allclose(
        loaded.transform(data_df), scaler.transform(data_df), equal_nan=True
    )


def test_inverse_normal_random_state_instance_cannot_be_saved():
    transform_file = os.path.join(tmpdir, "test_transform_io_rs_instance.onnx")
    scaler = InverseNormalTransform(
        method="blom", ties="random", random_state=np.random.RandomState(0)
    ).fit(data_df)

    with pytest.raises(ValueError, match="random_state"):
        save_normalize_transform(
            scaler=scaler, method="inverse_normal", output_file=transform_file
        )


def test_loaded_affine_transform_rejects_mismatched_features():
    transform_file = os.path.join(tmpdir, "test_transform_io_mismatch.onnx")
    save_normalize_transform(
        scaler=_fitted_scaler("standardize"),
        method="standardize",
        output_file=transform_file,
    )
    loaded, _, _, _ = load_normalize_transform(transform_file)
    assert isinstance(loaded, OnnxNormalizeTransform)

    with pytest.raises(ValueError, match="feature names should match"):
        loaded.transform(data_df.rename(columns={"x": "renamed"}))


def test_loaded_transform_handles_missing_values():
    transform_file = os.path.join(tmpdir, "test_transform_io_nan.onnx")
    scaler = _fitted_scaler("standardize")
    save_normalize_transform(
        scaler=scaler, method="standardize", output_file=transform_file
    )
    loaded, _, _, _ = load_normalize_transform(transform_file)

    with_nan = data_df.copy()
    with_nan.iloc[0, 0] = np.nan
    np.testing.assert_allclose(
        loaded.transform(with_nan), scaler.transform(with_nan), equal_nan=True
    )
