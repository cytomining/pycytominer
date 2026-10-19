"""
Save and load the fitted parameters of a normalize() transform.

Fitted transforms are stored as a single joblib file (at whatever path the
caller provides, with no suffix or parent-directory requirements imposed
here) containing the fitted scaler object, the normalization method, and
(for provenance/inspection) the method-specific fitting parameters it was
fit with. This lets a transform fit on one set of samples (e.g. controls)
be reapplied later to other subsets of data without re-fitting.

The feature order a scaler was fit on is not duplicated in this file: every
scaler pycytominer's normalize() can produce is fit on a pandas DataFrame,
which sklearn (or, for pycytominer's own estimators, `pycytominer.operations
.transform`) records on the fitted object itself as `feature_names_in_`.
That attribute is the single source of truth `load_normalize_transform`
reads back, instead of pycytominer keeping its own separate copy.
"""

import pathlib
from typing import Union

AVAIL_METHODS = [
    "standardize",
    "robustize",
    "mad_robustize",
    "spherize",
    "inverse_normal",
]


def _extract_normalize_scaler_params(scaler: any, method: str) -> dict[str, any]:
    """Pull the method-specific fitting parameters off a fitted scaler.

    These are already embedded in the pickled scaler object itself (they're
    ordinary constructor attributes), so they aren't needed to reapply the
    transform. They're recorded as their own payload entry purely so the
    joblib file is self-documenting: a user can inspect which parameters a
    saved transform was fit with without unpickling and introspecting the
    scaler.
    """
    if method == "mad_robustize":
        return {"mad_robustize_epsilon": scaler.epsilon}
    if method == "spherize":
        return {
            "spherize_center": scaler.center,
            "spherize_method": scaler.method,
            "spherize_epsilon": scaler.epsilon,
        }
    if method == "inverse_normal":
        return {"inverse_normal_n_quantiles": scaler.n_quantiles}
    return {}


def save_normalize_transform(
    scaler: any, method: str, joblib_output_file: Union[str, pathlib.Path]
) -> Union[str, pathlib.Path]:
    """Save the fitted parameters of a normalize() scaler to disk.

    Parameters
    ----------
    scaler : fitted scaler object
        The fitted StandardScaler, RobustScaler, RobustMAD, Spherize, or
        InverseNormalTransform instance produced by the fitting step of
        `pycytominer.normalize`. Must have been fit on a pandas DataFrame,
        so that it carries a `feature_names_in_` attribute.
    method : str
        The normalization method used to fit `scaler`. One of "standardize",
        "robustize", "mad_robustize", "spherize", "inverse_normal".
    joblib_output_file : str or pathlib.Path
        Where to write the transform, passed through as-is to
        `joblib.dump`. Pycytominer does not create any missing parent
        directories, and does not require or append a ".joblib" suffix;
        both match the behavior of the underlying `joblib.dump` call.

    Raises
    ------
    ValueError
        If `method` is not a supported method, or if `scaler` was not fit on
        a pandas DataFrame (i.e. has no `feature_names_in_` attribute).
    """
    import joblib

    method = method.lower()
    if method not in AVAIL_METHODS:
        raise ValueError(f"method '{method}' must be one of {AVAIL_METHODS}")

    if not hasattr(scaler, "feature_names_in_"):
        raise ValueError(
            "scaler must be fit on a pandas DataFrame so its feature names "
            "(scaler.feature_names_in_) are recorded before saving."
        )

    joblib.dump(
        {
            "scaler": scaler,
            "method": method,
            "params": _extract_params(scaler, method),
        },
        joblib_output_file,
    )
    return joblib_output_file


def load_normalize_transform(
    joblib_input_file: Union[str, pathlib.Path],
) -> tuple[any, str, list[str], dict[str, any]]:
    """Load a previously-saved normalize() transform from disk.

    Parameters
    ----------
    joblib_input_file : str or pathlib.Path
        Path to the saved transform, exactly as passed to
        `save_normalize_transform`.

    Returns
    -------
    scaler : fitted scaler object
        A StandardScaler, RobustScaler, RobustMAD, Spherize, or
        InverseNormalTransform instance with its fitted state restored,
        ready to call `.transform()`.
    method : str
        The normalization method the scaler was fit with.
    features : list of str
        The feature columns (in order) the scaler expects as input, read
        from `scaler.feature_names_in_`.
    params : dict
        The method-specific fitting parameters recorded for provenance (see
        `_extract_params`). Not required to reapply the transform.

    Raises
    ------
    FileNotFoundError
        If `joblib_input_file` does not exist.
    ValueError
        If the loaded payload records a method outside `AVAIL_METHODS`.
    """
    import joblib

    input_path = pathlib.Path(joblib_input_file)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Could not find '{input_path}', as written by save_normalize_transform()."
        )

    payload = joblib.load(input_path)
    method = payload["method"]

    if method not in AVAIL_METHODS:
        raise ValueError(f"Cannot load transform for unsupported method '{method}'")

    scaler = payload["scaler"]
    features = list(scaler.feature_names_in_)
    params = payload.get("params", {})

    return scaler, method, features, params
