"""
Save and load the fitted parameters of a normalize() transform.

Fitted transforms are stored as a single joblib file containing the fitted
scaler object alongside the normalization method and feature order it was
fit on. This lets a transform fit on one set of samples (e.g. controls) be
reapplied later to other subsets of data without re-fitting.
"""

import pathlib

import joblib

avail_methods = ["standardize", "robustize", "mad_robustize", "spherize"]


def _joblib_path(file):
    """Derive the ".joblib" path from a single user-provided path."""
    file = pathlib.Path(file)
    if file.suffix.lower() != ".joblib":
        file = file.with_suffix(".joblib")
    return file


def save_normalize_transform(scaler, method, features, output_file):
    """Save the fitted parameters of a normalize() scaler to disk.

    Parameters
    ----------
    scaler : fitted scaler object
        The fitted StandardScaler, RobustScaler, RobustMAD, or Spherize
        instance produced by the fitting step of `pycytominer.normalize`.
    method : str
        The normalization method used to fit `scaler`. One of "standardize",
        "robustize", "mad_robustize", "spherize".
    features : list of str
        The feature columns (in order) that the scaler was fit on.
    output_file : str or pathlib.Path
        Where to write the transform. Written as a joblib file with a
        ".joblib" suffix, containing the fitted scaler, method name, and
        feature order.
    """
    method = method.lower()
    if method not in avail_methods:
        raise ValueError(f"method must be one of {avail_methods}")

    output_path = _joblib_path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(
        {"scaler": scaler, "method": method, "features": list(features)},
        output_path,
    )


def load_normalize_transform(input_file):
    """Load a previously-saved normalize() transform from disk.

    Parameters
    ----------
    input_file : str or pathlib.Path
        Path to the saved transform, as passed to `save_normalize_transform`
        (either the ".joblib" file or a path missing that suffix).

    Returns
    -------
    scaler : fitted scaler object
        A StandardScaler, RobustScaler, RobustMAD, or Spherize instance with
        its fitted state restored, ready to call `.transform()`.
    method : str
        The normalization method the scaler was fit with.
    features : list of str
        The feature columns (in order) the scaler expects as input.
    """
    input_path = _joblib_path(input_file)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Could not find '{input_path}', as written by save_normalize_transform()."
        )

    payload = joblib.load(input_path)
    method = payload["method"]

    if method not in avail_methods:
        raise ValueError(f"Cannot load transform for unsupported method '{method}'")

    return payload["scaler"], method, payload["features"]
