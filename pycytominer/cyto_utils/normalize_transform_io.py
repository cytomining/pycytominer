"""
Save and load the fitted parameters of a normalize() transform.

Fitted transforms are stored as a single ONNX file (at whatever path the
caller provides, with no suffix or parent-directory requirements imposed
here). ONNX stores plain data -- a small computation graph plus its weights --
rather than pickled Python objects, so loading a file never executes code. The
versions of the software that wrote a file are recorded, and loading raises an
error if they differ from the current environment, to keep results reproducible.
This lets a transform fit on one set of samples (e.g. controls) be reapplied
later to other subsets of data without re-fitting.

By convention, use the ".onnx" extension (e.g. "controls_standardize.onnx") so
that humans and agents encountering the file can tell what it is and that it
should be read back with `load_normalize_transform`. This is a suggestion only
and is not enforced.

What is stored, by method:

- "standardize", "robustize", "mad_robustize" and "spherize" are affine, so
  they are written as an ONNX graph (``Sub`` -> ``Div`` -> ``MatMul``, each
  step optional) that is run with ONNX Runtime when the transform is applied.
- "inverse_normal" cannot be expressed with standard ONNX operators (it needs
  the inverse normal CDF and per-feature interpolation, or data-dependent
  ranks), so its fitted values (the quantile landmarks, if any) are stored in
  the file as ordinary tensors next to an identity graph, and pycytominer
  rebuilds the estimator from them on load. This is still data-only: nothing
  in the file is executed.

The method, the feature order the transform was fit on, the method-specific
fitting parameters, and the Python, pandas, scikit-learn and pycytominer
versions in use at save time (for provenance) are stored in the model's
``metadata_props`` so the file is self-documenting (e.g. in a viewer such as
Netron).
"""

import json
import pathlib
import platform
from typing import Any, Union

import numpy as np
import pandas as pd

AVAIL_NORM_METHODS = [
    "standardize",
    "robustize",
    "mad_robustize",
    "spherize",
    "inverse_normal",
]

_METADATA_PREFIX = "pycytominer_"
_PRODUCER_NAME = "pycytominer"
# Smallest ONNX opset providing every operator used here (Sub, Div, MatMul, Identity)
_OPSET_VERSION = 13


class OnnxNormalizeTransform:
    """A fitted affine normalize() transform backed by an ONNX Runtime session.

    Mimics the small part of the sklearn transformer API that `normalize()`
    relies on: `transform()`, `feature_names_in_` and, for spherize, `columns`.

    Attributes
    ----------
    feature_names_in_ : numpy.ndarray
        The feature columns (in order) the transform was fit on.
    columns : pandas.Index
        Names of the output columns.
    """

    def __init__(
        self, model_bytes: bytes, feature_names: list[str], columns: list[str]
    ) -> None:
        import onnxruntime as ort

        self._session = ort.InferenceSession(
            model_bytes, providers=["CPUExecutionProvider"]
        )
        self._input_name = self._session.get_inputs()[0].name
        n_inputs = self._session.get_inputs()[0].shape[-1]
        if isinstance(n_inputs, int) and n_inputs != len(feature_names):
            raise ValueError(
                f"the graph takes {n_inputs} inputs but {len(feature_names)} "
                "features are recorded"
            )
        self.feature_names_in_ = np.asarray(feature_names, dtype=object)
        self.n_features_in_ = len(feature_names)
        self.columns = pd.Index(columns)

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        """Apply the transform.

        Parameters
        ----------
        X : pd.DataFrame
            Profiles with the same columns, in the same order, as at fit time.

        Returns
        -------
        numpy.ndarray
            The transformed values.
        """
        incoming = np.asarray(X.columns, dtype=object)
        if not np.array_equal(incoming, self.feature_names_in_):
            raise ValueError(
                "The feature names should match those that were passed during fit. "
                f"Features seen at fit time: {list(self.feature_names_in_)}. "
                f"Features seen now: {list(incoming)}."
            )
        values = X.to_numpy(dtype=np.float64, na_value=np.nan)
        return self._session.run(None, {self._input_name: values})[0]


def _affine_parts(scaler: Any, method: str) -> dict[str, Any]:
    """Pull the (offset, divisor, projection) arrays off a fitted affine scaler.

    Every affine method computes ``((X - offset) / divisor) @ projection`` where
    any of the three may be absent.
    """
    if method in ("standardize", "robustize"):
        center, scale = (
            (scaler.mean_, scaler.scale_)
            if method == "standardize"
            else (scaler.center_, scaler.scale_)
        )
        return {"offset": center, "divisor": scale, "projection": None}
    if method == "mad_robustize":
        return {
            "offset": scaler.median.to_numpy(),
            "divisor": (scaler.mad + scaler.epsilon).to_numpy(),
            "projection": None,
        }
    # spherize
    if scaler.method in ("PCA-cor", "ZCA-cor"):
        offset, divisor = scaler.standard_scaler.mean_, scaler.standard_scaler.scale_
    elif scaler.center:
        offset, divisor = scaler.mean_centerer.mean_, None
    else:
        offset, divisor = None, None
    return {"offset": offset, "divisor": divisor, "projection": scaler.W}


def _build_affine_graph(parts: dict[str, Any], n_features: int, n_outputs: int):
    """Build the ONNX graph for ``((X - offset) / divisor) @ projection``."""
    from onnx import TensorProto, helper, numpy_helper

    nodes, initializers = [], []
    current = "X"
    for step, op, key in (
        ("center", "Sub", "offset"),
        ("scale", "Div", "divisor"),
        ("project", "MatMul", "projection"),
    ):
        array = parts[key]
        if array is None:
            continue
        initializers.append(
            numpy_helper.from_array(np.asarray(array, dtype=np.float64), name=key)
        )
        nodes.append(helper.make_node(op, [current, key], [step], name=step))
        current = step
    if not nodes:
        nodes.append(helper.make_node("Identity", ["X"], ["Y"], name="identity"))
    else:
        nodes[-1].output[0] = "Y"

    return helper.make_graph(
        nodes,
        "pycytominer_normalize",
        [helper.make_tensor_value_info("X", TensorProto.DOUBLE, [None, n_features])],
        [helper.make_tensor_value_info("Y", TensorProto.DOUBLE, [None, n_outputs])],
        initializer=initializers,
    )


def _build_inverse_normal_graph(scaler: Any, n_features: int):
    """Store an inverse normal transform's fitted values next to an identity graph.

    The graph itself does not compute the transform (see the module docstring);
    the tensors are read back by `load_normalize_transform`.
    """
    from onnx import TensorProto, helper, numpy_helper

    initializers = []
    if scaler.method == "quantile":
        quantiles = scaler.transformer_
        initializers = [
            numpy_helper.from_array(
                np.asarray(quantiles.quantiles_, dtype=np.float64), name="quantiles"
            ),
            numpy_helper.from_array(
                np.asarray(quantiles.references_, dtype=np.float64), name="references"
            ),
        ]
    return helper.make_graph(
        [helper.make_node("Identity", ["X"], ["Y"], name="placeholder_identity")],
        "pycytominer_normalize_inverse_normal",
        [helper.make_tensor_value_info("X", TensorProto.DOUBLE, [None, n_features])],
        [helper.make_tensor_value_info("Y", TensorProto.DOUBLE, [None, n_features])],
        initializer=initializers,
        doc_string=(
            "Placeholder: the inverse normal transform is applied by pycytominer "
            "from the stored tensors and metadata, not by this graph."
        ),
    )


def _extract_normalize_scaler_params(scaler: Any, method: str) -> dict[str, Any]:
    """Pull the method-specific fitting parameters off a fitted scaler.

    These are already embedded in the graph itself (as weights), so they aren't
    needed to reapply the transform. They're recorded as their own metadata
    entry purely so the file is self-documenting: a user can see which
    parameters a saved transform was fit with without loading it.
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


def _inverse_normal_config(scaler: Any) -> dict[str, Any]:
    """The constructor arguments needed to rebuild an InverseNormalTransform."""
    random_state = scaler.random_state
    if random_state is not None and not isinstance(random_state, (int, np.integer)):
        raise ValueError(
            "inverse_normal_random_state must be an int or None to be saved, "
            f"not {type(random_state).__name__}."
        )
    config = {
        "n_quantiles": int(scaler.n_quantiles),
        "random_state": None if random_state is None else int(random_state),
        "method": scaler.method,
        "ties": scaler.ties,
    }
    if scaler.method == "quantile":
        config["n_quantiles_"] = int(scaler.n_quantiles_)
    return config


def _software_versions() -> dict[str, str]:
    """Versions of the software used to write a transform, for provenance only."""
    import sklearn

    from pycytominer.__about__ import __version__

    return {
        "python": platform.python_version(),
        "pandas": pd.__version__,
        "scikit-learn": sklearn.__version__,
        "pycytominer": __version__,
    }


def _check_software_versions(input_path: pathlib.Path, saved: Any) -> None:
    """Raise if the versions recorded at save time differ from this environment.

    Files without a versions entry (e.g. written before it was recorded) are not
    checked.
    """
    if saved is None:
        return
    current = _software_versions()
    if not isinstance(saved, dict):
        raise ValueError(
            f"'{input_path}' is not a valid pycytominer normalize transform: its "
            "versions metadata must be a mapping of package name to version."
        )
    mismatched = {
        name: (saved.get(name), current_version)
        for name, current_version in current.items()
        if saved.get(name) != current_version
    }
    if mismatched:
        details = "; ".join(
            f"{name}: saved with {was}, current {now}"
            for name, (was, now) in mismatched.items()
        )
        raise ValueError(
            f"'{input_path}' was saved in a different software environment ({details}). "
            "Recreate the original environment to reproduce its results."
        )


def _spherize_output_columns(scaler: Any, features: list[str]) -> list[str]:
    if scaler.method in ("PCA", "PCA-cor"):
        return [f"PC{i}" for i in range(1, len(features) + 1)]
    return features


def save_normalize_transform(
    scaler: Any, method: str, output_file: Union[str, pathlib.Path]
) -> Union[str, pathlib.Path]:
    """Save the fitted parameters of a normalize() scaler to disk as ONNX.

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
    output_file : str or pathlib.Path
        Where to write the transform. Pycytominer does not create any missing
        parent directories, and does not require or append an ".onnx" suffix.
        The suggested extension is ".onnx", e.g. "controls_standardize.onnx".

    Raises
    ------
    ValueError
        If `method` is not a supported method, if `scaler` was not fit on
        a pandas DataFrame (i.e. has no `feature_names_in_` attribute), or if
        an inverse normal transform's `random_state` is not an int or None.

    Examples
    --------
    >>> save_normalize_transform(
    ...     scaler, "standardize", "controls_standardize.onnx"
    ... )  # doctest: +SKIP
    """
    import onnx
    from onnx import helper

    method = method.lower()
    if method not in AVAIL_NORM_METHODS:
        raise ValueError(f"method '{method}' must be one of {AVAIL_NORM_METHODS}")

    if not hasattr(scaler, "feature_names_in_"):
        raise ValueError(
            "scaler must be fit on a DataFrame with named columns (not a bare "
            "array) so its feature names (scaler.feature_names_in_) are "
            "recorded before saving."
        )

    # feature_names_in_ is an object array that may hold non-str labels (ints,
    # np.str_); the names go into ONNX metadata as JSON, so they must be plain str.
    features = [str(name) for name in scaler.feature_names_in_]
    n_features = len(features)
    metadata = {
        "method": method,
        "features": features,
        "params": _extract_normalize_scaler_params(scaler, method),
        "versions": _software_versions(),
    }

    if method == "inverse_normal":
        metadata["config"] = _inverse_normal_config(scaler)
        graph = _build_inverse_normal_graph(scaler, n_features)
    else:
        parts = _affine_parts(scaler, method)
        if parts["projection"] is None:
            n_outputs = n_features
            metadata["columns"] = features
        else:
            n_outputs = parts["projection"].shape[1]
            metadata["columns"] = _spherize_output_columns(scaler, features)
        graph = _build_affine_graph(parts, n_features, n_outputs)

    model = helper.make_model(
        graph,
        producer_name=_PRODUCER_NAME,
        producer_version=metadata["versions"]["pycytominer"],
        opset_imports=[helper.make_opsetid("", _OPSET_VERSION)],
    )
    model.ir_version = 8
    for key, value in metadata.items():
        entry = model.metadata_props.add()
        entry.key = _METADATA_PREFIX + key
        entry.value = json.dumps(value)
    onnx.checker.check_model(model)

    with open(output_file, "wb") as handle:
        handle.write(model.SerializeToString())
    return output_file


def _load_inverse_normal(
    model: Any, metadata: dict[str, Any], features: list[str]
) -> Any:
    """Rebuild a fitted InverseNormalTransform from tensors and config."""
    from onnx import numpy_helper
    from sklearn.preprocessing import QuantileTransformer

    from pycytominer.operations.transform import InverseNormalTransform

    config = dict(metadata["config"])
    n_quantiles_ = config.pop("n_quantiles_", None)
    scaler = InverseNormalTransform(**config)
    scaler.feature_names_in_ = np.asarray(features, dtype=object)
    scaler.n_features_in_ = len(features)

    if scaler.method == "quantile":
        tensors = {t.name: numpy_helper.to_array(t) for t in model.graph.initializer}
        scaler.n_quantiles_ = n_quantiles_
        transformer = QuantileTransformer(
            n_quantiles=n_quantiles_,
            output_distribution="normal",
            random_state=scaler.random_state,
        )
        transformer.quantiles_ = tensors["quantiles"]
        transformer.references_ = tensors["references"]
        transformer.n_features_in_ = len(features)
        transformer.feature_names_in_ = scaler.feature_names_in_
        scaler.transformer_ = transformer
    return scaler


def load_normalize_transform(
    input_file: Union[str, pathlib.Path],
) -> tuple[Any, str, list[str], dict[str, Any]]:
    """Load a previously-saved normalize() transform from disk.

    Parameters
    ----------
    input_file : str or pathlib.Path
        Path to the saved transform, exactly as passed to
        `save_normalize_transform`.

    Returns
    -------
    scaler : fitted transform object
        An object with its fitted state restored, ready to call `.transform()`
        on a DataFrame with the saved features. For the affine methods this is
        an `OnnxNormalizeTransform` executed by ONNX Runtime; for
        "inverse_normal" it is a rebuilt `InverseNormalTransform`.
    method : str
        The normalization method the scaler was fit with.
    features : list of str
        The feature columns (in order) the scaler expects as input.
    params : dict
        The method-specific fitting parameters recorded for provenance (see
        `_extract_normalize_scaler_params`). Not required to reapply the transform.

    Raises
    ------
    FileNotFoundError
        If `input_file` does not exist.
    ValueError
        If the file cannot be parsed as ONNX, is not a pycytominer normalize
        transform, records a method outside `AVAIL_NORM_METHODS`, carries a
        malformed payload (bad features, missing tensors or columns), or was
        saved with different Python, pandas, scikit-learn or pycytominer
        versions than the current environment.

    Examples
    --------
    >>> scaler, method, features, params = load_normalize_transform(
    ...     "controls_standardize.onnx"
    ... )  # doctest: +SKIP
    """
    import onnx

    input_path = pathlib.Path(input_file)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Could not find '{input_path}', as written by save_normalize_transform()."
        )

    not_a_transform = f"'{input_path}' is not a valid pycytominer normalize transform"

    try:
        model = onnx.load(str(input_path))
        onnx.checker.check_model(model)
        metadata = {
            entry.key[len(_METADATA_PREFIX) :]: json.loads(entry.value)
            for entry in model.metadata_props
            if entry.key.startswith(_METADATA_PREFIX)
        }
    except Exception as exc:
        raise ValueError(f"{not_a_transform}: it could not be parsed ({exc}).") from exc

    if "method" not in metadata or "features" not in metadata:
        raise ValueError(
            f"'{input_path}' is not a pycytominer normalize transform: its "
            f"'{_METADATA_PREFIX}method'/'{_METADATA_PREFIX}features' metadata "
            "is missing."
        )

    _check_software_versions(input_path, metadata.get("versions"))

    method = metadata["method"]
    if method not in AVAIL_NORM_METHODS:
        raise ValueError(f"Cannot load transform for unsupported method '{method}'")

    features = metadata["features"]
    if (
        not isinstance(features, list)
        or not features
        or not all(isinstance(f, str) for f in features)
    ):
        raise ValueError(
            f"{not_a_transform}: its features metadata must be a non-empty list "
            "of strings."
        )
    params = metadata.get("params", {})

    try:
        if method == "inverse_normal":
            scaler = _load_inverse_normal(model, metadata, features)
        else:
            scaler = OnnxNormalizeTransform(
                model.SerializeToString(), features, metadata["columns"]
            )
    except Exception as exc:
        raise ValueError(
            f"{not_a_transform}: its '{method}' payload is incomplete or "
            f"inconsistent ({type(exc).__name__}: {exc})."
        ) from exc

    return scaler, method, features, params
