"""
Normalize observation features based on specified normalization method
"""

from typing import Any, Literal, Optional, Union

import numpy as np
import pandas as pd
from sklearn.preprocessing import RobustScaler, StandardScaler

from pycytominer.cyto_utils.features import infer_cp_features
from pycytominer.cyto_utils.load import load_profiles
from pycytominer.cyto_utils.normalize_transform_io import (
    AVAIL_NORM_METHODS,
    load_normalize_transform,
    save_normalize_transform,
)
from pycytominer.cyto_utils.util import write_to_file_if_user_specifies_output_details
from pycytominer.operations import InverseNormalTransform, RobustMAD, Spherize
from pycytominer.operations.transform import _RANKIT_CONSTANTS


@write_to_file_if_user_specifies_output_details
def normalize(
    profiles: Union[str, pd.DataFrame],
    features: Union[str, list[str]] = "infer",
    image_features: bool = False,
    meta_features: Union[str, list[str]] = "infer",
    samples: str = "all",
    method: str = "standardize",
    drop_cosmicqc_rows: bool = False,
    output_file: Optional[str] = None,
    output_type: Optional[
        Literal["csv", "parquet", "anndata_h5ad", "anndata_zarr"]
    ] = "csv",
    compression_options: Optional[Union[str, dict[str, Any]]] = None,
    float_format: Optional[str] = None,
    mad_robustize_epsilon: Optional[float] = 1e-18,
    spherize_center: bool = True,
    spherize_method: str = "ZCA-cor",
    spherize_epsilon: float = 1e-6,
    inverse_normal_n_quantiles: int = 1000,
    inverse_normal_method: str = "quantile",
    inverse_normal_ties: str = "average",
    inverse_normal_random_state: Optional[int] = None,
    transform_output_file: Optional[str] = None,
    fitted_transform_file: Optional[str] = None,
) -> pd.DataFrame:
    """Normalize profiling features

    Parameters
    ----------
    profiles : pd.DataFrame or path
        Either a pandas DataFrame or a file that stores profile data
    features : list
        A list of strings corresponding to feature measurement column names in the
        `profiles` DataFrame. All features listed must be found in `profiles`.
        Defaults to "infer". If "infer", then assume features are from CellProfiler output and
        prefixed with "Cells", "Nuclei", or "Cytoplasm". Selected feature columns
        must be numeric. Missing values are allowed as long as the column
        remains numeric. As a temporary compatibility measure, Pycytominer
        also treats common missing-value strings such as ``"nan"`` and
        ``"None"`` as missing values in selected feature columns before
        numeric validation. If you are working with mixed profile and image
        payload data, pass explicit feature columns when needed to avoid
        selecting non-profile content.
    image_features : bool, default False
        Whether to include inferred ``Image_*`` feature columns alongside the
        default CellProfiler compartments. This preserves support for numeric
        image-level measurements while avoiding non-numeric ``Image_*``
        payload columns from OME-Arrow-backed or similarly mixed tables.
        Non-normalized image payload columns are preserved in the output.
    meta_features : list
        A list of strings corresponding to metadata column names in the `profiles`
        DataFrame. All features listed must be found in `profiles`. Defaults to "infer".
        If "infer", then assume CellProfiler metadata features, identified by
        column names that begin with the `Metadata_` prefix.
    samples : str
        The metadata column values to use as a normalization reference. We often use
        control samples. The function uses a pd.query() function, so you should
        structure samples in this fashion. An example is
        "Metadata_treatment == 'control'" (include all quotes). Defaults to "all".
    method : str
        How to normalize the dataframe. Defaults to "standardize". Check AVAIL_NORM_METHODS
        for available normalization methods.
    drop_cosmicqc_rows : bool
        Whether to drop rows that are flagged as QC failures. The function looks for columns
        from coSMicQC with "Metadata_cqc_" prefix and drop rows with True. Defaults to False.
        Suggested use after a prior call to `pycytominer.annotate(external_metadata=qc.parquet)`.
    output_file : str, optional
        If provided, will write normalized profiles to file. If not specified, will
        return the normalized profiles as output. We recommend that this output file be
        suffixed with "_normalized.csv".
    output_type : str, optional
        If provided, will write normalized profiles as a specified file type ("csv", "parquet", "anndata_h5ad", or "anndata_zarr").
        If not specified and output_file is provided, then the file will be output as CSV by default.
    compression_options : str or dict, optional
        Contains compression options as input to
        pd.DataFrame.to_csv(compression=compression_options). pandas version >= 1.2.
    float_format : str, optional
        Decimal precision to use in writing output file as input to
        pd.DataFrame.to_csv(float_format=float_format). For example, use "%.3g" for 3
        decimal precision.
    mad_robustize_epsilon : float, optional
        The mad_robustize fudge factor parameter. The function only uses
        this variable if method = "mad_robustize". Set this to 0 if
        mad_robustize generates features with large values.
    spherize_center : bool
        If the function should center data before sphering (aka whitening). The
        function only uses this variable if method = "spherize". Defaults to True.
    spherize_method : str
        The sphering (aka whitening) normalization selection. The function only uses
        this variable if method = "spherize". Defaults to "ZCA-cor". See
        :py:func:`pycytominer.operations.transform` for available spherize methods.
    spherize_epsilon : float, default 1e-6.
        The sphering (aka whitening) fudge factor parameter. The function only uses
        this variable if method = "spherize".
    inverse_normal_n_quantiles : int, default=1000
        Number of cumulative distribution function landmarks used for the inverse
        normal transformation. Values larger than the number of samples are capped
        at the number of samples. Only used when ``method="inverse_normal"``.
    inverse_normal_method : {"quantile", "blom", "tukey", "van_der_waerden", "hazen"}, default="quantile"
        How values are mapped to normal scores. "quantile" interpolates between
        ``inverse_normal_n_quantiles`` landmarks (sklearn's QuantileTransformer).
        The other four options use exact ranks (a "rank-based inverse normal
        transformation"), differing only in which published plotting-position
        constant they use: "blom" (Blom, 1958) is a good general-purpose default
        and is what the JUMP profiling recipe uses; "tukey" is a close
        alternative, rarely distinguishable from "blom" in practice; "hazen"
        (also called "rankit") is the simplest and oldest formula, more
        common in engineering/hydrology than profiling work; "van_der_waerden"
        matches the scores used by the van der Waerden normal-scores test.
        Reach for "blom" unless you have a specific reason to match one of the
        others (e.g. reproducing a paper or tool that uses a different
        convention) — see :py:class:`pycytominer.operations.InverseNormalTransform`
        for more detail on choosing between them. With any of the four, the
        ranks are computed over all rows, so ``samples`` can never affect the
        result; a ``ValueError`` is raised unless ``samples="all"``. Only used
        when ``method="inverse_normal"``.
    inverse_normal_ties : {"average", "random"}, default="average"
        How tied values are ranked when ``inverse_normal_method`` is rank-based
        (``"blom"``, ``"tukey"``, ``"van_der_waerden"``, or ``"hazen"``): "average"
        gives tied values the same score, "random" breaks ties with a random order
        so that every value gets a distinct score.
    inverse_normal_random_state : int, optional
        Seed for the random tie-breaking (``inverse_normal_ties="random"``).
    transform_output_file : str, optional
        If provided, save the fitted transform to this path so it can be
        reapplied later (e.g. to other subsets of data) via
        `fitted_transform_file`. The transform is written as a single joblib
        file at exactly this path (no suffix is required or appended).
        Mutually exclusive with `fitted_transform_file`: since that skips
        fitting entirely, providing both raises a ``ValueError``.
        Pycytominer does not create missing parent directories for this
        path; the directory must already exist.
    fitted_transform_file : str, optional
        If provided, skip fitting a new transform and instead load a previously
        saved transform (written by a prior call using `transform_output_file`)
        and apply it directly to `profiles`. The `samples` argument and the
        method-specific fitting parameters (`mad_robustize_epsilon`,
        `spherize_center`, `spherize_method`, `spherize_epsilon`,
        `inverse_normal_n_quantiles`, `inverse_normal_method`, `inverse_normal_ties`,
        `inverse_normal_random_state`) are ignored, since they are embedded in
        the loaded, already-fitted scaler. `method` must equal the method
        the loaded transform was fit with (including when left at its
        "standardize" default), or a ``ValueError`` is raised. `features`,
        if not "infer", must contain the same feature columns the transform
        was fit on (any order). If "infer", the transform's saved features are
        used. Features are always reordered to match the order the transform
        expects.

    Returns
    -------
    pd.DataFrame
        DataFrame of normalized features. if output_file=None, then return the
        DataFrame. If you specify output_file, profiles will be written on disk
        based on provided output_file path.

    Raises
    ------
    ValueError
        Raised when inferred or manually selected feature columns are non-numeric,
        because Pycytominer normalization methods operate on numeric features
        only. In that case, select numeric features explicitly before calling
        ``normalize()``, for example by passing a curated feature list or by
        running ``feature_select()`` first. Also raised when ``method`` is
        ``"inverse_normal"`` and ``inverse_normal_method`` is not one of the
        available methods, and when ``samples`` is not
        ``"all"`` and ``inverse_normal_method`` is a rank-based method
        (``"blom"``, ``"tukey"``, ``"van_der_waerden"``, or ``"hazen"``), since
        those methods rank every row and ``samples`` can never affect the
        result.

        Also raised when both `transform_output_file` and `fitted_transform_file`
        are given; when ``drop_cosmicqc_rows=True`` and there are no
        ``Metadata_cqc_`` columns, or every row is dropped by the QC filter; when
        `features` or `meta_features` is a single string; when
        ``inverse_normal_ties`` is not "average" or "random"; when `features` and
        `meta_features` overlap; when `method` or `features` do not match the
        loaded `fitted_transform_file`, or its features are missing from
        `profiles`; and when ``mad_robustize_epsilon`` is None while fitting a
        new ``mad_robustize`` transform.

    Notes
    -----
    Parameters: `output_file`, `output_type`, `compression_options`, and `float_format`
    are passed as kwargs to the `write_to_file_if_user_specifies_output_details` decorator,
    which handles writing the output DataFrame to file if the user specifies output
    details. If `output_file` is not specified, the function will return the normalized
    DataFrame instead of writing to file.

    Examples
    --------
    .. code-block:: python

        import pandas as pd
        from pycytominer import normalize

        data_df = pd.DataFrame(
            {
                "Metadata_plate": ["a", "a", "a", "a", "b", "b", "b", "b"],
                "Metadata_treatment": [
                    "drug",
                    "drug",
                    "control",
                    "control",
                    "drug",
                    "drug",
                    "control",
                    "control",
                ],
                "x": [1, 2, 8, 2, 5, 5, 5, 1],
                "y": [3, 1, 7, 4, 5, 9, 6, 1],
                "z": [1, 8, 2, 5, 6, 22, 2, 2],
                "zz": [14, 46, 1, 6, 30, 100, 2, 2],
            }
        ).reset_index(drop=True)

        normalized_df = normalize(
            profiles=data_df,
            features=["x", "y", "z", "zz"],
            meta_features=["Metadata_plate", "Metadata_treatment"],
            samples="Metadata_treatment == 'control'",
            method="standardize",
        )
    """
    # check for clashes in the args
    if fitted_transform_file is not None and transform_output_file is not None:
        raise ValueError(
            "Only one of `fitted_transform_file` or `transform_output_file` should be specified."
        )

    # Load Data
    # Copy so the cleanup below never modifies the caller's DataFrame
    profiles = load_profiles(profiles).copy()

    # If drop_cosmicqc_rows is True, drop rows that are flagged (True) as QC failures from coSMicQC
    if drop_cosmicqc_rows:
        qc_columns = [
            col for col in profiles.columns if col.startswith("Metadata_cqc_")
        ]
        if qc_columns:
            passed_qc = ~profiles[qc_columns].any(axis="columns")
            profiles = profiles[passed_qc].copy()
            if profiles.empty:
                raise ValueError(
                    "All rows were dropped after QC filtering; cannot normalize an empty dataset."
                )
        else:
            raise ValueError(
                "No QC columns found with prefix 'Metadata_cqc_'. Cannot drop QC rows."
            )

    method = method.lower()
    inverse_normal_method = inverse_normal_method.lower()
    inverse_normal_ties = inverse_normal_ties.lower()

    if method not in AVAIL_NORM_METHODS:
        raise ValueError(f"method must be one of {AVAIL_NORM_METHODS}, not {method!r}")

    avail_inverse_normal_methods = ["quantile", *_RANKIT_CONSTANTS]
    if method == "inverse_normal" and inverse_normal_method not in (
        avail_inverse_normal_methods
    ):
        raise ValueError(
            f"inverse_normal_method must be one of {avail_inverse_normal_methods}, "
            f"not {inverse_normal_method!r}"
        )

    if method == "inverse_normal" and inverse_normal_ties not in ("average", "random"):
        raise ValueError(
            f"inverse_normal_ties must be 'average' or 'random', not {inverse_normal_ties!r}"
        )

    if (
        fitted_transform_file is None
        and method == "inverse_normal"
        and inverse_normal_method in _RANKIT_CONSTANTS
        and samples != "all"
    ):
        raise ValueError(
            f"inverse_normal_method={inverse_normal_method!r} ranks the values of all "
            f"rows and ignores `samples` entirely, so samples={samples!r} can never "
            'affect the result. Use samples="all" (the default) instead.'
        )

    if method == "standardize":
        scaler = StandardScaler()
    elif method == "robustize":
        scaler = RobustScaler()
    elif method == "mad_robustize":
        if fitted_transform_file is None and mad_robustize_epsilon is None:
            raise ValueError("mad_robustize_epsilon must be a float")
        # epsilon is None only when a fitted transform is loaded below, which
        # replaces this scaler, so the default is never used for fitting
        scaler = (
            RobustMAD()
            if mad_robustize_epsilon is None
            else RobustMAD(epsilon=mad_robustize_epsilon)
        )
    elif method == "spherize":
        scaler = Spherize(
            center=spherize_center,
            method=spherize_method,
            epsilon=spherize_epsilon,
            return_numpy=True,
        )
    else:
        scaler = InverseNormalTransform(
            n_quantiles=inverse_normal_n_quantiles,
            random_state=inverse_normal_random_state,
            method=inverse_normal_method,
            ties=inverse_normal_ties,
        )

    if isinstance(features, str) and features != "infer":
        raise ValueError("features must be a list of strings, not a single string")

    if fitted_transform_file is not None:
        # Load a previously-fit transform and apply it directly, skipping
        # `samples`/method-specific fitting parameters entirely
        fitted_scaler, loaded_method, saved_features, _ = load_normalize_transform(
            fitted_transform_file
        )

        # check that method matches the loaded transform
        if method != loaded_method:
            raise ValueError(
                f"The specified method '{method}' does not match the method '{loaded_method}' "
                f"used to fit the transform loaded from `fitted_transform_file`."
            )

        if features != "infer" and set(features) != set(saved_features):
            raise ValueError(
                "The provided `features` do not match the features used to fit "
                "the transform loaded from `fitted_transform_file`."
            )

        # Use the order the transform was fit on: the fitted scaler applies
        # positionally, so the saved order must win
        features = list(saved_features)

        missing_features = [f for f in features if f not in profiles.columns]
        if missing_features:
            raise ValueError(
                f"Features used to fit the transform loaded from `fitted_transform_file` "
                f"are missing from `profiles`: {missing_features}"
            )

    elif features == "infer":
        features = infer_cp_features(profiles, image_features=image_features)

    # Temporary compatibility path for accommodating CellProfiler-style imports
    # that encode
    # missing feature values as strings such as "nan" or "None". Only
    # missing-value-like strings are coerced; other non-numeric content still
    # fails validation below. String matching is case-insensitive after
    # whitespace stripping and lowercasing.
    missing_string_tokens = {"", "na", "n/a", "nan", "none", "null"}
    for feature in features:
        # Already-numeric feature columns do not need compatibility cleanup.
        if pd.api.types.is_numeric_dtype(profiles[feature]):
            continue

        non_null_values = profiles[feature].dropna()
        # A column of only missing values can pass through to downstream numeric
        # handling without special coercion here.
        if non_null_values.empty:
            continue

        # Split mixed object columns into string markers versus other values so
        # we can distinguish missing-value strings from truly malformed content.
        string_values = non_null_values[
            non_null_values.map(lambda value: isinstance(value, str))
        ]
        non_string_values = non_null_values[
            non_null_values.map(lambda value: not isinstance(value, str))
        ]

        # Non-string values must still be numeric for this compatibility path
        # to apply.
        non_string_values_are_numeric = non_string_values.map(
            pd.api.types.is_number
        ).all()

        # If the column mixes strings with non-numeric payloads, leave it
        # untouched so validation can reject it below.
        if not non_string_values_are_numeric:
            continue

        # Object columns holding only numbers (no strings) just need a numeric dtype
        if string_values.empty:
            profiles[feature] = pd.to_numeric(profiles[feature], errors="coerce")
            continue

        # Only coerce when every remaining string is a missing-value marker;
        # this avoids silently converting arbitrary strings to NaN.
        if (
            string_values
            .map(lambda value: value.strip().lower())
            .isin(missing_string_tokens)
            .all()
        ):
            profiles[feature] = profiles[feature].map(
                lambda value: (
                    np.nan
                    if isinstance(value, str)
                    and value.strip().lower() in missing_string_tokens
                    else value
                )
            )
            profiles[feature] = pd.to_numeric(profiles[feature], errors="coerce")

    non_numeric_features = [
        feature
        for feature in features
        if not pd.api.types.is_numeric_dtype(profiles[feature])
    ]
    if non_numeric_features:
        raise ValueError(
            "normalize() requires numeric feature columns. "
            "Found non-numeric columns: "
            f"{non_numeric_features}. "
            "Select numeric features explicitly before normalization, "
            "for example by passing a curated feature list or by running "
            "feature_select() first."
        )

    # Separate out the features and meta
    feature_df = profiles.loc[:, features]
    if meta_features == "infer":
        meta_features = infer_cp_features(profiles, metadata=True)

    if isinstance(meta_features, str):
        raise ValueError("meta_features must be a list of strings, not a single string")

    overlapping_columns = sorted(set(features).intersection(meta_features))
    if overlapping_columns:
        raise ValueError(
            f"Columns cannot be both `features` and `meta_features`: {overlapping_columns}"
        )

    meta_df = profiles.loc[:, meta_features]
    # Preserve image payload columns without normalizing them. The
    # ``ome_arrow_*`` prefix is a flexible naming convention used here for
    # OME-Arrow payload columns, not a strict upstream requirement; see the
    # ome-arrow project docs for the broader format context:
    # https://pypi.org/project/ome-arrow/
    feature_and_meta_columns = set(features).union(meta_features)
    passthrough_image_columns = [
        column
        for column in profiles.columns
        if column not in feature_and_meta_columns
        and (column.startswith("Image_") or column.startswith("ome_arrow_"))
    ]
    passthrough_image_df = profiles.loc[:, passthrough_image_columns]

    # If no fitted transform is provided, fit a new one
    if fitted_transform_file is None:
        # Fit the sklearn scaler
        if samples == "all":
            fitted_scaler = scaler.fit(feature_df)
        else:
            # Subset to only the features measured in the sample query
            fitted_scaler = scaler.fit(profiles.query(samples).loc[:, features])

        # if a transform output file is specified, save the fitted transform
        if transform_output_file is not None:
            save_normalize_transform(
                scaler=fitted_scaler,
                method=method,
                joblib_output_file=transform_output_file,
            )

    fitted_scaled = fitted_scaler.transform(feature_df)

    columns = fitted_scaler.columns if method == "spherize" else feature_df.columns

    feature_df = pd.DataFrame(
        fitted_scaled,
        columns=columns,
        index=feature_df.index,
    )

    normalized = pd.concat([meta_df, passthrough_image_df, feature_df], axis="columns")

    if feature_df.shape != profiles.loc[:, features].shape:
        error_detail = "The number of rows and columns in the feature dataframe does not match the original dataframe"
        context = f"the `{method}` method in `pycytominer.normalize`"
        raise ValueError(f"{error_detail}. This is likely a bug in {context}")

    if (normalized.shape[0] != profiles.shape[0]) or (
        normalized.shape[1]
        != len(features) + len(meta_features) + len(passthrough_image_columns)
    ):
        error_detail = "The number of rows and columns in the normalized dataframe does not match the original dataframe"
        context = f"the `{method}` method in `pycytominer.normalize`"
        raise ValueError(f"{error_detail}. This is likely a bug in {context}.")

    return normalized
