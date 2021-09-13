"""
Normalize observation features based on specified normalization method
"""

import pandas as pd
from sklearn.preprocessing import StandardScaler, RobustScaler

from pycytominer.cyto_utils import (
    output,
    infer_cp_features,
    load_profiles,
)
from pycytominer.operations import Spherize, RobustMAD


def normalize(
    profiles,
    features="infer",
    image_features=False,
    meta_features="infer",
    samples="all",
    method="standardize",
    output_file="none",
    compression_options=None,
    float_format=None,
    mad_robustize_epsilon=1e-18,
    spherize_center=True,
    spherize_method="ZCA-cor",
    spherize_epsilon=1e-6,
):
    """Normalize profiling features

    Parameters
    ----------
    profiles : pandas.core.frame.DataFrame or path
        Either a pandas DataFrame or a file that stores profile data
    features : list
        A list of strings corresponding to feature measurement column names in the
        `profiles` DataFrame. All features listed must be found in `profiles`.
        Defaults to "infer". If "infer", then assume cell painting features are those
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    image_features: bool, default False
        Whether the profiles contain image features.
    meta_features : list
        A list of strings corresponding to metadata column names in the `profiles`
        DataFrame. All features listed must be found in `profiles`. Defaults to "infer".
        If "infer", then assume metadata features are those prefixed with "Metadata"
    samples : str
        The metadata column values to use as a normalization reference. We often use
        control samples. The function uses a pd.query() function, so you should
        structure samples in this fashion. An example is
        "Metadata_treatment == 'control'" (include all quotes). Defaults to "all".
    method : str
        How to normalize the dataframe. Defaults to "standardize". Check avail_methods
        for available normalization methods.
    output_file : str, optional
        If provided, will write annotated profiles to file. If not specified, will
        return the normalized profiles as output. We recommend that this output file be
        suffixed with "_normalized.csv".
    compression_options : str or dict, optional
        Contains compression options as input to
        pd.DataFrame.to_csv(compression=compression_options). pandas version >= 1.2.
    float_format : str, optional
        Decimal precision to use in writing output file as input to
        pd.DataFrame.to_csv(float_format=float_format). For example, use "%.3g" for 3
        decimal precision.
    mad_robustize_epsilon: float, optional
        The mad_robustize fudge factor parameter. The function only uses
        this variable if method = "mad_robustize". Set this to 0 if
        mad_robustize generates features with large values.
    spherize_center : bool
        If the function should center data before sphering (aka whitening). The
        function only uses this variable if method = "spherize". Defaults to True.
    spherize_method : str
        The sphering (aka whitening) normalization selection. The function only uses
        this variable if method = "spherize". Defaults to "ZCA-corr". See
        :py:func:`pycytominer.operations.transform` for available spherize methods.
    spherize_epsilon : float, default 1e-6.
        The sphering (aka whitening) fudge factor parameter. The function only uses
        this variable if method = "spherize".

    Returns
    -------
    normalized : pandas.core.frame.DataFrame, optional
        The normalized profile DataFrame. If output_file="none", then return the
        DataFrame. If you specify output_file, then write to file and do not return
        data.

    Examples
    --------
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
        meta_features="infer",
        samples="Metadata_treatment == 'control'",
        method="standardize"
    )
    """

    # Load Data
    profiles = load_profiles(profiles)

    # Define which scaler to use
    method = method.lower()

    avail_methods = ["standardize", "robustize", "mad_robustize", "spherize"]
    assert method in avail_methods, "operation must be one {}".format(avail_methods)

    if method == "standardize":
        scaler = StandardScaler()
    elif method == "robustize":
        scaler = RobustScaler()
    elif method == "mad_robustize":
        scaler = RobustMAD(epsilon=mad_robustize_epsilon)
    elif method == "spherize":
        scaler = Spherize(
            center=spherize_center, method=spherize_method, epsilon=spherize_epsilon
        )

    if features == "infer":
        features = infer_cp_features(profiles, image_features=image_features)

    # Separate out the features and meta
    feature_df = profiles.loc[:, features]
    if meta_features == "infer":
        meta_features = infer_cp_features(profiles, metadata=True)

    meta_df = profiles.loc[:, meta_features]

    # Fit the sklearn scaler
    if samples == "all":
        fitted_scaler = scaler.fit(feature_df)
    else:
        # Subset to only the features measured in the sample query
        fitted_scaler = scaler.fit(profiles.query(samples).loc[:, features])

    # Scale the feature dataframe
    feature_df = pd.DataFrame(
        fitted_scaler.transform(feature_df),
        columns=feature_df.columns,
        index=feature_df.index,
    )

    normalized = meta_df.merge(feature_df, left_index=True, right_index=True)

    if output_file != "none":
        output(
            df=normalized,
            output_filename=output_file,
            compression_options=compression_options,
            float_format=float_format,
        )
    else:
        return normalized
