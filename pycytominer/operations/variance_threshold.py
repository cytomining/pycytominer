"""
Identify low-information features by filtering columns with variance below a threshold.
"""

from typing import Union

import pandas as pd

from pycytominer.cyto_utils.features import infer_cp_features


def variance_threshold(
    population_df: pd.DataFrame,
    features: Union[str, list[str]] = "infer",
    samples: str = "all",
    min_variance: float = 1e-6,
) -> list[str]:
    """Exclude features that have low variance (low information content)

    This is done by calculating the variance of each feature in the population_df and then
    removing features with variance less than the `min_variance` threshold. A low value
    will remove features that have very low variance (e.g. this will remove a
    feature: [1.0000, 1.0001, 1.0000, 1.0001, 1.0000]).

    Parameters
    ----------
    population_df : pd.DataFrame
        DataFrame that includes metadata and observation features.
    features : list, default "infer"
        A list of strings corresponding to feature measurement column names in the
        `population_df` DataFrame. All features listed must be found in `population_df`.
        Defaults to "infer". If "infer", then assume CellProfiler features are those
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    samples : str, default "all"
        List of samples to perform operation on. The function uses a pd.DataFrame.query()
        function, so you should  structure samples in this fashion. An example is
        "Metadata_treatment == 'control'" (include all quotes).
        If "all", use all samples to calculate.
    min_variance: float, default 1e-6
        Removes features with variance less than this value.

    Returns
    -------
    excluded_features : list[str]
         List of features to exclude from the population_df.

    """

    # Checking for min_variance type and value
    if not isinstance(min_variance, float) or isinstance(min_variance, bool):
        raise ValueError("min_variance must be a float value")
    if min_variance < 0:
        raise ValueError("min_variance must be non-negative")

    # Checking for features type and value
    if features == "infer":
        inferred_features = infer_cp_features(population_df)
    elif isinstance(features, list):
        inferred_features = features
    else:
        raise ValueError('features must be a list of column names or "infer"')

    # Checking for samples type and value
    if not isinstance(samples, str):
        raise ValueError("samples must be a string")

    # Subset the population_df based on features and samples
    if samples != "all":
        population_df = population_df.query(expr=samples)

    if features == "infer":
        inferred_features = infer_cp_features(population_df)
    elif isinstance(features, list):
        inferred_features = features

    # Subset the population_df based on the inferred features
    population_df = population_df.loc[:, inferred_features]

    # Calculate the variance of each feature in the population_df.
    # This returns a series of feature names as the index and the variance as the values.
    feature_variances = population_df.var(ddof=0, skipna=True)

    # Set a mask for features with variance less than min_variance.
    is_low_variance_mask = feature_variances < min_variance

    # return features with low variance as a list of feature names
    excluded_low_variance_features = feature_variances.index[
        is_low_variance_mask
    ].tolist()

    return excluded_low_variance_features
