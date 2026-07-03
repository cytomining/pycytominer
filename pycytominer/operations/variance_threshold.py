"""
Identify low-information features by filtering columns with variance below a threshold.
"""

from typing import Union

import pandas as pd
from sklearn.feature_selection import VarianceThreshold

from pycytominer.cyto_utils.features import infer_cp_features


def variance_threshold(
    population_df: pd.DataFrame,
    features: Union[str, list[str]] = "infer",
    samples: str = "all",
    min_variance: int | float = 1e-6,
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
    if not isinstance(min_variance, (int, float)):
        raise ValueError("min_variance must be a float value")
    if min_variance < 0:
        raise ValueError("min_variance must be non-negative")

    # Checking for samples type and value
    if not isinstance(samples, str):
        raise ValueError("samples must be a string")

    # Subset the population_df based on features and samples
    if samples != "all":
        population_df = population_df.query(expr=samples)

    # infer features or set features based on user input
    if features == "infer":
        inferred_features = infer_cp_features(population_df)
    elif isinstance(features, list):
        inferred_features = features

    # Subset the population_df based on the inferred features
    population_df = population_df.loc[:, inferred_features]

    # Create a VarianceThreshold object with the specified min_variance threshold
    # and fit it to the population_df to identify low-variance features.
    variance_selector = VarianceThreshold(threshold=min_variance)
    variance_selector.fit(population_df)

    # Set a mask for features with variance less than min_variance.
    is_low_variance_mask = ~variance_selector.get_support()

    # return features with low variance as a list of feature names
    excluded_low_variance_features = population_df.columns[
        is_low_variance_mask
    ].tolist()

    return excluded_low_variance_features
