"""
Identify low-information features based on repeated values and low uniqueness.
"""

from typing import Union

import pandas as pd

from pycytominer.cyto_utils.features import infer_cp_features


def frequency_threshold(
    population_df: pd.DataFrame,
    features: Union[str, list[str]] = "infer",
    samples: str = "all",
    freq_cut: float = 0.05,
    unique_cut: float = 0.01,
) -> list[str]:
    """Exclude features that have low variance (low information content) based on
    frequency and uniqueness.

    Frequency is defined as the ratio of the second most common value to the most common
    value. Uniqueness is defined as the ratio of the number of unique values to the total
    number of samples.

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
    freq_cut : float, default 0.05
        Ratio (2nd most common feature val / most common). Must range between 0 and 1.
        Remove features lower than freq_cut. A low freq_cut will remove features
        that have large difference between the most common feature value and second most
        common feature value. (e.g. this will remove a feature: [1, 1, 1, 1, 0.01, 0.01, ...])
    unique_cut: float, default 0.01
        Ratio (num unique features / num samples). Must range between 0 and 1.
        Remove features less than unique cut. A low unique_cut will remove features
        that have very few different measurements compared to the number of samples.

    Returns
    -------
    excluded_features : list[str]
         List of features to exclude from the population_df.

    """
    # check if freq_cut and unique_cut are between 0 and 1
    if not 0 <= freq_cut <= 1:
        raise ValueError("freq_cut variable must be between (0 and 1)")
    if not 0 <= unique_cut <= 1:
        raise ValueError("unique_cut variable must be between (0 and 1)")

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
    else:
        raise ValueError('features must be a list of column names or "infer"')

    # set population df with only the features of interest
    population_df = population_df.loc[:, inferred_features]

    # Calculate the frequency ratio (2nd most common value count / most common value
    # count) for each feature. Returns a pandas Series [feature name, ratio].
    freq_ratios = population_df.apply(calculate_frequency, axis=0)

    # Get the feature names that have a frequency ratio below the freq_cut threshold
    low_freq_mask = freq_ratios < freq_cut
    excluded_features_freq_index_list = low_freq_mask[low_freq_mask].index.tolist()

    # Get the number of samples
    n = population_df.shape[0]

    # Get the number of unique features
    num_unique_features = population_df.nunique()

    # Exclude features with too many (defined by unique_ratio) values in common, where
    # unique_ratio is defined as the number of unique features divided by the total
    # number of samples
    unique_ratio = num_unique_features / n
    unique_ratio_mask = unique_ratio < unique_cut

    # Get the feature names that have a unique ratio less than the unique_cut
    # This represents features that have too few unique values compared to the number
    # of samples.
    excluded_features_unique = unique_ratio_mask[unique_ratio_mask].index.tolist()

    # Compile the final list of excluded features by combining the frequency-based and
    # unique-based exclusions
    excluded_features = list(
        set(excluded_features_freq_index_list + excluded_features_unique)
    )

    return excluded_features


def calculate_frequency(feature_column: pd.Series) -> float:
    """Calculate the ratio of the second most common value count to the most common
    value count for a feature. Used in pandas.apply()

    Parameters
    ----------
    feature_column : pd.Series
        Pandas series of the specific feature in the population_df

    Returns
    -------
    float
        Ratio of the second most common value count to the most common value count.
        Returns 0.0 if the feature has fewer than two observed values.
    """

    # Calculate the value counts for the feature column
    val_counts = feature_column.value_counts()
    if len(val_counts) < 2:
        return 0.0

    return val_counts.iloc[1] / val_counts.iloc[0]
