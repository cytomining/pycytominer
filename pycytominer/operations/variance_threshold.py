"""
Remove variables with near-zero variance.
Modified from caret::nearZeroVar()
"""

import numpy as np
import pandas as pd
from pycytominer.cyto_utils import infer_cp_features


def variance_threshold(
    population_df, features="infer", samples="all", freq_cut=0.05, unique_cut=0.01
):
    """Exclude features that have low variance (low information content)

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
        DataFrame that includes metadata and observation features.
    features : list, default "infer"
         List of features present in the population dataframe [default: "infer"]
         if "infer", then assume cell painting features are those that start with
         "Cells_", "Nuclei_", or "Cytoplasm_".
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
    excluded_features : list of str
         List of features to exclude from the population_df.

    """

    assert 0 <= freq_cut <= 1, "freq_cut variable must be between (0 and 1)"
    assert 0 <= unique_cut <= 1, "unique_cut variable must be between (0 and 1)"

    # Subset dataframe
    if samples != "all":
        population_df.query(samples, inplace=True)

    if features == "infer":
        features = infer_cp_features(population_df)

    population_df = population_df.loc[:, features]

    # Exclude features with extreme (defined by freq_cut ratio) common values
    excluded_features_freq = population_df.apply(
        lambda x: calculate_frequency(x, freq_cut), axis="rows"
    )

    excluded_features_freq = excluded_features_freq[
        excluded_features_freq.isna()
    ].index.tolist()

    # Exclude features with too many (defined by unique_ratio) values in common
    n = population_df.shape[0]
    num_unique_features = population_df.nunique()

    unique_ratio = num_unique_features / n
    unique_ratio = unique_ratio < unique_cut
    excluded_features_unique = unique_ratio[unique_ratio].index.tolist()

    excluded_features = list(set(excluded_features_freq + excluded_features_unique))
    return excluded_features


def calculate_frequency(feature_column, freq_cut):
    """Calculate frequency of second most common to most common feature.
    Used in pandas.apply()

    Parameters
    ----------
    feature_column : pandas.core.series.series
        Pandas series of the specific feature in the population_df
    freq_cut : float, default 0.05
        Ratio (2nd most common feature val / most common). Must range between 0 and 1.
        Remove features lower than freq_cut. A low freq_cut will remove features
        that have large difference between the most common feature and second most
        common feature. (e.g. this will remove a feature: [1, 1, 1, 1, 0.01, 0.01, ...])

    Returns
    -------
    Feature name if it passes threshold, "NA" otherwise

    """

    val_count = feature_column.value_counts()
    try:
        max_count = val_count.iloc[0]
    except IndexError:
        return np.nan
    try:
        second_max_count = val_count.iloc[1]
    except IndexError:
        return np.nan

    freq = second_max_count / max_count

    if freq < freq_cut:
        return np.nan
    else:
        return feature_column.name
