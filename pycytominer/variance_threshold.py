"""
Remove variables with near-zero variance.
Modified from caret::nearZeroVar()
"""

import numpy as np
import pandas as pd


def variance_threshold(
    population_df, features="infer", samples="none", freq_cut=0.05, unique_cut=0.01
):
    """
    Exclude features that have correlations below a certain threshold

    Arguments:
    population_df - pandas DataFrame that includes metadata and observation features
    features - a list of features present in the population dataframe [default: "infer"]
               if "infer", then assume cell painting features are those that do not
               start with "Metadata_"
    samples - list samples to perform operation on
              [default: "none"] - if "none", use all samples to calculate
    freq_cut - float of ratio (second most common feature value / most common) [default: 0.1]
    unique_cut - float of ratio (num unique features / num samples) [default: 0.1]

    Return:
    list of features to exclude from the population_df
    """

    assert 0 <= freq_cut <= 1, "freq_cut variable must be between (0 and 1)"
    assert 0 <= unique_cut <= 1, "unique_cut variable must be between (0 and 1)"

    # Subset dataframe and calculate correlation matrix across subset features
    if samples != "none":
        population_df = population_df.loc[samples, :]

    if features == "infer":
        features = [
            x for x in population_df.columns.tolist() if not x.startswith("Metadata_")
        ]
    else:
        population_df = population_df.loc[:, features]

    # Test if excluded for low frequency
    excluded_features_freq = population_df.apply(
        lambda x: calculate_frequency(x, freq_cut), axis="rows"
    )

    excluded_features_freq = excluded_features_freq[
        excluded_features_freq.isna()
    ].index.tolist()

    # Test if excluded for uniqueness
    n = population_df.shape[0]
    num_unique_features = population_df.nunique()

    unique_ratio = num_unique_features / n
    unique_ratio = unique_ratio < unique_cut
    excluded_features_unique = unique_ratio[unique_ratio].index.tolist()

    excluded_features = list(set(excluded_features_freq + excluded_features_unique))
    return excluded_features


def calculate_frequency(feature_column, freq_cut):
    """
    Calculate frequency of second most common to most common feature.
    Used in pandas.apply()

    Arguments:
    feature_column - pandas series of the specific feature in the population_df
    freq_cut - float of ratio (second most common feature value / most common)

    Return:
    Feature name if it passes threshold, "NA" otherwise
    """
    val_count = feature_column.value_counts()
    max_count = val_count.iloc[0]
    try:
        second_max_count = val_count.iloc[1]
    except IndexError:
        return np.nan

    freq = second_max_count / max_count

    if freq < freq_cut:
        return np.nan
    else:
        return feature_column.name
