"""
Remove variables with near-zero variance.
Modified from caret::nearZeroVar()
"""

import numpy as np
import pandas as pd


def variance_threshold(population_df, samples="none", freq_cut=0.05, unique_cut=0.1):
    """
    Exclude features that have correlations above a certain threshold

    Arguments:
    population_df - pandas DataFrame that includes metadata and observation features
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

    # Test if excluded for low frequency
    excluded_features_freq = population_df.apply(
        lambda x: calculate_frequency(x, freq_cut), axis="columns"
    )

    excluded_features_freq = [x for x in excluded_features_freq if not np.isnan(x)]

    # Test if excluded for uniqueness
    n = population_df.shape[0]
    num_unique_features = population_df.nunique()

    unique_ratio = num_unique_features / n
    unique_ratio = unique_ratio < unique_cut
    excluded_features_unique = unique_ratio[unique_ratio].index.tolist()


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
    second_max_count = val_count.iloc[1]
    freq = second_max_count / max_count

    if freq > freq_cut:
        return np.nan
    else:
        return feature_column.name
