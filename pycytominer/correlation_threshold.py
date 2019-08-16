"""
Returns list of features such that no two features have a correlation greater than a
specified threshold
"""

import numpy as np
import pandas as pd


def correlation_threshold(
    population_df, features="infer", samples="none", threshold=0.9, method="pearson"
):
    """
    Exclude features that have correlations above a certain threshold

    Arguments:
    population_df - pandas DataFrame that includes metadata and observation features
    features - a list of features present in the population dataframe [default: "infer"]
               if "infer", then assume cell painting features are those that do not
               start with "Metadata_"
    samples - list samples to perform operation on
              [default: "none"] - if "none", use all samples to calculate
    threshold - float between (0, 1) to exclude features [default: 0.9]
    method - string indicating which correlation metric to use to test cutoff
             [default: "pearson"]

    Return:
    list of features to exclude from the population_df
    """
    method = method.lower()

    assert 0 <= threshold <= 1, "threshold variable must be between (0 and 1)"
    assert method in [
        "pearson",
        "spearman",
        "kendall",
    ], "method not supported, select one of ['pearson', 'spearman', 'kendall']"

    # Subset dataframe and calculate correlation matrix across subset features
    if samples != "none":
        population_df = population_df.loc[samples, :]

    if features == "infer":
        features = [
            x for x in population_df.columns.tolist() if not x.startswith("Metadata_")
        ]
    else:
        population_df = population_df.loc[:, features]

    data_cor_df = population_df.corr(method=method)

    # Create a copy of the dataframe to generate upper triangle of zeros
    data_cor_zerotri_df = data_cor_df.copy()

    # Zero out upper triangle in correlation matrix
    data_cor_zerotri_df.loc[:, :] = np.tril(data_cor_df, k=-1)

    # Get absolute sum of correlation across features
    # The lower the index, the less correlation to the full data frame
    # We want to drop features with highest correlation, so drop higher index
    variable_cor_sum = data_cor_df.abs().sum().sort_values().index

    # Acquire pairwise correlations in a long format
    # Note that we are using the zero triangle DataFrame
    pairwise_df = data_cor_zerotri_df.stack().reset_index()
    pairwise_df.columns = ["pair_a", "pair_b", "correlation"]

    # And subset to only variable combinations that pass the threshold
    pairwise_df = pairwise_df.query("correlation > @threshold")

    # Return an empty list if nothing is over correlation threshold
    if pairwise_df.shape[0] == 0:
        return []

    # Output the excluded features
    excluded = pairwise_df.apply(
        lambda x: determine_high_cor_pair(x, variable_cor_sum), axis="columns"
    )

    return list(set(excluded.tolist()))


def determine_high_cor_pair(correlation_row, sorted_correlation_pairs):
    """
    Select highest correlated variable given a correlation row with columns:
    ["pair_a", "pair_b", "correlation"]

    For use in a pandas.apply()
    """

    pair_a = correlation_row["pair_a"]
    pair_b = correlation_row["pair_b"]

    if sorted_correlation_pairs.get_loc(pair_a) > sorted_correlation_pairs.get_loc(
        pair_b
    ):
        return pair_a
    else:
        return pair_b
