"""
Returns list of variables such that no two variables have a correlation greater than a
specified threshold
"""

import numpy as np
import pandas as pd


def correlation_threshold(variables, data_df, threshold=0.9, method="pearson"):
    """
    Exclude variables that have correlations above a certain threshold

    Arguments:
    variables - list specifying observation variables
    data_df - Pandas DataFrame containing the data to calculate variable correlation
              typically, this DataFrame is a sampled subset of the full dataframe
    threshold - float between (0, 1) to exclude variables [default: 0.9]
    method - string indicating which correlation metric to use to test cutoff
             [default: "pearson"]

    Return:
    A list of variables to exclude
    """
    method = method.lower()

    assert 0 <= threshold <= 1, "threshold variable must be between (0 and 1)"
    assert method in [
        "pearson",
        "spearman",
        "kendall",
    ], "method not supported, select one of ['pearson', 'spearman', 'kendall']"

    # Subset dataframe and calculate correlation matrix across subset variables
    data_cor_df = data_df.loc[:, variables].corr(method=method)

    # Create a copy of the dataframe to generate upper triangle of zeros
    data_cor_zerotri_df = data_cor_df.copy()

    # Zero out upper triangle in correlation matrix
    data_cor_zerotri_df.loc[:, :] = np.tril(data_cor_df, k=-1)

    # Get absolute sum of correlation across variables
    # The lower the index, the less correlation to the full data frame
    # We want to drop variables with highest correlation, so drop higher index
    variable_cor_sum = data_cor_df.abs().sum().sort_values().index

    # Acquire pairwise correlations in a long format
    # Note that we are using the zero triangle DataFrame
    pairwise_df = data_cor_zerotri_df.stack().reset_index()
    pairwise_df.columns = ["pair_a", "pair_b", "correlation"]

    # And subset to only variable combinations that pass the threshold
    pairwise_df = pairwise_df.query("correlation > @threshold")

    # Output the excluded variables
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
