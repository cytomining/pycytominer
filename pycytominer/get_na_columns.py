"""
Remove variables with specified threshold of NA values
Note: This was called `drop_na_columns` in cytominer for R
"""

import pandas as pd


def get_na_columns(population_df, variables, cutoff=0.05):
    """
    Get features that have more NA values than cutoff defined

    Arguments:
    population_df - pandas DataFrame storing profiles
    variables - a list of features present in the population dataframe

    Output:
    A list of the features to exclude
    """

    num_rows = population_df.shape[0]
    na_prop_df = population_df.loc[:, variables].isna().sum() / num_rows

    na_prop_df = na_prop_df[na_prop_df > cutoff]
    return list(set(na_prop_df.index.tolist()))
