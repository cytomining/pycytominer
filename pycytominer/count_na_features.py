"""
Count the number of NAs per variable
Note this was called `count_na_rows()` in cytominer
"""

import pandas as pd


def count_na_features(population_df, features):
    """
    Given a population dataframe and features, count how many nas per feature

    Arguments:
    population_df - pandas DataFrame storing profiles
    features - a list of features present in the population dataframe

    Return:
    Dataframe of NA counts per variable
    """

    return pd.DataFrame(
        population_df.loc[:, features].isna().sum(), columns=["num_na"]
    )
