"""
Aggregate single cell data based on given grouping variables
"""

import pandas as pd


def aggregate(population_df, strata, variables="all", operation="median"):
    """
    Combine population dataframe variables by strata groups using given operation

    Arguments:
    population_df - pandas DataFrame to group and aggregate
    strata - list indicating the columns to groupby and aggregate
    variables - [default: "all] or list indicating variables that should be aggregated
    operation - [default: "median"] a string indicating how the data is aggregated
                currently only supports one of ['mean', 'median']

    Return:
    Pandas DataFrame of aggregated features
    """

    operation = operation.lower()

    assert operation in ["mean", "median"], "operation must be one ['mean', 'median']"

    # Subset dataframe to only specified variables if provided
    if variables != "all":
        strata_df = population_df.loc[:, strata]
        population_df = population_df.loc[:, variables]
        population_df = pd.concat([strata_df, population_df], axis="columns")

    population_df = population_df.groupby(strata)

    if operation == "median":
        return population_df.median().reset_index()
    else:
        return population_df.mean().reset_index()
