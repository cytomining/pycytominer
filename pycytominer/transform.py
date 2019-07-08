"""
Transform observation variables by specified groups
"""

from scipy.cluster.vq import whiten
import pandas as pd


def transform(population_df, strata="none", variables="all", operation="whiten"):
    """
    Transform a given population dataframe

    Arguments:
    population_df - pandas DataFrame to group and aggregate
    strata - [default: "none"] list indicating the columns to groupby and transform
    variables - [default: "all] or list indicating variables that should be aggregated
    operation - [default: "generalized_log"] string indicating how to transform
                currently only supports one of ['whiten']

    Return:
    Pandas DataFrame of transformed features
    """

    operation = operation.lower()

    assert operation in ["whiten"], "operation must be one ['whiten']"

    if strata != "none":
        assert variables != "all", "if strata is specified, must provide variables"
        population_df = population_df.groupby(strata)

    if operation == "whiten":
        transformed_df = whiten_transform(
            population_df=population_df, variables=variables
        )

    return transformed_df


def whiten_transform(population_df, variables="all"):
    """
    Whitening removes linear correlation across variables by scaling by standard
    deviation. Transforms the input covariance matrix into an identity matrix.
    """

    if type(population_df) == pd.core.groupby.generic.DataFrameGroupBy:
        output_df = population_df[variables].apply(lambda x: pd.DataFrame(whiten(x)))
        output_df.columns = variables
        output_df = output_df.reset_index()
        output_df = output_df.loc[:, ~output_df.columns.str.startswith("level_")]
        return output_df

    if variables != "all":
        output_df = pd.DataFrame(
            whiten(population_df.loc[:, variables]), columns=variables
        )

    else:
        output_df = pd.DataFrame(whiten(population_df), columns=population_df.columns)

    return output_df
