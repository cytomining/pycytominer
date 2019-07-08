"""
Compute covariance matrix and vectorize
"""

import numpy as np
import pandas as pd


def covariance_base(population_df, variables="all"):
    """
    Extract the covariance matrix for a given population DataFrame and input features

    Arguments:
    population_df - pandas DataFrame to group and aggregate
    variables - [default: "all"] or list indicating variables that should be applied

    Return:
    One row of a vectorized covariance matrix
    """

    # Get the covariance matrix for the given variables
    if variables != "all":
        pop_cov_df = population_df.loc[:, variables].cov()
    else:
        pop_cov_df = population_df.cov()

    # The covariance matrix is symmetrical, take only the lower triangle
    pop_cov_df.loc[:, :] = np.tril(pop_cov_df, k=0)

    # Process the dataframe to extract a single row of variable covariances
    pop_cov_df = pop_cov_df.stack().reset_index()
    pop_cov_df.columns = ["var_1", "var_2", "covar"]
    pop_cov_df = pop_cov_df.query("covar > 0").reset_index(drop=True)
    pop_cov_df = (
        pop_cov_df
        .assign(covar_feature=pop_cov_df.var_1 + "__" + pop_cov_df.var_2)
        .transpose()
        )
    pop_cov_df.columns = pop_cov_df.loc["covar_feature", :]
    pop_cov_df = pop_cov_df.loc[["covar"], :]

    return pop_cov_df


def covariance(population_df, variables="all", strata="none"):
    """
    Extract the covariance matrix for a given population DataFrame and input features

    Arguments:
    population_df - pandas DataFrame to group and aggregate
    variables - [default: "all"] or list indicating variables that should be applied
    strata - [default: "none"] or list indicating the columns to groupby and covar

    Return:
    One row of a vectorized covariance matrix
    """
    # If strata is specified, then group input before calculating covariance
    if strata != "none":
        population_group = population_df.groupby(strata)
        pop_cov_df = population_group.apply(lambda x: covariance_base(x, variables))
    else:
        pop_cov_df = covariance_base(population_df=population_df, variables=variables)

    return pop_cov_df
