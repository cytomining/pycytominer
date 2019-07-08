"""
Compute sparse random projection of matrix to reduce dimensionality of population

reduces the dimensionality of a population by projecting the original data with a
sparse random matrix. Generally more efficient and faster to compute than a Gaussian
random projection matrix, while providing similar embedding quality.
"""

import numpy as np
import pandas as pd
from sklearn import random_projection


def sparse_random_projection(
    population_df, variables="all", n_components=3000, seed="none"
):
    """
    Output the sparse random projection matrix of a given population dataframe

    Arguments:
    population_df - pandas DataFrame to group and aggregate
    variables - [default: "all"] or list indicating variables that should be applied
    n_components - [default: 3000] the number of components to project
                   the default is set from https://doi.org/10.1038/s41467-019-10154-8
    seed - [default: "none"] set the random seed to control random matrix

    Output:
    DataFrame of a sparse randomized projection matrix
    """

    if seed != "none":
        np.random.seed(seed)

    transformer = random_projection.SparseRandomProjection(n_components=n_components)
    transformed_col_names = ["sparse_comp_{}".format(x) for x in range(0, n_components)]

    if variables == "all":
        transformed_population_df = pd.DataFrame(
            transformer.fit_transform(population_df), columns=transformed_col_names
        )
    else:
        transformed_population_df = pd.DataFrame(
            transformer.fit_transform(population_df.loc[:, variables]),
            columns=transformed_col_names,
        )

    return transformed_population_df
