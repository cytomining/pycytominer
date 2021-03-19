"""
Remove variables with specified threshold of NA values
Note: This was called `drop_na_columns` in cytominer for R
"""

import pandas as pd
from pycytominer.cyto_utils.features import infer_cp_features


def get_na_columns(population_df, features="infer", samples="all", cutoff=0.05):
    """
    Get features that have more NA values than cutoff defined

    Arguments:
    population_df - pandas DataFrame storing profiles
    features - list of features present in the population dataframe [default: "infer"]
               if "infer", then assume cell painting features are those that do not
               start with "Cells", "Nuclei", or "Cytoplasm"
    samples - if provided, a list of samples to provide operation on
              [default: "all"] - if "all", use all samples to calculate
    cutoff - float to exclude features that have a higher proportion of missingness

    Output:
    A list of the features to exclude
    """

    if samples != "all":
        population_df = population_df.loc[samples, :]

    if features == "infer":
        features = infer_cp_features(population_df)
    else:
        population_df = population_df.loc[:, features]

    num_rows = population_df.shape[0]
    na_prop_df = population_df.isna().sum() / num_rows

    na_prop_df = na_prop_df[na_prop_df > cutoff]
    return list(set(na_prop_df.index.tolist()))
