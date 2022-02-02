"""
Remove variables with specified threshold of NA values
Note: This was called `drop_na_columns` in cytominer for R
"""

import pandas as pd
from pycytominer.cyto_utils.features import infer_cp_features


def get_na_columns(population_df, features="infer", samples="all", cutoff=0.05):
    """Get features that have more NA values than cutoff defined

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
        DataFrame that includes metadata and observation features.
    features : list, default "infer"
         List of features present in the population dataframe [default: "infer"]
         if "infer", then assume cell painting features are those that start with
         "Cells_", "Nuclei_", or "Cytoplasm_".
    samples : list or str, default "all"
        List of samples to perform operation on. If "all", use all samples to calculate.
    cutoff : float
        Exclude features that have a certain proportion of missingness

    Returns
    -------
    excluded_features : list of str
         List of features to exclude from the population_df.
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
