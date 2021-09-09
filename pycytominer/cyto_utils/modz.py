import numpy as np
import pandas as pd
from pycytominer.cyto_utils.util import (
    get_pairwise_correlation,
    check_correlation_method,
    infer_cp_features,
)


def modz_base(population_df, method="spearman", min_weight=0.01, precision=4):
    """
    Perform a modified z score transformation. This code is modified from cmapPy.
    (see https://github.com/cytomining/pycytominer/issues/52). Note that this will
    apply the transformation to the FULL population_df.
    See modz() for replicate level procedures.

    Arguments:
    population_df - pandas DataFrame that includes metadata and observation features.
                    rows are samples and columns are features
    method - string indicating which correlation metric to use [default: "spearman"]
    min_weight - the minimum correlation to clip all non-negative values lower to
    precision - how many significant digits to round weights to

    Return:
    modz transformed dataframe - a consensus signature of the input population_df
    weighted by replicate correlation
    """
    assert population_df.shape[0] > 0, "population_df must include at least one sample"

    method = check_correlation_method(method=method)

    # Step 1: Extract pairwise correlations of samples
    # Transpose so samples are columns
    population_df = population_df.transpose()
    cor_df, pair_df = get_pairwise_correlation(population_df, method=method)

    # Round correlation results
    pair_df = pair_df.round(precision)

    # Step 2: Identify sample weights
    # Fill diagonal of correlation_matrix with np.nan
    np.fill_diagonal(cor_df.values, np.nan)

    # Remove negative values
    cor_df = cor_df.clip(lower=0)

    # Get average correlation for each profile (will ignore NaN)
    raw_weights = cor_df.mean(axis=1)

    # Threshold weights (any value < min_weight will become min_weight)
    raw_weights = raw_weights.clip(lower=min_weight)

    # normalize raw_weights so that they add to 1
    weights = raw_weights / sum(raw_weights)
    weights = weights.round(precision)

    # Step 3: Normalize
    if population_df.shape[1] == 1:
        # There is only one sample (note that columns are now samples)
        modz_df = population_df.sum(axis=1)
    else:
        modz_df = population_df * weights
        modz_df = modz_df.sum(axis=1)

    return modz_df


def modz(
    population_df,
    replicate_columns,
    features="infer",
    method="spearman",
    min_weight=0.01,
    precision=4,
):
    """
    Collapse replicates into a consensus signature using a weighted transformation

    Arguments:
    population_df - pandas DataFrame that includes metadata and observation features.
                    rows are samples and columns are features
    replicate_columns - a string or list of column(s) in the population dataframe that
                        indicate replicate level information
    features - a list of features present in the population dataframe [default: "infer"]
               if "infer", then assume cell painting features are those that start with
               "Cells_", "Nuclei_", or "Cytoplasm_"
    method - string indicating which correlation metric to use [default: "spearman"]
    min_weight - the minimum correlation to clip all non-negative values lower to
    precision - how many significant digits to round weights to

    Return:
    Consensus signatures for all replicates in the given DataFrame
    """
    population_features = population_df.columns.tolist()
    assert_error = "{} not in input dataframe".format(replicate_columns)
    if isinstance(replicate_columns, list):
        assert all([x in population_features for x in replicate_columns]), assert_error
    elif isinstance(replicate_columns, str):
        assert replicate_columns in population_features, assert_error
        replicate_columns = replicate_columns.split()
    else:
        return ValueError("replicate_columns must be a list or string")

    if features == "infer":
        features = infer_cp_features(population_df)

    subset_features = list(set(replicate_columns + features))
    population_df = population_df.loc[:, subset_features]

    modz_df = (
        population_df.groupby(replicate_columns)
        .apply(
            lambda x: modz_base(
                x.loc[:, features],
                method=method,
                min_weight=min_weight,
                precision=precision,
            )
        )
        .reset_index()
    )

    return modz_df
