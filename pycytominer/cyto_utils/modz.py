import numpy as np
import pandas as pd

from pycytominer.cyto_utils.features import infer_cp_features
from pycytominer.cyto_utils.util import (
    check_correlation_method,
    get_pairwise_correlation,
)


def modz_base(population_df, method="spearman", min_weight=0.01, precision=4):
    """Perform a modified z score transformation.

    This code is modified from cmapPy.
    (see https://github.com/cytomining/pycytominer/issues/52). Note that this will
    apply the transformation to the FULL population_df.
    See modz() for replicate level procedures.

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
        DataFrame that includes metadata and observation features.
    method : str, default "spearman"
        indicating which correlation metric to use.
    min_weight : float, default 0.01
        the minimum correlation to clip all non-negative values lower to
    precision : int, default 4
        how many significant digits to round weights to

    Returns
    -------
    modz_df : pandas.core.frame.DataFrame
        modz transformed dataframe - a consensus signature of the input data
        weighted by replicate correlation
    """
    assert population_df.shape[0] > 0, "population_df must include at least one sample"  # noqa: S101

    method = check_correlation_method(method=method)

    # Step 1: Extract pairwise correlations of samples
    # Transpose so samples are columns
    population_df = population_df.transpose()
    cor_df, pair_df = get_pairwise_correlation(population_df, method=method)

    # Round correlation results
    pair_df = pair_df.round(precision)

    # create a copy of cor_df values for use with np.fill_diagonal
    cor_df_values = cor_df.values.copy()

    # Step 2: Identify sample weights
    # Fill diagonal of correlation_matrix with np.nan
    np.fill_diagonal(cor_df_values, np.nan)

    # reconstitute the changed data as a new dataframe to avoid read-only behavior
    cor_df = pd.DataFrame(
        data=cor_df_values, index=cor_df.index, columns=cor_df.columns
    )

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
    """Collapse replicates into a consensus signature using a weighted transformation

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
        DataFrame that includes metadata and observation features.
    replicate_columns : str, list
        a string or list of column(s) in the population dataframe that
        indicate replicate level information
    features : list, default "infer"
        A list of strings corresponding to feature measurement column names in the
        `population_df` DataFrame. All features listed must be found in `population_df`.
        Defaults to "infer". If "infer", then assume CellProfiler features are those
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    method : str, default "spearman"
        indicating which correlation metric to use.
    min_weight : float, default 0.01
        the minimum correlation to clip all non-negative values lower to
    precision : int, default 4
        how many significant digits to round weights to

    Returns
    -------
    modz_df : pandas.core.frame.DataFrame
        Consensus signatures with metadata for all replicates in the given DataFrame
    """
    population_features = population_df.columns.tolist()
    assert_error = f"{replicate_columns} not in input dataframe"
    if isinstance(replicate_columns, list):
        assert all(x in population_features for x in replicate_columns), assert_error  # noqa: S101
    elif isinstance(replicate_columns, str):
        assert replicate_columns in population_features, assert_error  # noqa: S101
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
