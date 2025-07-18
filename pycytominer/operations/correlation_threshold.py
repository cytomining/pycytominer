"""
Returns list of features such that no two features have a correlation greater than a
specified threshold
"""

from pycytominer.cyto_utils.features import infer_cp_features
from pycytominer.cyto_utils.util import (
    check_correlation_method,
    get_pairwise_correlation,
)


def correlation_threshold(
    population_df, features="infer", samples="all", threshold=0.9, method="pearson"
):
    """Exclude features that have correlations above a certain threshold

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
        DataFrame that includes metadata and observation features.
    features : list, default "infer"
        A list of strings corresponding to feature measurement column names in the
        `population_df` DataFrame. All features listed must be found in `population_df`.
        Defaults to "infer". If "infer", then assume CellProfiler features are those
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    samples : str, default "all"
        List of samples to perform operation on. The function uses a pd.DataFrame.query()
        function, so you should  structure samples in this fashion. An example is
        "Metadata_treatment == 'control'" (include all quotes).
        If "all", use all samples to calculate.
    threshold - float, default 0.9
        Must be between (0, 1) to exclude features
    method - str, default "pearson"
        indicating which correlation metric to use to test cutoff

    Returns
    -------
    excluded_features : list of str
         List of features to exclude from the population_df.
    """

    # Checking if the provided correlation method is supported
    method = check_correlation_method(method)

    # Checking if the threshold is between 0 and 1
    if not 0 <= threshold <= 1:
        raise ValueError("threshold variable must be between (0 and 1)")

    # Subset dataframe and calculate correlation matrix across subset features
    # If samples is not 'all', then subset the dataframe
    if samples != "all":
        # Using pandas query to filter rows based on the conditions provided in the
        # samples parameter
        population_df = population_df.query(expr=samples)

    # Infer CellProfiler features if 'features' is set to 'infer'
    if features == "infer":
        # Infer CellProfiler features
        features = infer_cp_features(population_df)

    # Subset the DataFrame to only include the features of interest
    population_df = population_df.loc[:, features]

    # Get correlation matrix and lower triangle of pairwise correlations in long format
    data_cor_df, pairwise_df = get_pairwise_correlation(
        population_df=population_df, method=method
    )

    # Get absolute sum of correlation across features
    # The lower the index, the less correlation to the full data frame
    # We want to drop features with highest correlation, so drop higher index
    variable_cor_sum = data_cor_df.abs().sum().sort_values().index

    # And subset to only variable combinations that pass the threshold
    pairwise_df = pairwise_df.query("correlation > @threshold")

    # Return an empty list if nothing is over correlation threshold
    if pairwise_df.shape[0] == 0:
        return []

    # Output the excluded features
    excluded = pairwise_df.apply(
        lambda x: determine_high_cor_pair(x, variable_cor_sum), axis="columns"
    )

    return list(set(excluded.tolist()))


def determine_high_cor_pair(correlation_row, sorted_correlation_pairs):
    """Select highest correlated variable given a correlation row with columns:
    ["pair_a", "pair_b", "correlation"]. For use in a pandas.apply().

    Parameters
    ----------
    correlation_row : pandas.core.series.series
        Pandas series of the specific feature in the pairwise_df
    sorted_correlation_pairs : pandas.DataFrame.index
        A sorted object by total correlative sum to all other features

    Returns
    -------
    The feature that has a lower total correlation sum with all other features
    """

    pair_a = correlation_row["pair_a"]
    pair_b = correlation_row["pair_b"]

    if sorted_correlation_pairs.get_loc(pair_a) > sorted_correlation_pairs.get_loc(
        pair_b
    ):
        return pair_a
    else:
        return pair_b
