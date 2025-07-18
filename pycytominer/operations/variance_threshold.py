"""
Remove variables with near-zero variance.
Modified from caret::nearZeroVar()
"""

import numpy as np

from pycytominer.cyto_utils.features import infer_cp_features


def variance_threshold(
    population_df, features="infer", samples="all", freq_cut=0.05, unique_cut=0.01
):
    """Exclude features that have low variance (low information content)

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
    freq_cut : float, default 0.05
        Ratio (2nd most common feature val / most common). Must range between 0 and 1.
        Remove features lower than freq_cut. A low freq_cut will remove features
        that have large difference between the most common feature value and second most
        common feature value. (e.g. this will remove a feature: [1, 1, 1, 1, 0.01, 0.01, ...])
    unique_cut: float, default 0.01
        Ratio (num unique features / num samples). Must range between 0 and 1.
        Remove features less than unique cut. A low unique_cut will remove features
        that have very few different measurements compared to the number of samples.

    Returns
    -------
    excluded_features : list of str
         List of features to exclude from the population_df.

    """

    # check if freq_cut and unique_cut are between 0 and 1
    if not 0 <= freq_cut <= 1:
        raise ValueError("freq_cut variable must be between (0 and 1)")
    if not 0 <= unique_cut <= 1:
        raise ValueError("unique_cut variable must be between (0 and 1)")

    # Subset the DataFrame if specific samples are specified
    # If "all", use the entire DataFrame without subsetting
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

    # Exclude features based on frequency
    # Frequency is the ratio of the second most common value to the most common value.
    # Features with a frequency below the `freq_cut` threshold are flagged for exclusion.
    excluded_features_freq = population_df.apply(
        lambda x: calculate_frequency(x, freq_cut), axis="rows"
    )

    # Remove features with NA values
    excluded_features_freq = excluded_features_freq[
        excluded_features_freq.isna()
    ].index.tolist()

    # Get the number of samples
    n = population_df.shape[0]

    # Get the number of unique features
    num_unique_features = population_df.nunique()

    # Exclude features with too many (defined by unique_ratio) values in common, where
    # unique_ratio is defined as the number of unique features divided by the total
    # number of samples
    unique_ratio = num_unique_features / n
    unique_ratio = unique_ratio < unique_cut

    # Get the feature names that have a unique ratio less than the unique_cut
    # This represents features that have too few unique values compared to the number
    # of samples.
    excluded_features_unique = unique_ratio[unique_ratio].index.tolist()

    # Combine the two lists of features to exclude
    excluded_features = list(set(excluded_features_freq + excluded_features_unique))
    return excluded_features


def calculate_frequency(feature_column, freq_cut):
    """Calculate frequency of second most common to most common feature.
    Used in pandas.apply()

    Parameters
    ----------
    feature_column : pandas.core.series.series
        Pandas series of the specific feature in the population_df
    freq_cut : float, default 0.05
        Ratio (2nd most common feature val / most common). Must range between 0 and 1.
        Remove features lower than freq_cut. A low freq_cut will remove features
        that have large difference between the most common feature and second most
        common feature. (e.g. this will remove a feature: [1, 1, 1, 1, 0.01, 0.01, ...])

    Returns
    -------
    Feature name if it passes threshold, "NA" otherwise

    """

    val_count = feature_column.value_counts()
    try:
        max_count = val_count.iloc[0]
    except IndexError:
        return np.nan
    try:
        second_max_count = val_count.iloc[1]
    except IndexError:
        return np.nan

    freq = second_max_count / max_count

    if freq < freq_cut:
        return np.nan
    else:
        return feature_column.name
