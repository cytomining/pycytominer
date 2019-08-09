"""
Select features to use in downstream analysis based on specified selection method
"""

import pandas
from correlation_threshold import correlation_threshold


def feature_select(
    population_df, features, samples="none", operation="variance_threshold", **kwargs
):
    """
    Performs feature selection based on the given operation

    Arguments:
    population_df - pandas DataFrame that includes metadata and observation variables
    features - list of cell painting features
    samples - if provided, a list of samples to provide operation on
              [default: "none"] - if "none", use all samples to calculate
    operation - str or list of given operations to perform on input population_df
    """
    na_cutoff = kwargs.pop("na_cutoff", 0.95)
    corr_threshold = kwargs.pop("corr_threshold", 0.9)
    corr_method = kwargs.pop("corr_method", "pearson")
    freq_cut = kwargs.pop("freq_cut", 0.05)
    unique_cut = kwargs.pop("unique_cut", 0.1)

    all_ops = ["variance_threshold", "correlation_threshold", "drop_na_columns"]

    # Make sure the user provides a supported operation
    if isinstance(operation, str):
        assert operation in all_ops, "{} not supported. Choose {}".format(
            operation, all_ops
        )
        operation = list(operation)
    elif isinstance(operation, list):
        assert all(
            [x in all_ops for x in operation]
        ), "Some operation(s) {} not supported. Choose {}".format(operation, all_ops)

    feature_df = population_df.loc[:, features]

    excluded_features = []
    for op in operation:
        if op == "variance_threshold":
            excluded_features = variance_threshold(
                population_df=feature_df, samples=samples
            )
        elif op == "drop_na_columns":
            na_count = feature_df.isna().sum()
            na_prop = na_count / feature_df.shape[0]
            excluded_features = na_prop > na_cutoff
            excluded_features = excluded_features[include_features].index.tolist()
        elif op == "correlation_threshold":
            excluded_features = correlation_threshold(
                population_df=feature_df,
                samples=samples,
                threshold=corr_threshold,
                method=corr_method,
            )

        excluded_features += excluded_features

    return population_df.drop(excluded_features, axis="columns")
