"""
Select features to use in downstream analysis based on specified selection method
"""

import pandas
from pycytominer.correlation_threshold import correlation_threshold
from pycytominer.variance_threshold import variance_threshold
from pycytominer.get_na_columns import get_na_columns


def feature_select(
    population_df,
    features="none",
    samples="none",
    operation="variance_threshold",
    **kwargs
):
    """
    Performs feature selection based on the given operation

    Arguments:
    population_df - pandas DataFrame that includes metadata and observation variables
    features - list of cell painting features
               [default: "none"] - if "none", use all features
    samples - if provided, a list of samples to provide operation on
              [default: "none"] - if "none", use all samples to calculate
    operation - str or list of given operations to perform on input population_df
    """
    na_cutoff = kwargs.pop("na_cutoff", 0.05)
    corr_threshold = kwargs.pop("corr_threshold", 0.9)
    corr_method = kwargs.pop("corr_method", "pearson")
    freq_cut = kwargs.pop("freq_cut", 0.05)
    unique_cut = kwargs.pop("unique_cut", 0.1)

    all_ops = ["variance_threshold", "correlation_threshold", "drop_na_columns"]

    # Make sure the user provides a supported operation
    if isinstance(operation, list):
        assert all(
            [x in all_ops for x in operation]
        ), "Some operation(s) {} not supported. Choose {}".format(operation, all_ops)
    elif isinstance(operation, str):
        assert operation in all_ops, "{} not supported. Choose {}".format(
            operation, all_ops
        )
        operation = operation.split()
    else:
        return ValueError("Operation must be a list or string")

    excluded_features = []
    for op in operation:
        if op == "variance_threshold":
            exclude = variance_threshold(
                population_df=population_df,
                features=features,
                samples=samples,
                freq_cut=freq_cut,
                unique_cut=unique_cut,
            )
        elif op == "drop_na_columns":
            exclude = get_na_columns(
                population_df=population_df,
                features=features,
                samples=samples,
                cutoff=na_cutoff,
            )
        elif op == "correlation_threshold":
            exclude = correlation_threshold(
                population_df=population_df,
                features=features,
                samples=samples,
                threshold=corr_threshold,
                method=corr_method,
            )
        excluded_features += exclude

    excluded_features = list(set(excluded_features))

    return population_df.drop(excluded_features, axis="columns")
