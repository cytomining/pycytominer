"""
Select features to use in downstream analysis based on specified selection method
"""

import os
import pandas as pd

from pycytominer.correlation_threshold import correlation_threshold
from pycytominer.variance_threshold import variance_threshold
from pycytominer.get_na_columns import get_na_columns
from pycytominer.cyto_utils.compress import compress
from pycytominer.cyto_utils.features import get_blacklist_features


def feature_select(
    profiles,
    features="infer",
    samples="none",
    operation="variance_threshold",
    output_file="none",
    **kwargs
):
    """
    Performs feature selection based on the given operation

    Arguments:
    profiles - either pandas DataFrame or a file that stores profile data
    features - list of cell painting features [default: "infer"]
               if "infer", then assume cell painting features are those that do not
               start with "Metadata_"
    samples - if provided, a list of samples to provide operation on
              [default: "none"] - if "none", use all samples to calculate
    operation - str or list of given operations to perform on input profiles
    output_file - [default: "none"] if provided, will write annotated profiles to file
                  if not specified, will return the annotated profiles. We recommend
                  that this output file be suffixed with
                  "_normalized_variable_selected.csv".
    """
    na_cutoff = kwargs.pop("na_cutoff", 0.05)
    corr_threshold = kwargs.pop("corr_threshold", 0.9)
    corr_method = kwargs.pop("corr_method", "pearson")
    freq_cut = kwargs.pop("freq_cut", 0.05)
    unique_cut = kwargs.pop("unique_cut", 0.1)
    how = kwargs.pop("how", None)
    float_format = kwargs.pop("float_format", None)
    blacklist_file = kwargs.pop("blacklist_file", None)

    all_ops = [
        "variance_threshold",
        "correlation_threshold",
        "drop_na_columns",
        "blacklist",
    ]

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

    # Load Data
    if not isinstance(profiles, pd.DataFrame):
        try:
            profiles = pd.read_csv(profiles)
        except FileNotFoundError:
            raise FileNotFoundError("{} profile file not found".format(profiles))

    if features == "infer":
        features = [
            x for x in profiles.columns.tolist() if not x.startswith("Metadata_")
        ]

    excluded_features = []
    for op in operation:
        if op == "variance_threshold":
            exclude = variance_threshold(
                population_df=profiles,
                features=features,
                samples=samples,
                freq_cut=freq_cut,
                unique_cut=unique_cut,
            )
        elif op == "drop_na_columns":
            exclude = get_na_columns(
                population_df=profiles,
                features=features,
                samples=samples,
                cutoff=na_cutoff,
            )
        elif op == "correlation_threshold":
            exclude = correlation_threshold(
                population_df=profiles,
                features=features,
                samples=samples,
                threshold=corr_threshold,
                method=corr_method,
            )
        elif op == "blacklist":
            if blacklist_file:
                exclude = get_blacklist_features(population_df=profiles, blacklist_file=blacklist_file)
            else:
                exclude = get_blacklist_features(population_df=profiles)

        excluded_features += exclude

    excluded_features = list(set(excluded_features))

    selected_df = profiles.drop(excluded_features, axis="columns")

    if output_file != "none":
        compress(
            df=selected_df,
            output_filename=output_file,
            how=how,
            float_format=float_format,
        )
    else:
        return selected_df
