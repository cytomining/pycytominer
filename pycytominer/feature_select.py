"""
Select features to use in downstream analysis based on specified selection method
"""

import os
import pandas as pd

from pycytominer.operations import (
    correlation_threshold,
    variance_threshold,
    get_na_columns,
)
from pycytominer.cyto_utils import (
    load_profiles,
    output,
    get_blocklist_features,
    infer_cp_features,
    drop_outlier_features,
)


def feature_select(
    profiles,
    features="infer",
    samples="all",
    operation="variance_threshold",
    output_file="none",
    na_cutoff=0.05,
    corr_threshold=0.9,
    corr_method="pearson",
    freq_cut=0.05,
    unique_cut=0.1,
    compression_options=None,
    float_format=None,
    blocklist_file=None,
    outlier_cutoff=15,
):
    """
    Performs feature selection based on the given operation

    Arguments:
    profiles - either pandas DataFrame or a file that stores profile data
    features - [default: "infer"] list of cell painting features
               if "infer", then assume cell painting features are those that start with
               "Cells", "Nuclei", or "Cytoplasm"
    samples - [default: "all"] if provided, a list of samples to provide operation on
              if "all", use all samples to calculate
    operation - [default: "variance_threshold"] str or list of given operations to perform on input profiles.
                See all_ops for available operations.
    output_file - [default: "none"] if provided, will write annotated profiles to file
                  if not specified, will return the annotated profiles. We recommend
                  that this output file be suffixed with
                  "_normalized_variable_selected.csv".
    na_cutoff - [default: 0.05] proportion of missing values in a column to tolerate before removing
    corr_threshold - [default: 0.9] float between (0, 1) to exclude features above if any
                     two features are correlated above this threshold.
    freq_cut - [default: 0.1] float of ratio (2nd most common feature val / most common)
    unique_cut - [default: 0.1] float of ratio (num unique features / num samples)
    compression - [default: None] the mechanism to compress. See cyto_utils/output.py for options.
    float_format - [default: None] decimal precision to use in writing output file
                   For example, use "%.3g" for 3 decimal precision.
    blocklist_file - [default: None] file location of dataframe with features to exclude
                     Note that if "blocklist" in operation then will remove standard
                     blocklist
    outlier_cutoff - [default: 15] the threshold at which the maximum or minimum value of a feature
                     across a full experiment is excluded. Note that this
                     procedure is typically applied (and therefore the default is
                     suitable) for after normalization.
    """
    all_ops = [
        "variance_threshold",
        "correlation_threshold",
        "drop_na_columns",
        "blocklist",
        "drop_outliers",
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
    profiles = load_profiles(profiles)

    if features == "infer":
        features = infer_cp_features(profiles)

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
        elif op == "blocklist":
            if blocklist_file:
                exclude = get_blocklist_features(
                    population_df=profiles, blocklist_file=blocklist_file
                )
            else:
                exclude = get_blocklist_features(population_df=profiles)
        elif op == "drop_outliers":
            exclude = drop_outlier_features(
                population_df=profiles,
                features=features,
                samples=samples,
                outlier_cutoff=outlier_cutoff,
            )

        excluded_features += exclude

    excluded_features = list(set(excluded_features))

    selected_df = profiles.drop(excluded_features, axis="columns")

    if output_file != "none":
        output(
            df=selected_df,
            output_filename=output_file,
            compression_options=compression_options,
            float_format=float_format,
        )
    else:
        return selected_df
