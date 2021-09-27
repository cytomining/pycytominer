"""
Select features to use in downstream analysis based on specified selection method
"""

import os
import pandas as pd

from pycytominer.operations import (
    correlation_threshold,
    variance_threshold,
    get_na_columns,
    noise_removal,
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
    image_features=False,
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
    noise_removal_perturb_groups=None,
    noise_removal_stdev_cutoff=None,
):
    """Performs feature selection based on the given operation.

    Parameters
    ----------
    profiles : pandas.core.frame.DataFrame or file
        DataFrame or file of profiles.
    features : list
        A list of strings corresponding to feature measurement column names in the
        `profiles` DataFrame. All features listed must be found in `profiles`.
        Defaults to "infer". If "infer", then assume cell painting features are those
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    image_features: bool, default False
        Whether the profiles contain image features.
    samples : list or str, default "all"
        Samples to provide operation on.
    operation: list of str or str, default "variance_threshold
        Operations to perform on the input profiles.
    output_file : str, optional
        If provided, will write annotated profiles to file. If not specified, will
        return the normalized profiles as output. We recommend that this output file be
        suffixed with "_normalized_variable_selected.csv".
    na_cutoff : float, default 0.05
        Proportion of missing values in a column to tolerate before removing.
    corr_threshold : float, default 0.1
        Value between (0, 1) to exclude features above if any two features are correlated above this threshold.
    corr_method : str, default "pearson"
        Correlation type to compute. Allowed methods are "spearman", "kendall" and "pearson".
    freq_cut : float, default 0.05
        Ratio (2nd most common feature val / most common).
    unique_cut: float, default 0.01
        Ratio (num unique features / num samples).
    compression_options : str or dict, optional
        Contains compression options as input to
        pd.DataFrame.to_csv(compression=compression_options). pandas version >= 1.2.
    float_format : str, optional
        Decimal precision to use in writing output file as input to
        pd.DataFrame.to_csv(float_format=float_format). For example, use "%.3g" for 3
        decimal precision.
    blocklist_file : str, optional
        File location of datafrmame with with features to exclude. Note that if "blocklist" in operation then will remove standard blocklist
    outlier_cutoff : float, default 15
        The threshold at which the maximum or minimum value of a feature across a full experiment is excluded. Note that this procedure is typically applied (and therefore the default is uitable) for after normalization.
    noise_removal_perturb_groups: str or list of str, optional
        Perturbation groups corresponding to rows in profiles or the the name of the metadata column containing this information.
    noise_removal_stdev_cutoff: float,optional
        Maximum mean feature standard deviation to be kept for noise removal, grouped by the identity of the perturbation from perturb_list. The data must already be normalized so that this cutoff can apply to all columns.

    Returns
    -------
    selected_df : pandas.core.frame.DataFrame, optional
        The feature selected profile DataFrame. If output_file="none", then return the
        DataFrame. If you specify output_file, then write to file and do not return
        data.

    """

    all_ops = [
        "variance_threshold",
        "correlation_threshold",
        "drop_na_columns",
        "blocklist",
        "drop_outliers",
        "noise_removal",
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
        features = infer_cp_features(profiles, image_features=image_features)

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
        elif op == "noise_removal":
            exclude = noise_removal(
                population_df=profiles,
                features=features,
                noise_removal_perturb_groups=noise_removal_perturb_groups,
                noise_removal_stdev_cutoff=noise_removal_stdev_cutoff,
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
