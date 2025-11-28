"""
Select features to use in downstream analysis based on specified selection method
"""

from typing import Any, Literal, Optional, Union

import pandas as pd

from pycytominer.cyto_utils import (
    drop_outlier_features,
    get_blocklist_features,
    infer_cp_features,
    load_profiles,
)
from pycytominer.cyto_utils.util import maybe_write_to_file
from pycytominer.operations import (
    correlation_threshold,
    get_na_columns,
    noise_removal,
    variance_threshold,
)


@maybe_write_to_file
def feature_select(
    profiles: Union[str, pd.DataFrame],
    features: Union[str, list[str]] = "infer",
    image_features: bool = False,
    samples: str = "all",
    operation: Union[str, list[str]] = "variance_threshold",
    output_file: Optional[str] = None,
    output_type: Optional[
        Literal["csv", "parquet", "anndata_h5ad", "anndata_zarr"]
    ] = "csv",
    na_cutoff: float = 0.05,
    corr_threshold: float = 0.9,
    corr_method: str = "pearson",
    freq_cut: float = 0.05,
    unique_cut: float = 0.01,
    compression_options: Optional[Union[str, dict[str, Any]]] = None,
    float_format: Optional[str] = None,
    blocklist_file: Optional[str] = None,
    outlier_cutoff: float = 500.0,
    noise_removal_perturb_groups: Optional[Union[str, list[str]]] = None,
    noise_removal_stdev_cutoff: Optional[float] = None,
) -> Union[pd.DataFrame, str]:
    """Performs feature selection based on the given operation.

    Parameters
    ----------
    profiles : pd.DataFrame or file
        DataFrame or file of profiles.
    features : list, default "infer"
        A list of strings corresponding to feature measurement column names in the
        `profiles` DataFrame. All features listed must be found in `profiles`.
        Defaults to "infer". If "infer", then assume CellProfiler features are those
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    image_features: bool, default False
        Whether the profiles contain image features.
    samples : str, default "all"
        Samples to provide operation on.
    operation: list of str or str, default "variance_threshold
        Operations to perform on the input profiles.
    output_file : str, optional
        If provided, will write feature selected profiles to file. If not specified, will
        return the feature selected profiles as output. We recommend that this output file be
        suffixed with "_normalized_variable_selected.csv".
    output_type : str, optional
        If provided, will write feature selected profiles as a specified file type (either CSV or parquet).
        If not specified and output_file is provided, then the file will be outputed as CSV as default.
    na_cutoff : float, default 0.05
        Proportion of missing values in a column to tolerate before removing.
    corr_threshold : float, default 0.9
        Value between (0, 1) to exclude features above if any two features are correlated above this threshold.
    corr_method : str, default "pearson"
        Correlation type to compute. Allowed methods are "spearman", "kendall" and "pearson".
    freq_cut : float, default 0.05
        Ratio (2nd most common feature val / most common). Must range between 0 and 1.
        Remove features lower than freq_cut. A low freq_cut will remove features
        that have large difference between the most common feature and second most
        common feature. (e.g. this will remove a feature: [1, 1, 1, 1, 0.01, 0.01, ...])
    unique_cut: float, default 0.01
        Ratio (num unique features / num samples). Must range between 0 and 1.
        Remove features less than unique cut. A low unique_cut will remove features
        that have very few different measurements compared to the number of samples.
    compression_options : str or dict, optional
        Contains compression options as input to
        pd.DataFrame.to_csv(compression=compression_options). pandas version >= 1.2.
    float_format : str, optional
        Decimal precision to use in writing output file as input to
        pd.DataFrame.to_csv(float_format=float_format). For example, use "%.3g" for 3
        decimal precision.
    blocklist_file : str, optional
        File location of datafrmame with with features to exclude. Note that if "blocklist" in operation then will remove standard blocklist
    outlier_cutoff : float, default 500
        The threshold at which the maximum or minimum value of a feature across a full experiment is excluded. Note that this procedure is typically applied after normalization.
    noise_removal_perturb_groups: str or list of str, optional
        Perturbation groups corresponding to rows in profiles or the the name of the metadata column containing this information.
    noise_removal_stdev_cutoff: float,optional
        Maximum mean feature standard deviation to be kept for noise removal, grouped by the identity of the perturbation from perturb_list. The data must already be normalized so that this cutoff can apply to all columns.

    Returns
    -------
    str or pd.DataFrame
        pd.DataFrame:
            The feature selected profile DataFrame. If output_file=None, then return the
            DataFrame. If you specify output_file, then write to file and do not return
            data.
        str:
            If output_file is provided, then the function returns the path to the
            output file.

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
        if not all(x in all_ops for x in operation):
            raise ValueError(
                f"Some operation(s) {operation} not supported. Choose {all_ops}"
            )
    elif isinstance(operation, str):
        if operation not in all_ops:
            raise ValueError(f"{operation} not supported. Choose {all_ops}")
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
            if (
                noise_removal_perturb_groups is None
                or noise_removal_stdev_cutoff is None
            ):
                raise ValueError(
                    "If using noise_removal, must provide both noise_removal_perturb_groups and noise_removal_stdev_cutoff"
                )

            exclude = noise_removal(
                population_df=profiles,
                features=features,
                samples=samples,
                noise_removal_perturb_groups=noise_removal_perturb_groups,
                noise_removal_stdev_cutoff=noise_removal_stdev_cutoff,
            )
        excluded_features += exclude
        features = [feat for feat in features if feat not in excluded_features]

    excluded_features = list(set(excluded_features))

    selected_df = profiles.drop(excluded_features, axis="columns")

    return selected_df
