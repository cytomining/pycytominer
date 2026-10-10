"""
Aggregate profiles based on given grouping variables.
"""

from typing import Any, Literal, Optional, Union

import numpy as np
import pandas as pd

from pycytominer.cyto_utils import check_aggregate_operation, infer_cp_features
from pycytominer.cyto_utils.util import (
    deprecate_renamed_parameter,
    write_to_file_if_user_specifies_output_details,
)


@deprecate_renamed_parameter(old_name="population_df", new_name="profiles")
@write_to_file_if_user_specifies_output_details
def aggregate(
    profiles: pd.DataFrame,
    strata: list[str] = ["Metadata_Plate", "Metadata_Well"],
    features: Union[list[str], str] = "infer",
    image_features: bool = False,
    operation: str = "median",
    output_file: Optional[str] = None,
    output_type: Literal[
        "csv", "parquet", "anndata_h5ad", "anndata_zarr", None
    ] = "csv",
    compute_object_count: bool = False,
    object_feature: str = "Metadata_ObjectNumber",
    subset_data_df: Optional[pd.DataFrame] = None,
    compression_options: Optional[Union[str, dict[str, Any]]] = None,
    float_format: Optional[str] = None,
) -> pd.DataFrame:
    """Combine profiles by strata groups using given operation.

    Parameters
    ----------
    profiles : pd.DataFrame
        DataFrame containing single-cell profiles to be aggregated according
        to the specified grouping criteria.

        .. deprecated:: 2.0
            The previous name of this parameter, ``population_df``, is still
            accepted as a keyword argument but emits a ``DeprecationWarning``.
            Use ``profiles`` instead. ``population_df`` will be removed in a
            future release.
    strata : list of str, default ["Metadata_Plate", "Metadata_Well"]
        Columns to groupby and aggregate.
    features : list of str, default "infer"
        List of features that should be aggregated.
    image_features : bool, default False
        Whether to include inferred ``Image_*`` feature columns. When True,
        Pycytominer preserves numeric image-level measurements while excluding
        non-numeric ``Image_*`` columns, which helps avoid treating image
        payload columns as profile features in mixed tables such as
        OME-Arrow-backed inputs.
    operation : str, default "median"
        How the data is aggregated. Currently only supports one of ['mean', 'median'].
    output_file : str or file handle, optional
        If provided, will write aggregated profiles to file. If not specified, will return the aggregated profiles.
        We recommend naming the file based on the plate name.
    output_type : str, optional
        If provided, will write aggregated profiles as a specified file type (either CSV or parquet).
        If not specified and output_file is provided, then the file will be outputed as CSV as default.
    compute_object_count : bool, default False
        Whether or not to compute object counts.
    object_feature : str, default "Metadata_ObjectNumber"
        Object number feature. Only used if compute_object_count=True.
    subset_data_df : pd.DataFrame
        How to subset the input.
    compression_options : str or dict, optional
        Contains compression options as input to
        pd.DataFrame.to_csv(compression=compression_options). pandas version >= 1.2.
    float_format : str, optional
        Decimal precision to use in writing output file as input to
        pd.DataFrame.to_csv(float_format=float_format). For example, use "%.3g" for 3
        decimal precision.

    Returns
    -------
    pd.DataFrame
        DataFrame of aggregated features. If output_file=None, then return the
        DataFrame. If you specify output_file, profiles will be written on disk
        based on provided output_file path.

    Notes
    -----
    Parameters: `output_file`, `output_type`, `compression_options`, and `float_format`
    are passed as kwargs to the `write_to_file_if_user_specifies_output_details` decorator,
    which handles writing the output DataFrame to file if the user specifies output
    details. If `output_file` is not specified, the function will return the aggregated
    DataFrame instead of writing to file.
    """

    # Check that the operation is supported
    operation = check_aggregate_operation(operation)

    # Subset the data to specified samples
    if isinstance(subset_data_df, pd.DataFrame):
        profiles = subset_data_df.merge(
            profiles, how="inner", on=subset_data_df.columns.tolist()
        ).reindex(profiles.columns, axis="columns")

    # Subset dataframe to only specified variables if provided
    strata_df = profiles[strata]

    # Only extract single object column in preparation for count
    if compute_object_count:
        count_object_df = (
            profiles
            .loc[:, list(np.union1d(strata, [object_feature]))]
            .groupby(strata)[object_feature]
            .count()
            .reset_index()
            .rename(columns={f"{object_feature}": "Metadata_Object_Count"})
        )

    if features == "infer":
        features = infer_cp_features(profiles, image_features=image_features)

    # recast as dataframe to protect against scenarios where a series may be returned
    profiles = pd.DataFrame(profiles[features])

    # Fix dtype of input features (they should all be floats!)
    profiles = profiles.astype(float)

    # Merge back metadata used to aggregate by
    profiles = pd.concat([strata_df, profiles], axis="columns")

    # Perform aggregating function
    # Note: type ignore added below to address the change in variable types for
    # label `profiles`.
    profiles = profiles.groupby(strata, dropna=False)  # type: ignore[assignment]

    if operation == "median":
        profiles = profiles.median().reset_index()
    else:
        profiles = profiles.mean().reset_index()

    # Compute objects counts
    if compute_object_count:
        profiles = count_object_df.merge(profiles, on=strata, how="right")

    # Aggregated image number and object number do not make sense
    if columns_to_drop := [
        column
        for column in profiles.columns
        if column in ["ImageNumber", "ObjectNumber"]
    ]:
        profiles = profiles.drop(columns=columns_to_drop, axis="columns")

    return profiles
