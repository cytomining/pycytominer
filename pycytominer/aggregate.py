"""
Aggregate profiles based on given grouping variables.
"""

from typing import Any, Literal, Optional, Union

import narwhals.stable.v1 as nw
import pandas as pd

from pycytominer.cyto_utils import check_aggregate_operation, infer_cp_features
from pycytominer.cyto_utils.util import write_to_file_if_user_specifies_output_details


@write_to_file_if_user_specifies_output_details
def aggregate(
    population_df: pd.DataFrame,
    strata: Union[list[str], str] = ["Metadata_Plate", "Metadata_Well"],
    features: Union[list[str], str] = "infer",
    operation: str = "median",
    output_file: Optional[str] = None,
    output_type: Literal[
        "csv", "parquet", "anndata_h5ad", "anndata_zarr", None
    ] = "csv",
    compute_object_count: bool = False,
    object_feature: str = "Metadata_ObjectNumber",
    subset_data_df: Optional[Any] = None,
    compression_options: Optional[Union[str, dict[str, Any]]] = None,
    float_format: Optional[str] = None,
) -> Union[pd.DataFrame, str]:
    """Combine population dataframe variables by strata groups using given operation.

    Parameters
    ----------
    population_df : pd.DataFrame
        DataFrame to group and aggregate.
    strata : list of str, default ["Metadata_Plate", "Metadata_Well"]
        Columns to groupby and aggregate.
    features : list of str, default "infer"
        List of features that should be aggregated.
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
    subset_data_df : dataframe-like
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
    str or pd.DataFrame
        pd.DataFrame:
            DataFrame of aggregated features. If output_file=None, then return the
            DataFrame. If you specify output_file, then write to file and do not return
            data.
        str:
            If output_file is provided, then the function returns the path to the
            output file.
    """

    # Check that the operation is supported
    operation = check_aggregate_operation(operation)
    narwhals_df = nw.from_native(population_df, eager_only=True)
    population_columns = list(narwhals_df.columns)

    if isinstance(strata, str):
        strata = [strata]

    if subset_data_df is not None:
        subset_nw = nw.from_native(subset_data_df, eager_only=True)
        subset_columns = list(subset_nw.columns)
        subset_nw = nw.from_dict(
            subset_nw.to_dict(as_series=False),
            backend=nw.get_native_namespace(nw.to_native(narwhals_df)),
        )
        narwhals_df = subset_nw.join(
            narwhals_df, on=subset_columns, how="inner"
        ).select(*population_columns)

    if features == "infer":
        features = infer_cp_features(nw.to_native(narwhals_df))
    elif isinstance(features, str):
        features = [features]

    # Only extract single object column in preparation for count
    if compute_object_count:
        count_object_nw = narwhals_df.group_by(*strata, drop_null_keys=False).agg(
            nw.col(object_feature).count().alias("Metadata_Object_Count")
        )

    narwhals_df = narwhals_df.select(
        *strata,
        *[nw.col(feature).cast(nw.Float64).alias(feature) for feature in features],
    )
    aggregated = narwhals_df.group_by(*strata, drop_null_keys=False).agg(*[
        getattr(nw.col(feature), operation)().alias(feature) for feature in features
    ])
    if compute_object_count:
        aggregated = aggregated.join(count_object_nw, on=strata, how="left").select(
            *strata, "Metadata_Object_Count", *features
        )

    aggregated_native = nw.to_native(aggregated)
    if isinstance(aggregated_native, pd.DataFrame):
        population_df = aggregated_native
    elif hasattr(aggregated_native, "to_pandas"):
        population_df = aggregated_native.to_pandas()
    else:
        population_df = pd.DataFrame(aggregated_native)
    population_df = population_df.sort_values(by=strata).reset_index(drop=True)

    # Aggregated image number and object number do not make sense
    if columns_to_drop := [
        column
        for column in population_df.columns
        if column in ["ImageNumber", "ObjectNumber"]
    ]:
        population_df = population_df.drop(columns=columns_to_drop, axis="columns")

    return population_df
