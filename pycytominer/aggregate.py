"""
Aggregate profiles based on given grouping variables.
"""

from typing import Any, Optional, Union

import numpy as np
import pandas as pd

from pycytominer.cyto_utils import (
    check_aggregate_operation,
    infer_cp_features,
    output,
)


def aggregate(
    population_df: pd.DataFrame,
    strata: list[str] = ["Metadata_Plate", "Metadata_Well"],
    features: Union[list[str], str] = "infer",
    operation: str = "median",
    output_file: Optional[str] = None,
    output_type: Optional[str] = "csv",
    compute_object_count: bool = False,
    object_feature: str = "Metadata_ObjectNumber",
    subset_data_df: Optional[pd.DataFrame] = None,
    compression_options: Optional[Union[str, dict[str, Any]]] = None,
    float_format: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Combine population dataframe variables by strata groups using given operation.

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
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
    subset_data_df : pandas.core.frame.DataFrame
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
    population_df : pandas.core.frame.DataFrame, optional
        DataFrame of aggregated features. If output_file=None, then return the
        DataFrame. If you specify output_file, then write to file and do not return
        data.

    """

    # Check that the operation is supported
    operation = check_aggregate_operation(operation)

    # Subset the data to specified samples
    if isinstance(subset_data_df, pd.DataFrame):
        population_df = subset_data_df.merge(
            population_df, how="inner", on=subset_data_df.columns.tolist()
        ).reindex(population_df.columns, axis="columns")

    # Subset dataframe to only specified variables if provided
    strata_df = population_df[strata]

    # Only extract single object column in preparation for count
    if compute_object_count:
        count_object_df = (
            population_df.loc[:, list(np.union1d(strata, [object_feature]))]
            .groupby(strata)[object_feature]
            .count()
            .reset_index()
            .rename(columns={f"{object_feature}": "Metadata_Object_Count"})
        )

    if features == "infer":
        features = infer_cp_features(population_df)

    # recast as dataframe to protect against scenarios where a series may be returned
    population_df = pd.DataFrame(population_df[features])

    # Fix dtype of input features (they should all be floats!)
    population_df = population_df.astype(float)

    # Merge back metadata used to aggregate by
    population_df = pd.concat([strata_df, population_df], axis="columns")

    # Perform aggregating function
    # Note: type ignore added below to address the change in variable types for
    # label `population_df`.
    population_df = population_df.groupby(strata, dropna=False)  # type: ignore[assignment]

    if operation == "median":
        population_df = population_df.median().reset_index()
    else:
        population_df = population_df.mean().reset_index()

    # Compute objects counts
    if compute_object_count:
        population_df = count_object_df.merge(population_df, on=strata, how="right")

    # Aggregated image number and object number do not make sense
    if columns_to_drop := [
        column
        for column in population_df.columns
        if column in ["ImageNumber", "ObjectNumber"]
    ]:
        population_df = population_df.drop(columns=columns_to_drop, axis="columns")

    if output_file is not None:
        return output(
            df=population_df,
            output_filename=output_file,
            output_type=output_type,
            compression_options=compression_options,
            float_format=float_format,
        )
    else:
        return population_df
