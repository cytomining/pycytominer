"""
Aggregate profiles based on given grouping variables.
"""

import numpy as np
import pandas as pd
from pycytominer.cyto_utils import (
    output,
    check_aggregate_operation,
    infer_cp_features,
)


def aggregate(
    population_df,
    strata=["Metadata_Plate", "Metadata_Well"],
    compute_object_count=False,
    object_feature="ObjectNumber",
    features="infer",
    operation="median",
    output_file="none",
    subset_data_df="none",
    compression_options=None,
    float_format=None,
):
    """
    Combine population dataframe variables by strata groups using given operation

    Arguments:
    population_df - pandas DataFrame to group and aggregate
    strata - [default: ["Metadata_Plate", "Metadata_Well"]] list indicating the columns to groupby and aggregate
    compute_object_count - [default: False] determine whether to compute object counts
    object_feature - [default: "ObjectNumber"] Object number feature
    features - [default: "all"] or list indicating features that should be aggregated
    operation - [default: "median"] a string indicating how the data is aggregated
                currently only supports one of ['mean', 'median']
    output_file - [default: "none"] if provided, will write aggregated profiles to file
                  if not specified, will return the aggregated profiles. We recommend
                  naming the file based on the plate name.
    subset_data_df - [default: "none"] a pandas dataframe indicating how to subset the input

    Return:
    Pandas DataFrame of aggregated features
    """
    # Check that the operation is supported
    operation = check_aggregate_operation(operation)

    # Subset the data to specified samples
    if isinstance(subset_data_df, pd.DataFrame):
        population_df = subset_data_df.merge(
            population_df, how="left", on=subset_data_df.columns.tolist()
        ).reindex(population_df.columns, axis="columns")

    # Subset dataframe to only specified variables if provided
    strata_df = population_df.loc[:, strata]
    if features == "infer":
        features = infer_cp_features(population_df)
        population_df = population_df.loc[:, features]
    else:
        population_df = population_df.loc[:, features]

    # Fix dtype of input features (they should all be floats!)
    convert_dict = {x: float for x in features}
    population_df = population_df.astype(convert_dict)

    # Merge back metadata used to aggregate by
    population_df = pd.concat([strata_df, population_df], axis="columns")

    # Perform aggregating function
    population_df = population_df.groupby(strata)

    if operation == "median":
        population_df = population_df.median().reset_index()
    else:
        population_df = population_df.mean().reset_index()

    # Compute objects counts
    if compute_object_count:
        count_object_df = population_df.loc[:, strata + [object_feature]]
        count_object_df = (
            count_object_df.groupby(strata)[object_feature]
            .count()
            .reset_index()
            .rename(columns={f'{object_feature}': f'Metadata_Object_Count'})
        )
        population_df = count_object_df.merge(population_df, on=strata, how='right')

    # Aggregated image number and object number do not make sense
    for col in ["ImageNumber", "ObjectNumber"]:
        if col in population_df.columns:
            population_df = population_df.drop([col], axis="columns")

    if output_file != "none":
        output(
            df=population_df,
            output_filename=output_file,
            compression_options=compression_options,
            float_format=float_format,
        )
    else:
        return population_df

    return population_df
