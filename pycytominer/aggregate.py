"""
Aggregate single cell data based on given grouping variables
"""

import pandas as pd
from sqlalchemy import create_engine


def aggregate(
    population_df,
    strata=["Metadata_Plate", "Metadata_Well"],
    features="all",
    operation="median",
):
    """
    Combine population dataframe variables by strata groups using given operation

    Arguments:
    population_df - pandas DataFrame to group and aggregate
    strata - [default: ["Metadata_Plate", "Metadata_Well"]] list indicating the columns to groupby and aggregate
    features - [default: "all"] or list indicating features that should be aggregated
    operation - [default: "median"] a string indicating how the data is aggregated
                currently only supports one of ['mean', 'median']

    Return:
    Pandas DataFrame of aggregated features
    """

    operation = operation.lower()

    assert operation in ["mean", "median"], "operation must be one ['mean', 'median']"

    # Subset dataframe to only specified variables if provided
    if features != "all":
        strata_df = population_df.loc[:, strata]
        population_df = population_df.loc[:, features]
        population_df = pd.concat([strata_df, population_df], axis="columns")

    population_df = population_df.groupby(strata)

    if operation == "median":
        return population_df.median().reset_index()
    else:
        return population_df.mean().reset_index()


def aggregate_objects(
    sql_file,
    compartments,
    strata=["Metadata_Plate", "Metadata_Well"],
    features="all",
    operation="median",
    output_file="none",
):
    """
    Aggregate morphological profiles

    Arguments:
    sql_file - string or sqlalchemy connection
    compartments - list of table names to extract in the sql_file
    strata - [default: ["Metadata_Plate", "Metadata_Well"]] list indicating the columns to groupby and aggregate
    features - [default: "all"] or list indicating features that should be aggregated
    operation - [default: "median"] a string indicating how the data is aggregated
                currently only supports one of ['mean', 'median']
    output_file - [default: "none"] string if specified, write to location

    Return:
    Either the merged object file or write object to disk
    """
    # Check compartments specified
    valid_compartments = ["cells", "cytoplasm", "nuclei"]
    assert all(
        [x in valid_compartments for x in compartments]
    ), "compartment not supported, use one of {}".format(valid_compartments)

    # Connect to sqlite engine
    engine = create_engine(sql_file)
    conn = engine.connect()

    # Extract image table
    image_cols = "TableNumber, ImageNumber, Image_Metadata_Plate, Image_Metadata_Well"
    image_query = "select {} from image".format(image_cols)
    object_df = pd.read_sql(sql=image_query, con=conn)

    # Extract tables and join with image
    merge_cols = ["TableNumber", "ImageNumber"]
    for compartment in compartments:

        compartment_query = "select * from {}".format(compartment)

        object_df = object_df.merge(
            aggregate(
                population_df=(pd.read_sql(sql=compartment_query, con=conn)),
                strata=strata,
                features=features,
                operation=operation,
            ),
            how="inner",
            left_on=merge_cols,
            right_on=merge_cols,
        )

    if output_file != "none":
        object_df.to_csv(output_file)
    else:
        return object_df
