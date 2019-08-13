"""
Aggregate single cell data based on given grouping variables
"""

import pandas as pd
from sqlalchemy import create_engine


class AggregateProfiles:
    """
    Class to aggregate single cell morphological profiles
    """

    def __init__(
        self,
        sql_file,
        strata=["Metadata_Plate", "Metadata_Well"],
        features="all",
        operation="median",
        output_file="none",
        compartments=["cells", "cytoplasm", "nuclei"],
        merge_cols=["TableNumber", "ImageNumber"],
        subsample=1,
    ):
        """
        Arguments:
        sql_file - string or sqlalchemy connection
        strata - [default: ["Metadata_Plate", "Metadata_Well"]] list indicating the columns to groupby and aggregate
        features - [default: "all"] or list indicating features that should be aggregated
        operation - [default: "median"] a string indicating how the data is aggregated
                    currently only supports one of ['mean', 'median']
        output_file - [default: "none"] string if specified, write to location
        compartments - list of compartments to process
        merge_cols - column indicating which columns to merge compartments using
        subsample - [default: 1] float (0 < subsample <= 1) indicating percentage of
                    single cells to select
        """
        # Check compartments specified
        valid_compartments = ["cells", "cytoplasm", "nuclei"]
        assert all(
            [x in valid_compartments for x in compartments]
        ), "compartment not supported, use one of {}".format(valid_compartments)

        # Check if correct operation is specified
        assert operation in [
            "mean",
            "median",
        ], "operation must be one ['mean', 'median']"

        self.sql_file = sql_file
        self.strata = strata
        self.features = features
        self.operation = operation.lower()
        self.output_file = output_file
        self.compartments = compartments
        self.merge_cols = merge_cols

        # Connect to sqlite engine
        self.engine = create_engine(self.sql_file)
        self.conn = self.engine.connect()

    def aggregate_compartment(self, compartment):
        """
        Aggregate morphological profiles

        Arguments:

        compartment - str indicating specific compartment to extract

        Return:
        Either the merged object file or write object to disk
        """
        # Extract image table
        image_cols = (
            "TableNumber, ImageNumber, Image_Metadata_Plate, Image_Metadata_Well"
        )
        image_query = "select {} from image".format(image_cols)
        object_df = pd.read_sql(sql=image_query, con=conn)

        compartment_query = "select * from {}".format(compartment)

        object_df = object_df.merge(
            aggregate(
                population_df=(pd.read_sql(sql=compartment_query, con=self.conn)),
                strata=self.strata,
                features=self.features,
                operation=self.operation,
            ),
            how="inner",
            on=self.merge_cols,
        )

        return object_df

    def aggregate_profiles(self):
        aggregated = (
            self.aggregate_compartment(sql_file=sql_file, compartment="cells")
            .merge(
                self.aggregate_compartment(sql_file=sql_file, compartment="cytoplasm"),
                on=self.merge_cols,
                how="inner",
            )
            .merge(
                self.aggregate_compartment(sql_file=sql_file, compartment="nuclei"),
                on=self.merge_cols,
                how="inner",
            )
        )

        if output_file != "none":
            aggregated.to_csv(output_file)
        else:
            return aggregated


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
