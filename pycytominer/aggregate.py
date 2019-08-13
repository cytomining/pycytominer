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
        load_image_data=True,
        subsample_frac=1,
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
        subsample_frac - [default: 1] float (0 < subsample <= 1) indicating percentage of
                         single cells to select
        """
        # Check compartments specified
        self._check_compartments(compartments)

        # Check if correct operation is specified
        assert operation in [
            "mean",
            "median",
        ], "operation must be one ['mean', 'median']"

        # Check that the subsample_fraction is between 0 and 1
        assert (
            0 < subsample_frac and 1 >= subsample_frac
        ), "subsample_frac must be between 0 and 1"

        self.sql_file = sql_file
        self.strata = strata
        self.features = features
        self.operation = operation.lower()
        self.output_file = output_file
        self.compartments = compartments
        self.merge_cols = merge_cols
        self.subsample_frac = subsample_frac

        # Connect to sqlite engine
        self.engine = create_engine(self.sql_file)
        self.conn = self.engine.connect()

        if load_image_data:
            self.load_image()

    def _check_compartments(self, compartments):
        valid_compartments = ["cells", "cytoplasm", "nuclei"]
        error_str = "compartment not supported, use one of {}".format(
            valid_compartments
        )
        if isinstance(compartments, list):
            assert all([x in valid_compartments for x in compartments]), error_str
        elif isinstance(compartments, str):
            assert compartments in valid_compartments, error_str

    def set_subsample_frac(self, subsample_frac):
        self.subsample_frac = subsample_frac

    def load_image(self):
        """
        Load image table from sqlite file
        """
        # Extract image metadata
        image_cols = (
            "TableNumber, ImageNumber, Image_Metadata_Plate, Image_Metadata_Well"
        )
        image_query = "select {} from image".format(image_cols)
        self.image_df = pd.read_sql(sql=image_query, con=self.conn)

    def subsample_profiles(self):
        """
        Sample a Pandas DataFrame given the subsampling fraction
        """
        return pd.DataFrame.sample(x, frac=self.subsample_frac)

    def get_subsample(self, compartment="cells"):
        """
        Extract subsample from sqlite file

        Arguments:
        compartment - [default: "cells"] string indicating the compartment to subset
        """
        self._check_compartments(compartment)

        subset_data = pd.read_sql(sql=compartment_query, con=self.conn).apply(
            subsample_profiles
        )

        return subset_data

    def aggregate_compartment(self, compartment, compute_subsample=False):
        """
        Aggregate morphological profiles

        Arguments:
        compartment - str indicating specific compartment to extract

        Return:
        Either the merged object file or write object to disk
        """
        self._check_compartments(compartment)

        compartment_query = "select * from {}".format(compartment)

        if self.subset_frac == 1 and not compute_subsample:
            subset_data = "none"
        else:
            subset_data = self.get_subsample(compartment=compartment)

        object_df = aggregate(
            population_df=self.image_df.merge(
                pd.read_sql(sql=compartment_query, con=self.conn),
                how="inner",
                on=self.merge_cols,
            ),
            strata=self.strata,
            features=self.features,
            operation=self.operation,
        )

        return object_df

    def aggregate_profiles(self):
        # TODO: Might have data duplicated - check other columns
        # TODO: Need to also join on ObjectNumber (check if this is actually consistent (assume))
        aggregated = (
            self.aggregate_compartment(compartment="cells")
            .merge(
                self.aggregate_compartment(compartment="cytoplasm"),
                on=self.merge_cols,
                how="inner",
            )
            .merge(
                self.aggregate_compartment(compartment="nuclei"),
                on=self.merge_cols,
                how="inner",
            )
        )

        if self.output_file != "none":
            aggregated.to_csv(self.output_file)
        else:
            return aggregated


def aggregate(
    population_df,
    strata=["Metadata_Plate", "Metadata_Well"],
    features="all",
    operation="median",
    subset_data="none",
):
    """
    Combine population dataframe variables by strata groups using given operation

    Arguments:
    population_df - pandas DataFrame to group and aggregate
    strata - [default: ["Metadata_Plate", "Metadata_Well"]] list indicating the columns to groupby and aggregate
    features - [default: "all"] or list indicating features that should be aggregated
    operation - [default: "median"] a string indicating how the data is aggregated
                currently only supports one of ['mean', 'median']
    subset_data - [default: "none"] a pandas dataframe indicating how to subset the input

    Return:
    Pandas DataFrame of aggregated features
    """
    # Subset dataframe to only specified variables if provided
    if features != "all":
        strata_df = population_df.loc[:, strata]
        population_df = population_df.loc[:, features]
        population_df = pd.concat([strata_df, population_df], axis="columns")

    # TODO: this needs to be fixed
    if subset_data != "none":
        population_df = population_df.subset(subset_data)

    population_df = population_df.groupby(strata)

    if operation == "median":
        return population_df.median().reset_index()
    else:
        return population_df.mean().reset_index()
