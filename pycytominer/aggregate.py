"""
Aggregate single cell data based on given grouping variables
"""

import numpy as np
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
        subsample_n="all",
        subsampling_random_state="none",
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
        subsample_n - [default: "all"] int indicating how many samples to include
        subsampling_random_state - [default: "none"] the random state to init subsample
        """
        # Check compartments specified
        self._check_compartments(compartments)

        # Check if correct operation is specified
        assert operation in [
            "mean",
            "median",
        ], "operation must be one ['mean', 'median']"

        # Check that the subsample_frac is between 0 and 1
        assert (
            0 < subsample_frac and 1 >= subsample_frac
        ), "subsample_frac must be between 0 and 1"

        # Check that the user didn't specify both subset frac and
        assert (
            subsample_frac == 1 or subsample_n == "all"
        ), "Do not set both subsample_frac and subsample_n"

        self.sql_file = sql_file
        self.strata = strata
        self.features = features
        self.operation = operation.lower()
        self.output_file = output_file
        self.compartments = compartments
        self.merge_cols = merge_cols
        self.subsample_frac = subsample_frac
        self.subsample_n = subsample_n
        self.subset_data = "none"
        self.subsampling_random_state = subsampling_random_state
        self.is_aggregated = False

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

    def set_output_file(self, output_file):
        self.output_file = output_file

    def set_subsample_frac(self, subsample_frac):
        self.subsample_n = "all"
        self.subsample_frac = subsample_frac

    def set_subsample_n(self, subsample_n):
        self.subsample_frac = 1
        self.subsample_n = subsample_n

    def load_image(self):
        """
        Load image table from sqlite file
        """
        # Extract image metadata
        image_cols = (
            "TableNumber, ImageNumber, {}".format(", ".join(self.strata))
        )
        image_query = "select {} from image".format(image_cols)
        self.image_df = pd.read_sql(sql=image_query, con=self.conn)

    def count_cells(self, compartment="cells", count_subset=False):
        """
        Determine how many cells are measured per well

        Arguments:
        compartment - string indicating the compartment to subset
        count_subset - [default: False] count the number of cells in subset partition
        """
        self._check_compartments(compartment)

        if count_subset:
            assert self.is_aggregated, "Make sure to aggregate_profiles() first!"
            count_df = pd.crosstab(
                self.subset_data.loc[:, self.strata[1]],
                self.subset_data.loc[:, self.strata[0]],
            ).reset_index()
        else:
            query_cols = "TableNumber, ImageNumber, ObjectNumber"
            query = "select {} from {}".format(query_cols, compartment)
            count_df = self.image_df.merge(
                pd.read_sql(sql=query, con=self.conn), how="inner", on=self.merge_cols
            )

            count_df = pd.crosstab(
                count_df.loc[:, self.strata[1]], count_df.loc[:, self.strata[0]]
            ).reset_index()

        return count_df

    def subsample_profiles(self, x, random_state="none"):
        """
        Sample a Pandas DataFrame given the subsampling fraction
        """
        if random_state == "none" and self.subsampling_random_state == "none":
            self.subsampling_random_state = np.random.randint(0, 10000, size=1)[0]

        if self.subsample_frac == 1:
            return pd.DataFrame.sample(
                x,
                n=self.subsample_n,
                replace=True,
                random_state=self.subsampling_random_state,
            )
        else:
            return pd.DataFrame.sample(
                x, frac=self.subsample_frac, random_state=self.subsampling_random_state
            )

    def get_subsample(self, compartment="cells", subsample_frac=1, subsample_n="all"):
        """
        Extract subsample from sqlite file

        Arguments:
        compartment - [default: "cells"] string indicating the compartment to subset
        """
        self._check_compartments(compartment)

        if subsample_frac < 1:
            self.set_subsample_frac(subsample_frac)

        if isinstance(subsample_n, int):
            self.set_subsample_n(subsample_n)

        query_cols = "TableNumber, ImageNumber, ObjectNumber"
        query = "select {} from {}".format(query_cols, compartment)

        # Load query and merge with image_df
        query_df = self.image_df.merge(
            pd.read_sql(sql=query, con=self.conn), how="inner", on=self.merge_cols
        )

        self.subset_data = (
            query_df.groupby(self.strata)
            .apply(lambda x: self.subsample_profiles(x))
            .reset_index(drop=True)
        )

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

        if (self.subsample_frac < 1 or self.subsample_n != "all") and compute_subsample:
            self.get_subsample(compartment=compartment)

        object_df = aggregate(
            population_df=self.image_df.merge(
                pd.read_sql(sql=compartment_query, con=self.conn),
                how="inner",
                on=self.merge_cols,
            ),
            strata=self.strata,
            features=self.features,
            operation=self.operation,
            subset_data=self.subset_data,
        )

        return object_df

    def aggregate_profiles(self, compute_subsample="False", output_file="none"):
        """
        Aggregate and merge compartments. This is the primary entry to this class.

        Arguments:
        compute_subsample - [default: False] boolean if subsample should be computed.
                            NOTE: Must be specified to perform subsampling. Will not
                            apply subsetting if set to False even if subsample is
                            initialized

        Return:
        if output_file is set, then write to file. If not then return
        """
        if output_file != "none":
            self.set_output_file(output_file)

        aggregated = (
            self.aggregate_compartment(
                compartment="cells", compute_subsample=compute_subsample
            )
            .merge(
                self.aggregate_compartment(compartment="cytoplasm"),
                on=self.strata,
                how="inner",
            )
            .merge(
                self.aggregate_compartment(compartment="nuclei"),
                on=self.strata,
                how="inner",
            )
        )

        self.is_aggregated = True

        if self.output_file != "none":
            aggregated.to_csv(self.output_file, index=False)
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

    if isinstance(subset_data, pd.DataFrame):
        population_df = subset_data.merge(
            population_df, how="left", on=subset_data.columns.tolist()
        ).reindex(population_df.columns, axis="columns")

    population_df = population_df.groupby(strata)

    if operation == "median":
        population_df = (
            population_df.median()
            .reset_index()
        )
    else:
        population_df = (
            population_df.mean()
            .reset_index()
        )

    # Aggregated image number and object number do not make sense
    for col in ["ImageNumber", "ObjectNumber"]:
        if col in population_df.columns:
            population_df = population_df.drop([col], axis="columns")

    return population_df
