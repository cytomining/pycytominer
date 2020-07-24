"""
Aggregate single cell data based on given grouping variables
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from pycytominer.cyto_utils import (
    output,
    check_compartments,
    check_aggregate_operation,
    infer_cp_features,
)


class AggregateProfiles:
    """
    Class to aggregate single cell morphological profiles
    """

    def __init__(
        self,
        sql_file,
        strata=["Metadata_Plate", "Metadata_Well"],
        features="infer",
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
        merge_cols - column indicating which columns to merge images and compartments
        subsample_frac - [default: 1] float (0 < subsample <= 1) indicating percentage of
                         single cells to select
        subsample_n - [default: "all"] int indicating how many samples to include
        subsampling_random_state - [default: "none"] the random state to init subsample
        """
        # Check compartments specified
        check_compartments(compartments)

        # Check if correct operation is specified
        operation = check_aggregate_operation(operation)

        # Check that the subsample_frac is between 0 and 1
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
        self.subsample_n = subsample_n
        self.subset_data_df = "none"
        self.subsampling_random_state = subsampling_random_state
        self.is_aggregated = False
        self.is_subset_computed = False

        if self.subsample_n != "all":
            self.set_subsample_n(self.subsample_n)

        # Connect to sqlite engine
        self.engine = create_engine(self.sql_file)
        self.conn = self.engine.connect()

        # Throw an error if both subsample_frac and subsample_n is set
        self._check_subsampling()

        if load_image_data:
            self.load_image()

    def _check_subsampling(self):
        # Check that the user didn't specify both subset frac and subsample all
        assert (
            self.subsample_frac == 1 or self.subsample_n == "all"
        ), "Do not set both subsample_frac and subsample_n"

    def set_output_file(self, output_file):
        self.output_file = output_file

    def set_subsample_frac(self, subsample_frac):
        self.subsample_frac = subsample_frac
        self._check_subsampling()

    def set_subsample_n(self, subsample_n):
        try:
            self.subsample_n = int(subsample_n)
        except ValueError:
            raise ValueError("subsample n must be an integer or coercable")
        self._check_subsampling()

    def set_subsample_random_state(self, random_state):
        self.subsampling_random_state = random_state

    def load_image(self):
        """
        Load image table from sqlite file
        """
        # Extract image metadata
        image_cols = "TableNumber, ImageNumber, {}".format(", ".join(self.strata))
        image_query = "select {} from image".format(image_cols)
        self.image_df = pd.read_sql(sql=image_query, con=self.conn)

    def count_cells(self, compartment="cells", count_subset=False):
        """
        Determine how many cells are measured per well.

        Arguments:
        compartment - string indicating the compartment to subset
        count_subset - [default: False] count the number of cells in subset partition
        """
        check_compartments(compartment)

        if count_subset:
            assert self.is_aggregated, "Make sure to aggregate_profiles() first!"
            assert self.is_subset_computed, "Make sure to get_subsample() first!"
            count_df = (
                self.subset_data_df.groupby(self.strata)["ObjectNumber"]
                .count()
                .reset_index()
                .rename({"ObjectNumber": "cell_count"}, axis="columns")
            )
        else:
            query_cols = "TableNumber, ImageNumber, ObjectNumber"
            query = "select {} from {}".format(query_cols, compartment)
            count_df = self.image_df.merge(
                pd.read_sql(sql=query, con=self.conn), how="inner", on=self.merge_cols
            )
            count_df = (
                count_df.groupby(self.strata)["ObjectNumber"]
                .count()
                .reset_index()
                .rename({"ObjectNumber": "cell_count"}, axis="columns")
            )

        return count_df

    def subsample_profiles(self, x):
        """
        Sample a Pandas DataFrame given the subsampling fraction
        """
        if self.subsampling_random_state == "none":
            random_state = np.random.randint(0, 10000, size=1)[0]
            self.set_subsample_random_state(random_state)

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

    def get_subsample(self, compartment="cells"):
        """
        Extract subsample from sqlite file

        Arguments:
        compartment - [default: "cells"] string indicating the compartment to subset
        """
        check_compartments(compartment)

        query_cols = "TableNumber, ImageNumber, ObjectNumber"
        query = "select {} from {}".format(query_cols, compartment)

        # Load query and merge with image_df
        query_df = self.image_df.merge(
            pd.read_sql(sql=query, con=self.conn), how="inner", on=self.merge_cols
        )

        self.subset_data_df = (
            query_df.groupby(self.strata)
            .apply(lambda x: self.subsample_profiles(x))
            .reset_index(drop=True)
        )

        self.is_subset_computed = True

    def aggregate_compartment(self, compartment, compute_subsample=False):
        """
        Aggregate morphological profiles

        Arguments:
        compartment - str indicating specific compartment to extract

        Return:
        Either the merged object file or write object to disk
        """
        check_compartments(compartment)

        compartment_query = "select * from {}".format(compartment)

        if (self.subsample_frac < 1 or self.subsample_n != "all") and compute_subsample:
            self.get_subsample(compartment=compartment)

        population_df = self.image_df.merge(
            pd.read_sql(sql=compartment_query, con=self.conn),
            how="inner",
            on=self.merge_cols,
        )

        object_df = aggregate(
            population_df=population_df,
            strata=self.strata,
            features=self.features,
            operation=self.operation,
            subset_data_df=self.subset_data_df,
        )

        return object_df

    def aggregate_profiles(
        self,
        compute_subsample="False",
        output_file="none",
        compression=None,
        float_format=None,
    ):
        """
        Aggregate and merge compartments. This is the primary entry to this class.

        Arguments:
        compute_subsample - [default: False] boolean if subsample should be computed.
                            NOTE: Must be specified to perform subsampling. Will not
                            apply subsetting if set to False even if subsample is
                            initialized
        output_file - [default: "none"] if provided, will write annotated profiles to file
                  if not specified, will return the annotated profiles. We recommend
                  that this output file be suffixed with "_augmented.csv".
        compression - the mechanism to compress [default: None]
        float_format - decimal precision to use in writing output file [default: None]
                           For example, use "%.3g" for 3 decimal precision.

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
            output(
                df=aggregated,
                output_filename=self.output_file,
                compression=compression,
                float_format=float_format,
            )
        else:
            return aggregated


def aggregate(
    population_df,
    strata=["Metadata_Plate", "Metadata_Well"],
    features="infer",
    operation="median",
    subset_data_df="none",
):
    """
    Combine population dataframe variables by strata groups using given operation

    Arguments:
    population_df - pandas DataFrame to group and aggregate
    strata - [default: ["Metadata_Plate", "Metadata_Well"]] list indicating the columns to groupby and aggregate
    features - [default: "all"] or list indicating features that should be aggregated
    operation - [default: "median"] a string indicating how the data is aggregated
                currently only supports one of ['mean', 'median']
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

    # Aggregated image number and object number do not make sense
    for col in ["ImageNumber", "ObjectNumber"]:
        if col in population_df.columns:
            population_df = population_df.drop([col], axis="columns")

    return population_df
