import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from pycytominer import aggregate
from pycytominer import normalize
from pycytominer.cyto_utils import (
    output,
    check_compartments,
    check_aggregate_operation,
    infer_cp_features,
)


class SingleCells(object):
    """This is a class to interact with single cell morphological profiles. Interaction
    includes aggregation, normalization, and output.

    :param file_or_conn: A file string or database connection storing the location of single cell profiles
    :type file_or_conn: str
    :param strata: The columns to groupby and aggregate single cells, defaults to ["Metadata_Plate", "Metadata_Well"]
    :type strata: list
    :param features: The features that should be aggregated, defaults to "infer"
    :type features: str, list
    :param aggregation_operation: operation to perform single cell aggregation, defaults to "median"
    :type aggregation_operation: str
    :param output_file: If specified, the location to write the file, defaults to "none"
    :type output_file: str
    :param compartments: list of compartments to process, defaults to ["cells", "cytoplasm", "nuclei"]
    :type compartments: list
    :param merge_cols: columns indicating how to merge image and compartment data, defaults to ["TableNumber", "ImageNumber"]
    :type merge_cols: list
    :param load_image_data: if image data should be loaded into memory, defaults to True
    :type load_image_data: bool
    :param subsample_frac: indicating percentage of single cells to select (0 < subsample_frac <= 1), defaults to 1
    :type subsample_frac: float
    :param subsample_n: indicate how many samples to subsample - do not specify both subsample_frac and subsample_n, defaults to "all"
    :type subsample_n:, str, int
    :param subsampling_random_state: the random state to init subsample, defaults to "none"
    :type subsampling_random_state: str, int
    """

    def __init__(
        self,
        file_or_conn,
        strata=["Metadata_Plate", "Metadata_Well"],
        features="infer",
        aggregation_operation="median",
        output_file="none",
        compartments=["cells", "cytoplasm", "nuclei"],
        merge_cols=["TableNumber", "ImageNumber"],
        load_image_data=True,
        subsample_frac=1,
        subsample_n="all",
        subsampling_random_state="none",
    ):
        """Constructor method"""
        # Check compartments specified
        check_compartments(compartments)

        # Check if correct operation is specified
        aggregation_operation = check_aggregate_operation(aggregation_operation)

        # Check that the subsample_frac is between 0 and 1
        assert (
            0 < subsample_frac and 1 >= subsample_frac
        ), "subsample_frac must be between 0 and 1"

        self.file_or_conn = file_or_conn
        self.strata = strata
        self.features = features
        self.aggregation_operation = aggregation_operation.lower()
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
        self.engine = create_engine(self.file_or_conn)
        self.conn = self.engine.connect()

        # Throw an error if both subsample_frac and subsample_n is set
        self._check_subsampling()

        if load_image_data:
            self.load_image()

    def _check_subsampling(self):
        """Internal method checking if subsampling options were specified correctly"""
        # Check that the user didn't specify both subset frac and subsample all
        assert (
            self.subsample_frac == 1 or self.subsample_n == "all"
        ), "Do not set both subsample_frac and subsample_n"

    def set_output_file(self, output_file):
        """Setting operation to conveniently rename output file

        :param output_file: the new output file name
        :type output_file: str
        """
        self.output_file = output_file

    def set_subsample_frac(self, subsample_frac):
        """Setting operation to conveniently update the subsample fraction

        :param subsample_frac: indicating percentage of single cells to select (0 < subsample_frac <= 1), defaults to 1
        :type subsample_frac: float
        """
        self.subsample_frac = subsample_frac
        self._check_subsampling()

    def set_subsample_n(self, subsample_n):
        """Setting operation to conveniently update the subsample n

        :param subsample_n: indicate how many samples to subsample - do not specify both subsample_frac and subsample_n, defaults to "all"
        :type subsample_n:, str, int
        """
        try:
            self.subsample_n = int(subsample_n)
        except ValueError:
            raise ValueError("subsample n must be an integer or coercable")
        self._check_subsampling()

    def set_subsample_random_state(self, random_state):
        """Setting operation to conveniently update the subsample random state

        :param random_state: the random state to init subsample, defaults to "none"
        :type random_state:, str, int
        """
        self.subsampling_random_state = random_state

    def load_image(self):
        """Load image table from sqlite file"""
        # Extract image metadata
        image_cols = "TableNumber, ImageNumber, {}".format(", ".join(self.strata))
        image_query = "select {} from image".format(image_cols)
        self.image_df = pd.read_sql(sql=image_query, con=self.conn)

    def count_cells(self, compartment="cells", count_subset=False):
        """Determine how many cells are measured per well.

        :param compartment: string indicating the compartment to subset, defaults to "cells"
        :type compartment: str
        :param count_subset: whether or not count the number of cells as specified by the strata groups
        :return: A pandas dataframe of cell counts in the experiment
        :rtype: pd.DataFrame
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

    def subsample_profiles(self, df):
        """Sample a Pandas DataFrame given subsampling information

        :param df: A single cell profile dataframe
        :type df: pd.DataFrame
        :return: A subsampled pandas dataframe of single cell profiles
        :rtype: pd.DataFrame
        """
        if self.subsampling_random_state == "none":
            random_state = np.random.randint(0, 10000, size=1)[0]
            self.set_subsample_random_state(random_state)

        if self.subsample_frac == 1:
            return pd.DataFrame.sample(
                df,
                n=self.subsample_n,
                replace=True,
                random_state=self.subsampling_random_state,
            )
        else:
            return pd.DataFrame.sample(
                df, frac=self.subsample_frac, random_state=self.subsampling_random_state
            )

    def get_subsample(self, compartment="cells"):
        """Apply the subsampling procedure

        :param compartment: string indicating the compartment to process, defaults to "cells"
        :type compartment: str
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
        """Aggregate morphological profiles. Uses pycytominer.aggregate()

        :param compartment: string indicating the specific compartment, defaults to "cells"
        :type compartment: str
        :param compute_subsample: determine if subsample should be computed, defaults to False
        :type compute_subsample: bool
        :return: Aggregated single-cell profiles
        :rtype: pd.DataFrame
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
            operation=self.aggregation_operation,
            subset_data_df=self.subset_data_df,
        )

        return object_df

    def aggregate_profiles(
        self,
        compute_subsample=False,
        output_file="none",
        compression=None,
        float_format=None,
    ):
        """Aggregate and merge compartments. This is the primary entry to this class.

        :param compute_subsample: Determine if subsample should be computed, defaults to False
        :type compute_subsample: bool
        :param output_file: the name of a file to output, defaults to "none":
        :type output_file: str, optional
        :param compression: the mechanism to compress, defaults to None
        :type compression: str, optional
        :param float_format: decimal precision to use in writing output file, defaults to None
        :type float_format: str, optional

        Return:
        if output_file is set, then write to file. If not then return

        .. note::
            compute_subsample must be specified to perform subsampling. The function
            aggregate_profiles(compute_subsample=True) will apply subsetting if even if
            subsample is initialized

        .. note::
            We recommend that, if provided, the output file be suffixed with "_augmented"

        :Example:
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
