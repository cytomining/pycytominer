import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from pycytominer import aggregate, normalize
from pycytominer.cyto_utils import (
    output,
    check_compartments,
    check_aggregate_operation,
    infer_cp_features,
    get_default_linking_cols,
    get_default_compartments,
    assert_linking_cols_complete,
    provide_linking_cols_feature_name_update,
    check_fields_of_view_format,
    check_fields_of_view,
)

default_compartments = get_default_compartments()
default_linking_cols = get_default_linking_cols()


class SingleCells(object):
    """This is a class to interact with single cell morphological profiles. Interaction
    includes aggregation, normalization, and output.

    :param file_or_conn: A file string or database connection storing the location of single cell profiles
    :type file_or_conn: str
    :param strata: The columns to groupby and aggregate single cells, defaults to ["Metadata_Plate", "Metadata_Well"]
    :type strata: list
    :param aggregation_operation: operation to perform single cell aggregation, defaults to "median"
    :type aggregation_operation: str
    :param output_file: If specified, the location to write the file, defaults to "none"
    :type output_file: str
    :param compartments: list of compartments to process, defaults to ["cells", "cytoplasm", "nuclei"]
    :type compartments: list
    :param compartment_linking_cols: dictionary identifying how to merge columns across tables, default noted below:
    :type compartment_linking_cols: dict
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
    :param fields_of_view: list of fields of view to include in the analysis, defaults to "all"
    :type fields_of_view: list, str
    :param object_feature: Object Number feature, defaults to "ObjectNumber"
    :type object_feature: str

    .. note::
        the argument compartment_linking_cols is designed to work with CellProfiler output,
        as curated by cytominer-database. The defaut is: {
            "cytoplasm": {
                "cells": "Cytoplasm_Parent_Cells",
                "nuclei": "Cytoplasm_Parent_Nuclei",
            },
            "cells": {"cytoplasm": "ObjectNumber"},
            "nuclei": {"cytoplasm": "ObjectNumber"},
        }
    """

    def __init__(
        self,
        file_or_conn,
        strata=["Metadata_Plate", "Metadata_Well"],
        aggregation_operation="median",
        output_file="none",
        compartments=default_compartments,
        compartment_linking_cols=default_linking_cols,
        merge_cols=["TableNumber", "ImageNumber"],
        load_image_data=True,
        subsample_frac=1,
        subsample_n="all",
        subsampling_random_state="none",
        fields_of_view="all",
        object_feature="ObjectNumber",
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
        self.load_image_data = load_image_data
        self.aggregation_operation = aggregation_operation.lower()
        self.output_file = output_file
        self.merge_cols = merge_cols
        self.subsample_frac = subsample_frac
        self.subsample_n = subsample_n
        self.subset_data_df = "none"
        self.subsampling_random_state = subsampling_random_state
        self.is_aggregated = False
        self.is_subset_computed = False
        self.compartments = compartments
        self.compartment_linking_cols = compartment_linking_cols
        self.fields_of_view = fields_of_view
        self.fields_of_view_feature = "Metadata_Site"
        self.object_feature = object_feature

        self.image_cols = ["TableNumber", "ImageNumber", "Metadata_Site"]

        # Confirm that the compartments and linking cols are formatted properly
        assert_linking_cols_complete(
            compartments=self.compartments, linking_cols=self.compartment_linking_cols
        )

        # Build a dictionary to update linking column feature names
        self.linking_col_rename = provide_linking_cols_feature_name_update(
            self.compartment_linking_cols
        )

        if self.subsample_n != "all":
            self.set_subsample_n(self.subsample_n)

        # Connect to sqlite engine
        self.engine = create_engine(self.file_or_conn)
        self.conn = self.engine.connect()

        # Throw an error if both subsample_frac and subsample_n is set
        self._check_subsampling()

        # Confirm that the input fields of view is valid
        self.fields_of_view = check_fields_of_view_format(self.fields_of_view)

        if self.load_image_data:
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
        image_query = "select {} from image".format(
            ", ".join(np.union1d(self.image_cols, self.strata))
        )
        self.image_df = pd.read_sql(sql=image_query, con=self.conn)
        if self.fields_of_view != "all":
            check_fields_of_view(
                list(np.unique(self.image_df[self.fields_of_view_feature])),
                list(self.fields_of_view),
            )
            self.image_df = self.image_df.query(
                f"{self.fields_of_view_feature}==@self.fields_of_view"
            )

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
                self.subset_data_df.groupby(self.strata)["Metadata_ObjectNumber"]
                .count()
                .reset_index()
                .rename({"Metadata_ObjectNumber": "cell_count"}, axis="columns")
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

    def subsample_profiles(self, df, rename_col=True):
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

            output_df = pd.DataFrame.sample(
                df,
                n=self.subsample_n,
                replace=True,
                random_state=self.subsampling_random_state,
            )
        else:
            output_df = pd.DataFrame.sample(
                df, frac=self.subsample_frac, random_state=self.subsampling_random_state
            )

        if rename_col:
            output_df = output_df.rename(self.linking_col_rename, axis="columns")

        return output_df

    def get_subsample(self, df=None, compartment="cells", rename_col=True):
        """Apply the subsampling procedure

        :param compartment: string indicating the compartment to process, defaults to "cells"
        :type compartment: str
        """
        check_compartments(compartment)

        query_cols = "TableNumber, ImageNumber, ObjectNumber"
        query = "select {} from {}".format(query_cols, compartment)

        # Load query and merge with image_df
        if df is None:
            df = pd.read_sql(sql=query, con=self.conn)

        query_df = self.image_df.merge(df, how="inner", on=self.merge_cols)

        self.subset_data_df = (
            query_df.groupby(self.strata)
            .apply(lambda x: self.subsample_profiles(x, rename_col=rename_col))
            .reset_index(drop=True)
        )

        self.is_subset_computed = True

    def load_compartment(self, compartment):
        compartment_query = "select * from {}".format(compartment)
        df = pd.read_sql(sql=compartment_query, con=self.conn)
        return df

    def aggregate_compartment(
        self,
        compartment,
        compute_subsample=False,
        compute_counts=False,
        aggregate_args=None,
    ):
        """Aggregate morphological profiles. Uses pycytominer.aggregate()

        :param compartment: string indicating the specific compartment, defaults to "cells"
        :type compartment: str
        :param compute_subsample: determine if subsample should be computed, defaults to False
        :type compute_subsample: bool
        :param compute_counts: determine if the number of the objects and fields should be computed, defaults to False
        :type compute_counts: bool
        :param aggregate_args: additional arguments passed as a dictionary as input to pycytominer.aggregate()
        :type aggregate_args: None, dict
        :return: Aggregated single-cell profiles
        :rtype: pd.DataFrame
        """
        check_compartments(compartment)

        if (self.subsample_frac < 1 or self.subsample_n != "all") and compute_subsample:
            self.get_subsample(compartment=compartment)

        # Load image data if not already loaded
        if not self.load_image_data:
            self.load_image()
            self.load_image_data = True

        population_df = self.image_df.merge(
            self.load_compartment(compartment=compartment),
            how="inner",
            on=self.merge_cols,
        ).rename(self.linking_col_rename, axis="columns")

        # Infering features is tricky with non-canonical data
        if aggregate_args is None:
            aggregate_args = {}
            features = infer_cp_features(population_df, compartments=compartment)
        elif "features" not in aggregate_args:
            features = infer_cp_features(population_df, compartments=compartment)
        elif aggregate_args["features"] == "infer":
            features = infer_cp_features(population_df, compartments=compartment)
        else:
            features = aggregate_args["features"]

        aggregate_args["features"] = features

        object_df = aggregate(
            population_df=population_df,
            strata=self.strata,
            compute_object_count=compute_counts,
            operation=self.aggregation_operation,
            subset_data_df=self.subset_data_df,
            object_feature=self.object_feature,
            **aggregate_args,
        )

        if compute_counts:
            fields_count_df = self.image_df.loc[
                :, self.strata + [self.fields_of_view_feature]
            ]
            fields_count_df = (
                fields_count_df.groupby(self.strata)[self.fields_of_view_feature]
                .count()
                .reset_index()
                .rename(
                    columns={f"{self.fields_of_view_feature}": f"Metadata_Fields_Count"}
                )
            )

            object_df = fields_count_df.merge(object_df, on=self.strata, how="right")

        return object_df

    def merge_single_cells(
        self,
        compute_subsample=False,
        sc_output_file="none",
        compression_options=None,
        float_format=None,
        single_cell_normalize=False,
        normalize_args=None,
    ):
        """Given the linking columns, merge single cell data. Normalization is also supported

        :param sc_output_file: the name of a file to output, defaults to "none":
        :type sc_output_file: str, optional
        :param compression_options: the mechanism to compress, defaults to None
        :type compression_options: str, optional
        :param float_format: decimal precision to use in writing output file, defaults to None
        :type float_format: str, optional
        :param single_cell_normalize: determine if the single cell data should also be normalized
        :type single_cell_normalize: bool
        :param normalize_args: additional arguments passed as a dictionary as input to pycytominer.normalize()
        :type normalize_args: None, dict
        :return: Either a dataframe (if output_file="none") or will write to file
        :rtype: pd.DataFrame, optional
        """

        # Load the single cell dataframe by merging on the specific linking columns
        sc_df = ""
        linking_check_cols = []
        merge_suffix_rename = []
        for left_compartment in self.compartment_linking_cols:
            for right_compartment in self.compartment_linking_cols[left_compartment]:
                # Make sure only one merge per combination occurs
                linking_check = "-".join(sorted([left_compartment, right_compartment]))
                if linking_check in linking_check_cols:
                    continue

                # Specify how to indicate merge suffixes
                merge_suffix = [
                    "_{comp_l}".format(comp_l=left_compartment),
                    "_{comp_r}".format(comp_r=right_compartment),
                ]
                merge_suffix_rename += merge_suffix
                left_link_col = self.compartment_linking_cols[left_compartment][
                    right_compartment
                ]
                right_link_col = self.compartment_linking_cols[right_compartment][
                    left_compartment
                ]

                if isinstance(sc_df, str):
                    initial_df = self.load_compartment(compartment=left_compartment)

                    if compute_subsample:
                        # Sample cells proportionally by self.strata
                        self.get_subsample(df=initial_df, rename_col=False)

                        subset_logic_df = self.subset_data_df.drop(
                            self.image_df.columns, axis="columns"
                        )

                        initial_df = subset_logic_df.merge(
                            initial_df, how="left", on=subset_logic_df.columns.tolist()
                        ).reindex(initial_df.columns, axis="columns")

                    sc_df = initial_df.merge(
                        self.load_compartment(compartment=right_compartment),
                        left_on=self.merge_cols + [left_link_col],
                        right_on=self.merge_cols + [right_link_col],
                        suffixes=merge_suffix,
                    )
                else:
                    sc_df = sc_df.merge(
                        self.load_compartment(compartment=right_compartment),
                        left_on=self.merge_cols + [left_link_col],
                        right_on=self.merge_cols + [right_link_col],
                        suffixes=merge_suffix,
                    )

                linking_check_cols.append(linking_check)

        # Add metadata prefix to merged suffixes
        full_merge_suffix_rename = []
        full_merge_suffix_original = []
        for col_name in self.merge_cols + list(self.linking_col_rename.keys()):
            full_merge_suffix_original.append(col_name)
            full_merge_suffix_rename.append("Metadata_{x}".format(x=col_name))

        for col_name in self.merge_cols + list(self.linking_col_rename.keys()):
            for suffix in set(merge_suffix_rename):
                full_merge_suffix_original.append("{x}{y}".format(x=col_name, y=suffix))
                full_merge_suffix_rename.append(
                    "Metadata_{x}{y}".format(x=col_name, y=suffix)
                )

        self.full_merge_suffix_rename = dict(
            zip(full_merge_suffix_original, full_merge_suffix_rename)
        )

        # Add image data to single cell dataframe
        if not self.load_image_data:
            self.load_image()
            self.load_image_data = True

        sc_df = (
            self.image_df.merge(sc_df, on=self.merge_cols, how="right")
            .rename(self.linking_col_rename, axis="columns")
            .rename(self.full_merge_suffix_rename, axis="columns")
        )
        if single_cell_normalize:
            # Infering features is tricky with non-canonical data
            if normalize_args is None:
                normalize_args = {}
                features = infer_cp_features(sc_df, compartments=self.compartments)
            elif "features" not in normalize_args:
                features = infer_cp_features(sc_df, compartments=self.compartments)
            elif normalize_args["features"] == "infer":
                features = infer_cp_features(sc_df, compartments=self.compartments)
            else:
                features = normalize_args["features"]

            normalize_args["features"] = features

            sc_df = normalize(profiles=sc_df, **normalize_args)

        if sc_output_file != "none":
            output(
                df=sc_df,
                output_filename=sc_output_file,
                compression_options=compression_options,
                float_format=float_format,
            )
        else:
            return sc_df

    def aggregate_profiles(
        self,
        compute_subsample=False,
        output_file="none",
        compression_options=None,
        float_format=None,
        aggregate_args=None,
    ):
        """Aggregate and merge compartments. This is the primary entry to this class.

        :param compute_subsample: Determine if subsample should be computed, defaults to False
        :type compute_subsample: bool
        :param output_file: the name of a file to output, defaults to "none"
        :type output_file: str, optional
        :param compression: the mechanism to compress, defaults to None
        :type compression: str, optional
        :param float_format: decimal precision to use in writing output file, defaults to None
        :type float_format: str, optional
        :param aggregate_args: additional arguments passed as a dictionary as input to pycytominer.aggregate()
        :type aggregate_args: None, dict
        :return: Either a dataframe (if output_file="none") or will write to file
        :rtype: pd.DataFrame, optional

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

        compartment_idx = 0
        for compartment in self.compartments:
            if compartment_idx == 0:
                aggregated = self.aggregate_compartment(
                    compartment=compartment,
                    compute_subsample=compute_subsample,
                    compute_counts=True,
                )
            else:
                aggregated = aggregated.merge(
                    self.aggregate_compartment(compartment=compartment),
                    on=self.strata,
                    how="inner",
                )
            compartment_idx += 1

        self.is_aggregated = True

        if self.output_file != "none":
            output(
                df=aggregated,
                output_filename=self.output_file,
                compression_options=compression_options,
                float_format=float_format,
            )
        else:
            return aggregated
