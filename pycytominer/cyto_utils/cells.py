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
    extract_image_features,
    aggregate_fields_count,
    aggregate_image_features,
)

default_compartments = get_default_compartments()
default_linking_cols = get_default_linking_cols()


class SingleCells(object):
    """This is a class to interact with single cell morphological profiles. Interaction
    includes aggregation, normalization, and output.

    Attributes
    ----------
    file_or_conn : str or pandas.core.frame.DataFrame
        A file string or database connection storing the location of single cell profiles.
    strata : list of str, default ["Metadata_Plate", "Metadata_Well"]
        The columns to groupby and aggregate single cells.
    aggregation_operation : str, default "median"
        Operation to perform single cell aggregation.
    output_file : str, default "none"
        If specified, the location to write the file.
    compartments : list of str, default ["cells", "cytoplasm", "nuclei"]
        List of compartments to process.
    compartment_linking_cols : dict, default noted below
        Dictionary identifying how to merge columns across tables.
    merge_cols : list of str, default ["TableNumber", "ImageNumber"]
        Columns indicating how to merge image and compartment data.
    image_cols : list of str, default ["TableNumber", "ImageNumber", "Metadata_Site"]
        Columns to select from the image table.
    add_image_features: bool, default False
        Whether to add image features to the profiles.
    image_feature_categories : list of str, optional
        List of categories of features from the image table to add to the profiles.
    features: str or list of str, default "infer"
        List of features that should be aggregated.
    load_image_data : bool, default True
        Whether or not the image data should be loaded into memory.
    subsample_frac : float, default 1
        The percentage of single cells to select (0 < subsample_frac <= 1).
    subsample_n : str or int, default "all"
        How many samples to subsample - do not specify both subsample_frac and subsample_n.
    subsampling_random_state : str or int, default "none"
        The random state to init subsample.
    fields_of_view : list of int, str, default "all"
        List of fields of view to aggregate.
    fields_of_view_feature : str, default "Metadata_Site"
        Name of the fields of view feature.
    object_feature : str, default "Metadata_ObjectNumber"
        Object number feature.

    Notes
    -----
    .. note::
        the argument compartment_linking_cols is designed to work with CellProfiler output,
        as curated by cytominer-database. The default is: {
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
        image_cols=["TableNumber", "ImageNumber", "Metadata_Site"],
        add_image_features=False,
        image_feature_categories=None,
        features="infer",
        load_image_data=True,
        subsample_frac=1,
        subsample_n="all",
        subsampling_random_state="none",
        fields_of_view="all",
        fields_of_view_feature="Metadata_Site",
        object_feature="Metadata_ObjectNumber",
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
        self.image_cols = image_cols
        self.add_image_features = add_image_features
        self.image_feature_categories = image_feature_categories
        self.features = features
        self.subsample_frac = subsample_frac
        self.subsample_n = subsample_n
        self.subset_data_df = "none"
        self.subsampling_random_state = subsampling_random_state
        self.is_aggregated = False
        self.is_subset_computed = False
        self.compartments = compartments
        self.compartment_linking_cols = compartment_linking_cols
        self.fields_of_view_feature = fields_of_view_feature
        self.object_feature = object_feature

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
        self.fields_of_view = check_fields_of_view_format(fields_of_view)

        if self.load_image_data:
            self.load_image()

    def _check_subsampling(self):
        """Internal method checking if subsampling options were specified correctly.

        Returns
        -------
        None
            Nothing is returned.

        """

        # Check that the user didn't specify both subset frac and subsample all
        assert (
            self.subsample_frac == 1 or self.subsample_n == "all"
        ), "Do not set both subsample_frac and subsample_n"

    def set_output_file(self, output_file):
        """Setting operation to conveniently rename output file.

        Parameters
        ----------
        output_file : str
            New output file name.

        Returns
        -------
        None
            Nothing is returned.

        """

        self.output_file = output_file

    def set_subsample_frac(self, subsample_frac):
        """Setting operation to conveniently update the subsample fraction.

        Parameters
        ----------
        subsample_frac : float, default 1
            Percentage of single cells to select (0 < subsample_frac <= 1).

        Returns
        -------
        None
            Nothing is returned.

        """

        self.subsample_frac = subsample_frac
        self._check_subsampling()

    def set_subsample_n(self, subsample_n):
        """Setting operation to conveniently update the subsample n.

        Parameters
        ----------
        subsample_n : int, default "all"
            Indicate how many sample to subsample - do not specify both subsample_frac and subsample_n.

        Returns
        -------
        None
            Nothing is returned.

        """

        try:
            self.subsample_n = int(subsample_n)
        except ValueError:
            raise ValueError("subsample n must be an integer or coercable")
        self._check_subsampling()

    def set_subsample_random_state(self, random_state):
        """Setting operation to conveniently update the subsample random state.

        Parameters
        ----------
        random_state: int, optional
            The random state to init subsample.

        Returns
        -------
        None
            Nothing is returned.

        """

        self.subsampling_random_state = random_state

    def load_image(self):
        """Load image table from sqlite file

        Returns
        -------
        None
            Nothing is returned.

        """

        image_query = "select * from image"
        self.image_df = pd.read_sql(sql=image_query, con=self.conn)

        if self.add_image_features:
            self.image_features_df = extract_image_features(
                self.image_feature_categories,
                self.image_df,
                self.image_cols,
                self.strata,
            )

        image_features = list(np.union1d(self.image_cols, self.strata))
        self.image_df = self.image_df[image_features]

        if self.fields_of_view != "all":
            check_fields_of_view(
                list(np.unique(self.image_df[self.fields_of_view_feature])),
                list(self.fields_of_view),
            )
            self.image_df = self.image_df.query(
                f"{self.fields_of_view_feature}==@self.fields_of_view"
            )

            if self.add_image_features:
                self.image_features_df = self.image_features_df.query(
                    f"{self.fields_of_view_feature}==@self.fields_of_view"
                )

    def count_cells(self, compartment="cells", count_subset=False):
        """Determine how many cells are measured per well.

        Parameters
        ----------
        compartment : str, default "cells"
            Compartment to subset.
        count_subset : bool, default False
            Whether or not count the number of cells as specified by the strata groups.

        Returns
        -------
        pandas.core.frame.DataFrame
            DataFrame of cell counts in the experiment.

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
        """Sample a Pandas DataFrame given subsampling information.

        Parameters
        ----------
        df : pandas.core.frame.DataFrame
            DataFrame of a single cell profile.
        rename_col : bool, default True
            Whether or not to rename the columns.

        Returns
        -------
        pandas.core.frame.DataFrame
            A subsampled pandas dataframe of single cell profiles.

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
        """Apply the subsampling procedure.

        Parameters
        ----------
        df : pandas.core.frame.DataFrame
            DataFrame of a single cell profile.
        compartment : str, default "cells"
            The compartment to process.
        rename_col : bool, default True
            Whether or not to rename the columns.

        Returns
        -------
        None
            Nothing is returned.

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
        """Creates the compartment dataframe.

        Parameters
        ----------
        compartment : str
            The compartment to process.

        Returns
        -------
        pandas.core.frame.DataFrame
            Compartment dataframe.

        """
        compartment_query = "select * from {}".format(compartment)
        df = pd.read_sql(sql=compartment_query, con=self.conn)
        return df

    def aggregate_compartment(
        self,
        compartment,
        compute_subsample=False,
        compute_counts=False,
        add_image_features=False,
        n_aggregation_memory_strata=1,
    ):
        """Aggregate morphological profiles. Uses pycytominer.aggregate()

        Parameters
        ----------
        compartment : str
            Compartment to aggregate.
        compute_subsample : bool, default False
            Whether or not to subsample.
        compute_counts : bool, default False
            Whether or not to compute the number of objects in each compartment
            and the number of fields of view per well.
        add_image_features : bool, default False
            Whether or not to add image features.
        n_aggregation_memory_strata : int, default 1
            Number of unique strata to pull from the database into working memory
            at once.  Typically 1 is fastest.  A larger number uses more memory.
            For example, if aggregating by "well", then n_aggregation_memory_strata=1
            means that one "well" will be pulled from the SQLite database into
            memory at a time.

        Returns
        -------
        pandas.core.frame.DataFrame
            DataFrame of aggregated profiles.

        """

        check_compartments(compartment)

        if (self.subsample_frac < 1 or self.subsample_n != "all") and compute_subsample:
            self.get_subsample(compartment=compartment)

        # Load image data if not already loaded
        if not self.load_image_data:
            self.load_image()
            self.load_image_data = True

        # Iteratively call aggregate() on chunks of the full compartment table
        object_dfs = []
        for compartment_df in self._compartment_df_generator(
            compartment=compartment,
            n_aggregation_memory_strata=n_aggregation_memory_strata,
        ):

            population_df = self.image_df.merge(
                compartment_df,
                how="inner",
                on=self.merge_cols,
            ).rename(self.linking_col_rename, axis="columns")

            if self.features == "infer":
                aggregate_features = infer_cp_features(
                    population_df, compartments=compartment
                )
            else:
                aggregate_features = self.features

            partial_object_df = aggregate(
                population_df=population_df,
                strata=self.strata,
                compute_object_count=compute_counts,
                operation=self.aggregation_operation,
                subset_data_df=self.subset_data_df,
                features=aggregate_features,
                object_feature=self.object_feature,
            )

            if compute_counts and self.fields_of_view_feature not in self.strata:
                fields_count_df = aggregate_fields_count(
                    self.image_df, self.strata, self.fields_of_view_feature
                )

                if add_image_features:
                    fields_count_df = aggregate_image_features(
                        fields_count_df,
                        self.image_features_df,
                        self.image_feature_categories,
                        self.image_cols,
                        self.strata,
                        self.aggregation_operation,
                    )

                partial_object_df = fields_count_df.merge(
                    partial_object_df,
                    on=self.strata,
                    how="right",
                )

                # Separate all the metadata and feature columns.
                metadata_cols = infer_cp_features(partial_object_df, metadata=True)
                feature_cols = infer_cp_features(partial_object_df, image_features=True)

                partial_object_df = partial_object_df.reindex(
                    columns=metadata_cols + feature_cols
                )

            object_dfs.append(partial_object_df)

        # Concatenate one or more aggregated dataframes row-wise into final output
        object_df = pd.concat(object_dfs, axis=0).reset_index(drop=True)

        return object_df

    def _compartment_df_generator(
        self,
        compartment,
        n_aggregation_memory_strata=1,
    ):
        """A generator function that returns chunks of the entire compartment
        table from disk.

        We want to return dataframes with all compartment entries within unique
        combinations of self.merge_cols when aggregated by self.strata

        Parameters
        ----------
        compartment : str
            Compartment to aggregate.
        n_aggregation_memory_strata : int, default 1
            Number of unique strata to pull from the database into working memory
            at once.  Typically 1 is fastest.  A larger number uses more memory.

        Returns
        -------
        image_df : Iterator[pandas.core.frame.DataFrame]
            A generator whose __next__() call returns a chunk of the compartment
            table, where rows comprising a unique aggregation stratum are not split
            between chunks, and thus groupby aggregations are valid

        """

        assert (
            n_aggregation_memory_strata > 0
        ), "Number of strata to pull into memory at once (n_aggregation_memory_strata) must be > 0"

        # Obtain data types of all columns of the compartment table
        cols = "*"
        compartment_row1 = pd.read_sql(
            sql=f"select {cols} from {compartment} limit 1",
            con=self.conn,
        )
        typeof_str = ", ".join([f"typeof({x})" for x in compartment_row1.columns])
        compartment_dtypes = pd.read_sql(
            sql=f"select {typeof_str} from {compartment} limit 1",
            con=self.conn,
        )
        # Strip the characters "typeof(" from the beginning and ")" from the end of
        # compartment column names returned by SQLite
        strip_typeof = lambda s: s[7:-1]
        dtype_dict = dict(
            zip(
                [strip_typeof(s) for s in compartment_dtypes.columns],  # column names
                compartment_dtypes.iloc[0].values,  # corresponding data types
            )
        )

        # Obtain all valid strata combinations, and their merge_cols values
        df_unique_mergecols = (
            self.image_df[self.strata + self.merge_cols]
            .groupby(self.strata)
            .agg(lambda s: np.unique(s).tolist())
            .reset_index(drop=True)
        )

        # Group the unique strata values into a list of SQLite condition strings
        # Find unique aggregated strata for the output
        strata_conditions = _sqlite_strata_conditions(
            df=df_unique_mergecols,
            dtypes=dtype_dict,
            n=n_aggregation_memory_strata,
        )

        # The generator, for each group of compartment values
        for strata_condition in strata_conditions:
            specific_compartment_query = (
                f"select {cols} from {compartment} where {strata_condition}"
            )
            image_df_chunk = pd.read_sql(sql=specific_compartment_query, con=self.conn)
            yield image_df_chunk

    def merge_single_cells(
        self,
        compute_subsample=False,
        sc_output_file="none",
        compression_options=None,
        float_format=None,
        single_cell_normalize=False,
        normalize_args=None,
    ):
        """Given the linking columns, merge single cell data. Normalization is also supported.

        Parameters
        ----------
        compute_subsample : bool, default False
            Whether or not to compute subsample.
        sc_output_file : str, optional
            The name of a file to output.
        compression_options : str, optional
            Compression arguments as input to pandas.to_csv() with pandas version >= 1.2.
        float_format : str, optional
            Decimal precision to use in writing output file.
        single_cell_normalize : bool, default False
            Whether or not to normalize the single cell data.
        normalize_args : dict, optional
            Additional arguments passed as input to pycytominer.normalize().

        Returns
        -------
        pandas.core.frame.DataFrame
            Either a dataframe (if output_file="none") or will write to file.

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
        n_aggregation_memory_strata=1,
    ):
        """Aggregate and merge compartments. This is the primary entry to this class.

        Parameters
        ----------
        compute_subsample : bool, default False
            Whether or not to compute subsample. compute_subsample must be specified to perform subsampling.
            The function aggregate_profiles(compute_subsample=True) will apply subsetting even if subsample is initialized.
        output_file : str, optional
            The name of a file to output. We recommended that, if provided, the output file be suffixed with "_augmented".
        compression_options : str, optional
            Compression arguments as input to pandas.to_csv() with pandas version >= 1.2.
        float_format : str, optional
            Decimal precision to use in writing output file.
        n_aggregation_memory_strata : int, default 1
            Number of unique strata to pull from the database into working memory
            at once.  Typically 1 is fastest.  A larger number uses more memory.

        Returns
        -------
        pandas.core.frame.DataFrame
            Either a dataframe (if output_file="none") or will write to file.

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
                    add_image_features=self.add_image_features,
                    n_aggregation_memory_strata=n_aggregation_memory_strata,
                )
            else:
                aggregated = aggregated.merge(
                    self.aggregate_compartment(
                        compartment=compartment,
                        n_aggregation_memory_strata=n_aggregation_memory_strata,
                    ),
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


def _sqlite_strata_conditions(df, dtypes, n=1):
    """Given a dataframe where columns are merge_cols and rows are unique
    value combinations that appear as aggregation strata, return a list
    of strings which constitute valid SQLite conditional statements.

    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        A dataframe where columns are merge_cols and rows represent
        unique aggregation strata of the compartment table
    dtypes : Dict[str, str]
        Dictionary to look up SQLite datatype based on column name
    n : int
        Number of rows of the input df to combine in each output
        conditional statement. n=1 means each row of the input will
        correspond to one string in the output list. n=2 means one
        string in the output list is comprised of two rows from the
        input df.

    Returns
    -------
    grouped_conditions : List[str]
        A list of strings, each string being a valid SQLite conditional

    Examples
    --------
    Suppose df looks like this:
        TableNumber | ImageNumber
        =========================
        [1]         | [1]
        [2]         | [1, 2, 3]
        [3]         | [1, 2]
        [4]         | [1]

    >>> _sqlite_strata_conditions(df, dtypes={'TableNumber': 'integer', 'ImageNumber': 'integer'}, n=1)
    ["(TableNumber in (1) and ImageNumber in (1))",
     "(TableNumber in (2) and ImageNumber in (1, 2, 3))",
     "(TableNumber in (3) and ImageNumber in (1, 2))",
     "(TableNumber in (4) and ImageNumber in (1))"]

    >>> _sqlite_strata_conditions(df, dtypes={'TableNumber': 'text', 'ImageNumber': 'integer'}, n=2)
    ["(TableNumber in ('1') and ImageNumber in (1))
      or (TableNumber in ('2') and ImageNumber in (1, 2, 3))",
     "(TableNumber in ('3') and ImageNumber in (1, 2))
      or (TableNumber in ('4') and ImageNumber in (1))"]
    """
    conditions = []
    for row in df.iterrows():
        series = row[1]
        values = [
            [f"'{a}'" for a in y] if dtypes[x] == "text" else y
            for x, y in zip(series.index, series.values)
        ]  # put quotes around text entries
        condition_list = [
            f"{x} in ({', '.join([str(a) for a in y]) if len(y) > 1 else y[0]})"
            for x, y in zip(series.index, values)
        ]
        conditions.append(f"({' and '.join(condition_list)})")
    grouped_conditions = [
        " or ".join(conditions[i : (i + n)]) for i in range(0, len(conditions), n)
    ]
    return grouped_conditions
