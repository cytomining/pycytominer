"""
Utility function to load and process the output files of a DeepProfiler run.
"""
import os
import pathlib
import numpy as np
import pandas as pd
import warnings

from pycytominer import aggregate
from pycytominer.cyto_utils import load_npz, infer_cp_features


class AggregateDeepProfiler:
    """This class holds all functions needed to load and annotate the DeepProfiler (DP) run.
    ----------
    Attributes
    ----------

    profile_dir : str
        file location of the output profiles from DP
        should be something like `/project1/outputs/results/features/`
    aggregate_operation : ['median', 'mean']
        method of aggregation
    aggregate_on : ['site', 'well', 'plate']
        up to which level will it aggregate
    file_delimiter : default = '_'
        delimiter for the filenames of the profiles (e.g. B02_4.npz)
    file_extension : default = '.npz'
        extension of the profiles
    index_df : dataframe
        load in the index.csv file from DeepProfiler
    filenames : list of paths
        list of Purepaths that point to the npz files
    aggregated_profiles : dataframe
        df to hold the metadata and profiles
    file_aggregate : dict
        dict that holds the file names and metadata. Is used to load in the npz files in the correct order and groupign
    """

    def __init__(
        self,
        index_file,
        profile_dir,
        aggregate_operation="median",
        aggregate_on="well",
        file_delimiter="_",
        file_extension=".npz",
    ):
        """
        __init__ function for this class.

        Arguments
        ---------
        index_file : str
            file location of the index.csv from DP

        See above for all other parameters.
        """
        self.profile_dir = profile_dir
        assert aggregate_operation in [
            "median",
            "mean",
        ], "Input of aggregate_operation is incorrect, it must be either median or mean"
        self.aggregate_operation = aggregate_operation
        assert aggregate_on in [
            "site",
            "well",
            "plate",
        ], "Input of aggregate_on is incorrect, it must be either site or well or plate"
        self.aggregate_on = aggregate_on
        self.file_delimiter = file_delimiter
        self.file_extension = file_extension
        if not self.file_extension.startswith("."):
            self.file_extension = f".{self.file_extension}"
        self.index_df = pd.read_csv(index_file)

    def build_filenames(self):
        """
        Create file names indicated by plate, well, and site information
        """
        self.filenames = self.index_df.apply(
            self.build_filename_from_index, axis="columns"
        )
        self.filenames = [
            pathlib.PurePath(f"{self.profile_dir}/{x}") for x in self.filenames
        ]

    def build_filename_from_index(self, row):
        """
        Builds the name of the profile files
        """
        plate = row["Metadata_Plate"]
        well = row["Metadata_Well"]
        site = row["Metadata_Site"]

        filename = f"{plate}/{well}_{site}{self.file_extension}"
        return filename

    def extract_filename_metadata(self, npz_file, delimiter="_"):
        """
        Extract metadata (site, well and plate) from the filename.
        The input format of the file: path/plate/well_site.npz

        Arguments
        ---------
        npz_file : str
            file path

        delimiter : str
            the delimiter used in the naming convention of the files. default = '_'

        Returns
        -------
        loc : dict
            dict with metadata
        """
        base_file = os.path.basename(npz_file).strip(".npz").split(delimiter)
        site = base_file[-1]
        well = base_file[-2]
        plate = str(npz_file).split("/")[-2]

        loc = {"site": site, "well": well, "plate": plate}
        return loc

    def setup_aggregate(self):
        """
        Sets up the file_aggregate attribute. This is a helper function to aggregate_deep()

        the file_aggregate dictionary contains the file locations and metadata for each grouping.
        If for example we are grouping by well then the keys of self.file_aggregate would be:
        plate1/well1, plate1/well2, plate2/well1, etc.
        """
        if not hasattr(self, "filenames"):
            self.build_filenames()

        self.file_aggregate = (
            {}
        )  # this is considered bad practice, I believe - may wont to change
        for filename in self.filenames:
            file_info = self.extract_filename_metadata(filename, self.file_delimiter)
            file_key = file_info[self.aggregate_on]

            if self.aggregate_on == "site":
                file_key = (
                    f"{file_info['plate']}/{file_info['well']}_{file_info['site']}"
                )

            if self.aggregate_on == "well":
                file_key = f"{file_info['plate']}/{file_info['well']}"

            if file_key in self.file_aggregate:
                self.file_aggregate[file_key]["files"].append(filename)
            else:
                self.file_aggregate[file_key] = {}
                self.file_aggregate[file_key]["files"] = [filename]

            self.file_aggregate[file_key]["metadata"] = file_info

    def aggregate_deep(self):
        """
        Aggregates the profiles into a pandas dataframe.

        For each key in file_aggregate, the profiles are loaded, concatenated and then aggregated.
        If files are missing, an error is thrown but the code continues.
        """
        if not hasattr(self, "file_aggregate"):
            self.setup_aggregate()

        self.aggregated_profiles = []
        self.aggregate_merge_col = f"Metadata_{self.aggregate_on.capitalize()}_Position"

        for metadata_level in self.file_aggregate:
            # uses custom load function to create df with metadata and profiles
            arr = [load_npz(x) for x in self.file_aggregate[metadata_level]["files"]]
            # empty dataframes from missing files are deleted
            arr = [x for x in arr if not x.empty]
            # if no files were found there is a miss-match between the index and the output files.
            if not len(arr):
                warnings.warn(
                    f"No files for the key {metadata_level} could be found.\nThis program will continue, but be aware that this might induce errors!"
                )
                continue

            df = pd.concat(arr)

            # the code around meta_df is a bit overcomplicated but it works for now.
            # at this point we are preparing the inputs for the aggregate function.
            meta_df = pd.DataFrame(
                self.file_aggregate[metadata_level]["metadata"], index=[0]
            )
            meta_df.columns = [f"Metadata_{x.capitalize()}" for x in meta_df.columns]

            if self.aggregate_on == "well":
                meta_df = meta_df.drop("Metadata_Site", axis="columns")

            metadata_cols = [x for x in df if x.startswith("Metadata_")]
            profiles = [x for x in df.columns.tolist() if x not in metadata_cols]
            df = df.assign(Metadata_Aggregate_On=self.aggregate_on)
            df = aggregate.aggregate(
                population_df=df,
                strata="Metadata_Aggregate_On",
                features=profiles,
                operation=self.aggregate_operation,
            )
            df.loc[:, self.aggregate_merge_col] = metadata_level
            df = meta_df.merge(df, left_index=True, right_index=True)
            self.aggregated_profiles.append(df)

        # now concatenate all of the above created profiles
        self.aggregated_profiles = pd.concat([x for x in self.aggregated_profiles])
        self.aggregated_profiles.columns = [
            str(x) for x in self.aggregated_profiles.columns
        ]
        meta_features = infer_cp_features(self.aggregated_profiles, metadata=True)
        reindex_profiles = [str(x) for x in profiles]
        self.aggregated_profiles = self.aggregated_profiles.reindex(
            meta_features + reindex_profiles, axis="columns"
        )

    def annotate_deep(
        self,
    ):
        """
        Main function of this class. Merges the index df and the profiles back into one dataframe.

        Returns
        -------
        df_out : pandas.dataframe
            dataframe with all metadata and the feature space. This is the input to any further pycytominer or pycytominer-eval processing
        """
        if not hasattr(self, "aggregated_profiles"):
            self.aggregate_deep()

        meta_df = self.index_df
        meta_df.columns = [
            "Metadata_{}".format(x) if not x.startswith("Metadata_") else x
            for x in meta_df.columns
        ]
        # prepare for merge with profiles

        if self.aggregate_on == "plate":
            meta_df = meta_df.drop(["Metadata_Site", "Metadata_Well"], axis="columns")
            merge_cols = ["Metadata_Plate"]
            meta_df = meta_df.drop_duplicates(subset=merge_cols)

            df_out = meta_df.merge(self.aggregated_profiles, on=merge_cols, how="inner")
            return df_out

        elif self.aggregate_on == "well":
            meta_df = meta_df.drop("Metadata_Site", axis="columns")
            merge_cols = ["Metadata_Well", "Metadata_Plate"]
            meta_df = meta_df.drop_duplicates(subset=merge_cols)

            df_out = meta_df.merge(self.aggregated_profiles, on=merge_cols, how="inner")
            return df_out

        # if on site level
        merge_cols = ["Metadata_Well", "Metadata_Plate", "Metadata_Site"]
        meta_df = meta_df.drop_duplicates(subset=merge_cols)
        df_out = meta_df.merge(self.aggregated_profiles, on=merge_cols, how="inner")
        return df_out
