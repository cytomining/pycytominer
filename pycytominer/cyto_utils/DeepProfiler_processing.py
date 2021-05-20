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
    index_file : str
        file location of the index.csv from DP
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
    index_df

    filenames

    aggregated_profiles

    file_aggregate


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
        See above for parameters.
        """
        self.index_file = index_file
        self.profile_dir = profile_dir
        self.aggregate_operation = aggregate_operation
        self.aggregate_on = aggregate_on
        self.file_delimiter = file_delimiter
        self.file_extension = file_extension
        if not self.file_extension.startswith("."):
            self.file_extension = f".{self.file_extension}"
        self.index_df = pd.read_csv(index_file)

    def build_filenames(self):
        """
        Single cell profile file names indicated by plate, well, and site information
        """
        self.filenames = self.index_df.apply(
            self.build_filename_from_index, axis="columns"
        )
        self.filenames = [
            pathlib.PurePath(f"{self.profile_dir}/{x}") for x in self.filenames
        ]

    def build_filename_from_index(self, row):
        plate = row["Metadata_Plate"]
        well = row["Metadata_Well"]
        site = row["Metadata_Site"]

        filename = f"{plate}/{well}_{site}{self.file_extension}"
        return filename

    def extract_filename_metadata(self, npz_file, delimiter="_"):
        """Extract metadata (site, well and plate) from the filename.
        The input format of the file: path/plate/well_site.npz

        Returns
        -------
        dict with metadata
        """
        base_file = os.path.basename(npz_file).strip(".npz").split(delimiter)
        site = base_file[-1]
        well = base_file[-2]
        plate = str(npz_file).split('/')[-2]

        return {"site": site, "well": well, "plate": plate}

    def setup_aggregate(self):
        """Sets up the file_aggregate attribute. This is a helper function to aggregate_deep()
        """
        if not hasattr(self, "filenames"):
            self.build_filenames()

        self.file_aggregate = {} # this is considered bad practice, I believe - may wont to change
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

        Returns
        -------

        """
        if not hasattr(self, "file_aggregate"):
            self.setup_aggregate()

        self.aggregated_profiles = []
        self.aggregate_merge_col = f"Metadata_{self.aggregate_on.capitalize()}_Position"

        for metadata_level in self.file_aggregate:
            # uses custom load function to create df with metadata and profiles
            arr = [load_npz(x) for x in self.file_aggregate[metadata_level]["files"]]
            arr = [x for x in arr if not x.empty]
            # if no files were found there is a miss-match between the index and the output files.
            if not len(arr):
                warnings.warn(f'No files for the key {metadata_level} could be found.\nThis program will continue, but be aware that this might induce errors!')
                continue

            df = pd.concat(arr)
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
        self, merge_cols=["Metadata_Plate", "Metadata_Well"]
    ):
        """Main function of this class. Merges the metadata df and the profiles to one dataframe.

        Arguments
        ----------
        annotate_cols : [list of str]
            list of all column names that should be added to the output. By default these are all feature columns from the profiles
        merge_cols : [list of str], default = ["Metadata_Plate", "Metadata_Well"]
            List of columns which the metadata and profiles should merge on. These depends on the aggregate level

        Returns
        -------
        meta_df : pandas.dataframe
            dataframe with all metadata and the feature space. This is the input to any further pycytominer or pycytominer-eval processing
        """
        if not hasattr(self, "aggregated_profiles"):
            self.aggregate_deep()

        meta_df = self.index_df
        # select
        if self.aggregate_on == "well":
            meta_df = meta_df.drop("Metadata_Site", axis="columns")
            meta_df = meta_df.drop_duplicates(subset=['Metadata_Well', 'Metadata_Plate'])

        if self.aggregate_on == "Plate":
            meta_df = meta_df.drop(["Metadata_Site", 'Metadata_Well'], axis="columns")
            meta_df = meta_df.drop_duplicates(subset=['Metadata_Plate'])

        meta_df.columns = [
            "Metadata_{}".format(x) if not x.startswith("Metadata_") else x
            for x in meta_df.columns
        ]
        df_out = meta_df.merge(self.aggregated_profiles, on=merge_cols, how="inner")
        return df_out
