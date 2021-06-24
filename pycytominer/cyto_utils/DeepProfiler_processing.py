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

    Attributes
    ----------
    profile_dir : str
        file location of the output profiles from DeepProfiler
        (e.g. `/project1/outputs/results/features/`)
    aggregate_operation : ['median', 'mean']
        method of aggregation
    aggregate_on : ['site', 'well', 'plate']
        up to which level to aggregate
    filename_delimiter : default = '_'
        delimiter for the filenames of the profiles (e.g. B02_4.npz).
    file_extension : default = '.npz'
        extension of the profile file.
    index_df : pandas.DataFrame
        load in the index.csv file from DeepProfiler, provided by an input index file.
    filenames : list of paths
        list of Purepaths that point to the npz files.
    aggregated_profiles : pandas.DataFrame
        df to hold the metadata and profiles.
    file_aggregate : dict
        dict that holds the file names and metadata.
        Is used to load in the npz files in the correct order and grouping.
    output_file : str
        If provided, will write annotated profiles to folder. Defaults to "none".

    Methods
    -------
    aggregate_deep()
        Given an initialized AggregateDeepProfiler() class, run this function to output
        level 3 profiles (aggregated profiles with annotated metadata).
    """

    def __init__(
        self,
        index_file,
        profile_dir,
        aggregate_operation="median",
        aggregate_on="well",
        filename_delimiter="_",
        file_extension=".npz",
        output_file="none",
    ):
        """
        __init__ function for this class.

        Arguments
        ---------
        index_file : str
            file location of the index.csv from DP

        See above for all other parameters.
        """
        assert aggregate_operation in [
            "median",
            "mean",
        ], "Input of aggregate_operation is incorrect, it must be either median or mean"
        assert aggregate_on in [
            "site",
            "well",
            "plate",
        ], "Input of aggregate_on is incorrect, it must be either site or well or plate"

        self.index_df = pd.read_csv(index_file, dtype=str)
        self.profile_dir = profile_dir
        self.aggregate_operation = aggregate_operation
        self.aggregate_on = aggregate_on
        self.filename_delimiter = filename_delimiter
        self.file_extension = file_extension
        if not self.file_extension.startswith("."):
            self.file_extension = f".{self.file_extension}"
        self.output_file = output_file

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
        Sets up the file_aggregate attribute. This is a helper function to aggregate_deep().

        the file_aggregate dictionary contains the file locations and metadata for each grouping.
        If for example we are grouping by well then the keys of self.file_aggregate would be:
        plate1/well1, plate1/well2, plate2/well1, etc.
        """
        if not hasattr(self, "filenames"):
            self.build_filenames()

        self.file_aggregate = {}
        for filename in self.filenames:
            file_info = self.extract_filename_metadata(
                filename, self.filename_delimiter
            )
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
        Main function of this class. Aggregates the profiles into a pandas dataframe.

        For each key in file_aggregate, the profiles are loaded, concatenated and then aggregated.
        If files are missing, we throw a warning but continue the code.
        After aggregation, the metadata is concatenated back onto the dataframe.

        Returns
        -------
        df_out : pandas.dataframe
            dataframe with all metadata and the feature space.
            This is the input to any further pycytominer or pycytominer-eval processing
        """
        if not hasattr(self, "file_aggregate"):
            self.setup_aggregate()

        self.aggregated_profiles = []
        self.aggregate_merge_col = f"Metadata_{self.aggregate_on.capitalize()}_Position"

        # Iterates over all sites, wells or plates
        for metadata_level in self.file_aggregate:
            # uses custom load function to create df with metadata and profiles
            arr = [load_npz(x) for x in self.file_aggregate[metadata_level]["files"]]
            # empty dataframes from missing files are deleted
            arr = [x for x in arr if not x.empty]
            # if no files were found there is a miss-match between the index and the output files
            if not len(arr):
                warnings.warn(
                    f"No files for the key {metadata_level} could be found.\nThis program will continue, but be aware that this might induce errors!"
                )
                continue
            df = pd.concat(arr)

            # extract metadata prior to aggregation
            meta_df = pd.DataFrame()
            metadata_cols = infer_cp_features(df, metadata=True)
            profiles = [x for x in df.columns.tolist() if x not in metadata_cols]

            # If all rows have the same Metadata information, that value is valid for the aggregated df
            for col in metadata_cols:
                if len(df[col].unique()) == 1:
                    meta_df[col] = [df[col].unique()[0]]

            # perform the aggregation
            df = df.assign(Metadata_Aggregate_On=self.aggregate_on)
            df = aggregate.aggregate(
                population_df=df,
                strata="Metadata_Aggregate_On",
                features=profiles,
                operation=self.aggregate_operation,
            ).reset_index(drop=True)

            # add the aggregation level as a column
            df.loc[:, self.aggregate_merge_col] = metadata_level
            # concatenate the metadata back onto the aggregated profile
            df = pd.concat([df, meta_df], axis=1)

            # save metalevel file
            if self.output_file != "none":
                if not os.path.exists(self.output_file):
                    os.mkdir(self.output_file)
                file_path = os.path.join(
                    self.output_file, metadata_level.replace("/", "_")
                )
                df.to_csv(f"{file_path}.csv", index=False)
            self.aggregated_profiles.append(df)

        # Concatenate all of the above created profiles
        self.aggregated_profiles = pd.concat(
            [x for x in self.aggregated_profiles]
        ).reset_index(drop=True)

        # clean and reindex columns
        self.aggregated_profiles.columns = [
            str(x) for x in self.aggregated_profiles.columns
        ]
        meta_features = infer_cp_features(self.aggregated_profiles, metadata=True)
        reindex_profiles = [str(x) for x in profiles]
        self.aggregated_profiles = self.aggregated_profiles.reindex(
            meta_features + reindex_profiles, axis="columns"
        )

        # If Columns have NaN values from concatenation, drop these
        self.aggregated_profiles.dropna(axis="columns", inplace=True)

        df_out = self.aggregated_profiles
        return df_out
