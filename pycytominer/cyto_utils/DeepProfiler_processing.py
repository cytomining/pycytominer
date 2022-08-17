"""
Utility function to load and process the output files of a DeepProfiler run.
"""
import os
import pathlib
import numpy as np
import pandas as pd
import warnings

from pycytominer import aggregate, normalize
from pycytominer.cyto_utils import (
    load_npz_features,
    load_npz_locations,
    infer_cp_features,
    output,
)


class DeepProfilerData:

    """This class holds all functions needed to load and annotate the DeepProfiler (DP) run.

    Attributes
    ----------
    profile_dir : str
        file location of the output profiles from DeepProfiler
        (e.g. `/project1/outputs/results/features/`)
    filename_delimiter : default = '_'
        delimiter for the filenames of the profiles (e.g. B02_4.npz).
    file_extension : default = '.npz'
        extension of the profile file.
    index_df : pandas.DataFrame
        load in the index.csv file from DeepProfiler, provided by an input index file.
    filenames : list of paths
        list of Purepaths that point to the npz files.

    Methods
    -------
    build_filenames()
        build filenames from index_df
    extract_filename_metadata(npz_file, delimiter="_")
        get site, well, plate info for npz file
    """

    def __init__(
        self,
        index_file,
        profile_dir,
        filename_delimiter="_",
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

        self.index_df = pd.read_csv(index_file, dtype=str)
        self.profile_dir = profile_dir
        self.filename_delimiter = filename_delimiter
        self.file_extension = file_extension
        if not self.file_extension.startswith("."):
            self.file_extension = f".{self.file_extension}"

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

        filename = f"{plate}/{well}{self.filename_delimiter}{site}{self.file_extension}"
        return filename

    def extract_filename_metadata(self, npz_file, delimiter="_"):
        """
        Extract metadata (site, well and plate) from the filename.
        The input format of the file: path/plate/well{delimiter}site.npz

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
        if delimiter == "/":
            site = str(npz_file).split("/")[-1].strip(".npz")
            well = str(npz_file).split("/")[-2]
        else:
            base_file = os.path.basename(npz_file).strip(".npz").split(delimiter)
            site = base_file[-1]
            well = base_file[-2]
        plate = str(npz_file).split("/")[-2]

        loc = {"site": site, "well": well, "plate": plate}
        return loc


class AggregateDeepProfiler:

    """This class holds all functions needed to aggregate the DeepProfiler (DP) run.

    Attributes
    ----------
    deep_data : DeepProfilerData
        DeepProfilerData object to load data from DeepProfiler project
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

    Example
    -------
    import pathlib
    from pycytominer.cyto_utils import DeepProfiler_processing

    index_file = pathlib.Path("path/to/index.csv")
    profile_dir = pathlib.Path("path/to/features/")

    deep_data = DeepProfiler_processing.DeepProfilerData(index_file, profile_dir, filename_delimiter="/", file_extension=".npz")
    deep_aggregate = DeepProfiler_processing.AggregateDeepProfiler(deep_data)
    deep_aggregate = aggregate.aggregate_deep()
    """

    def __init__(
        self,
        deep_data: DeepProfilerData,
        aggregate_operation="median",
        aggregate_on="well",
        output_file="none",
    ):
        """
        __init__ function for this class.

        Arguments
        ---------
        See above for all parameters.
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

        self.deep_data = deep_data
        self.aggregate_operation = aggregate_operation
        self.aggregate_on = aggregate_on
        self.output_file = output_file

    def setup_aggregate(self):
        """
        Sets up the file_aggregate attribute. This is a helper function to aggregate_deep().

        the file_aggregate dictionary contains the file locations and metadata for each grouping.
        If for example we are grouping by well then the keys of self.file_aggregate would be:
        plate1/well1, plate1/well2, plate2/well1, etc.
        """
        if not hasattr(self.deep_data, "filenames"):
            self.deep_data.build_filenames()

        self.file_aggregate = {}
        for filename in self.deep_data.filenames:
            file_info = self.deep_data.extract_filename_metadata(
                filename, self.deep_data.filename_delimiter
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
            arr = [
                load_npz_features(x)
                for x in self.file_aggregate[metadata_level]["files"]
            ]
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


class SingleCellDeepProfiler:

    """This class holds functions needed to analyze single cells from the DeepProfiler (DP) run. Only pycytominer.normalization() is implemented.

    Attributes
    ----------
    deep_data : DeepProfilerData
        DeepProfilerData object to load data from DeepProfiler project
    aggregated_profiles : pandas.DataFrame
        df to hold the metadata and profiles.
    file_aggregate : dict
        dict that holds the file names and metadata.
        Is used to load in the npz files in the correct order and grouping.
    output_file : str
        If provided, will write annotated profiles to folder. Defaults to "none".

    Methods
    -------
    normalize(profiles, features, image_features, meta_features, samples, method, output_file, compression_options,
    float_format, mad_robustize_epsilon, spherize_center, spherize_method, spherize_epsilon)
        normalize profiling features from DeepProfiler run with pycytominer.normalize()

    Example
    -------
    import pathlib
    from pycytominer.cyto_utils import DeepProfiler_processing

    index_file = pathlib.Path("path/to/index.csv")
    profile_dir = pathlib.Path("path/to/features/")

    deep_data = DeepProfiler_processing.DeepProfilerData(index_file, profile_dir, filename_delimiter="/", file_extension=".npz")
    deep_single_cell = DeepProfiler_processing.SingleCellDeepProfiler(deep_data)
    normalized = deep_single_cell.normalize_deep_single_cells()
    """

    def __init__(
        self,
        deep_data: DeepProfilerData,
    ):
        """
        __init__ function for this class.

        Arguments
        ---------
        See above for all parameters.
        """

        self.deep_data = deep_data

    def get_single_cells(self, output=False, location_x_col_index = 0, location_y_col_index = 1):
        """
        Sets up the single_cells attribute or output as a variable. This is a helper function to normalize_deep_single_cells().
        single_cells is a pandas dataframe in the format expected by pycytominer.normalize().

        Arguments
        -----------
        output : bool
            If true, will output the single cell dataframe instead of setting to self attribute
        location_x_col_index: int
            index of the x location column (which column in DP output has X coords)
        location_y_col_index: int
            index of the y location column (which column in DP output has Y coords)
        """
        # build filenames if they do not already exist
        if not hasattr(self.deep_data, "filenames"):
            self.deep_data.build_filenames()

        # compile features dataframe with single cell locations
        total_df = []
        for features_path in self.deep_data.filenames:
            features = load_npz_features(features_path)
            # skip a file if there are no features
            if len(features.index) == 0:
                warnings.warn(
                    f"No features could be found at {features_path}.\nThis program will continue, but be aware that this might induce errors!"
                )
                continue
            locations = load_npz_locations(features_path, location_x_col_index, location_y_col_index)
            detailed_df = pd.concat([locations, features], axis=1)

            total_df.append(detailed_df)

        sc_df = pd.concat(total_df).reset_index(drop=True)
        if output:
            return sc_df
        else:
            self.single_cells = sc_df

    def normalize_deep_single_cells(
        self,
        location_x_col_index = 0, 
        location_y_col_index = 1,
        image_features=False,  # not implemented with DeepProfiler
        meta_features="infer",
        samples="all",
        method="standardize",
        output_file="none",
        compression_options=None,
        float_format=None,
        mad_robustize_epsilon=1e-18,
        spherize_center=True,
        spherize_method="ZCA-cor",
        spherize_epsilon=1e-6,
    ):

        """
        Normalizes all cells into a pandas dataframe.

        For each file in the DP project features folder, the features from each cell are loaded.
        These features are put into a profiles dataframe for use in pycytominer.normalize.
        A features list is also compiled for use in pycytominer.normalize.

        Returns
        -------
        df_out : pandas.dataframe
            dataframe with all metadata and the feature space.
            This is the input to any further pycytominer or pycytominer-eval processing
        """
        print("getting single cells")
        # setup single_cells attribute
        if not hasattr(self, "single_cells"):
            self.get_single_cells(output=False, location_x_col_index=location_x_col_index, location_y_col_index=location_y_col_index)

        # extract metadata prior to normalization
        metadata_cols = infer_cp_features(self.single_cells, metadata=True)
        # locations are not automatically inferred with cp features
        metadata_cols.append("Location_Center_X")
        metadata_cols.append("Location_Center_Y")
        derived_features = [
            x for x in self.single_cells.columns.tolist() if x not in metadata_cols
        ]

        # wrapper for pycytominer.normalize() function
        normalized = normalize.normalize(
            profiles=self.single_cells,
            features=derived_features,
            image_features=image_features,
            meta_features=meta_features,
            samples=samples,
            method=method,
            output_file="none",
            compression_options=compression_options,
            float_format=float_format,
            mad_robustize_epsilon=mad_robustize_epsilon,
            spherize_center=spherize_center,
            spherize_method=spherize_method,
            spherize_epsilon=spherize_epsilon,
        )

        # move x locations and y locations to metadata columns of normalized df
        x_locations = self.single_cells["Location_Center_X"]
        normalized.insert(0, "Location_Center_X", x_locations)
        y_locations = self.single_cells["Location_Center_Y"]
        normalized.insert(1, "Location_Center_Y", y_locations)

        # separate code because normalize() will not return if it has an output file specified
        if output_file != "none":
            output(
                df=normalized,
                output_filename=output_file,
                compression_options=compression_options,
                float_format=float_format,
            )

        return normalized
