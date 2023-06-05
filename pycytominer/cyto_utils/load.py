import csv
import gzip
import pathlib
import numpy as np
import pandas as pd
from typing import Union


def is_path_a_parquet_file(file: Union[str, pathlib.PurePath]) -> bool:
    """Checks if the provided file path is a parquet file.

    Identify parquet files by inspecting the file extensions.
    If the file does not end with `parquet`, this will return False, else True.

    Parameters
    ----------
    file : Union[str, pathlib.PurePath]
        path to parquet file

    Returns
    -------
    bool
        Returns True if the file path contains `.parquet`, else it will return
        False

    Raises
    ------
    TypeError
        Raised if a non str or non-path object is passed in the `file` parameter
    FileNotFoundError
        Raised if the provided path in the `file` does not exist
    """

    file = pathlib.PurePath(file)
    try:
        # strict=true tests if path exists
        file = pathlib.Path(file).resolve(strict=True)
    except FileNotFoundError as e:
        print("load_profiles() didn't find the path.", e, sep="\n")

    # Check if file path is a parquet file
    if file.suffix.lower() == ".parquet":
        return True

    return False


def infer_delim(file: str):
    """
    Sniff the delimiter in the given file

    Parameters
    ----------
    file : str
        File name

    Return
    ------
    the delimiter used in the dataframe (typically either tab or commas)
    """
    try:
        with open(file, "r") as csvfile:
            line = csvfile.readline()
    except UnicodeDecodeError:
        with gzip.open(file, "r") as gzipfile:
            line = gzipfile.readline().decode()

    dialect = csv.Sniffer().sniff(line)

    return dialect.delimiter


def load_profiles(profiles):
    """
    Unless a dataframe is provided, load the given profile dataframe from path or string

    Parameters
    ----------
    profiles : {str, pathlib.Path, pandas.DataFrame}
        file location or actual pandas dataframe of profiles

    Return
    ------
    pandas DataFrame of profiles

    Raises:
    -------
    FileNotFoundError
        Raised if the provided profile does not exists
    """
    if not isinstance(profiles, pd.DataFrame):
        # Check if path exists and load depending on file type
        if is_path_a_parquet_file(profiles):
            return pd.read_parquet(profiles, engine="pyarrow")

        else:
            delim = infer_delim(profiles)
            return pd.read_csv(profiles, sep=delim)

    return profiles


def load_platemap(platemap, add_metadata_id=True):
    """
    Unless a dataframe is provided, load the given platemap dataframe from path or string

    Parameters
    ----------
    platemap : pandas dataframe
        location or actual pandas dataframe of platemap file

    add_metadata_id : bool
        boolean if "Metadata_" should be appended to all platemap columns

    Return
    ------
    platemap : pandas.core.frame.DataFrame
        pandas DataFrame of profiles
    """
    if not isinstance(platemap, pd.DataFrame):
        try:
            delim = infer_delim(platemap)
            platemap = pd.read_csv(platemap, sep=delim)
        except FileNotFoundError:
            raise FileNotFoundError(f"{platemap} platemap file not found")
    else:
        # Setting platemap to a copy to prevent column name changes from back-propagating
        platemap = platemap.copy()

    if add_metadata_id:
        platemap.columns = [
            f"Metadata_{x}" if not x.startswith("Metadata_") else x
            for x in platemap.columns
        ]
    return platemap


def load_npz_features(npz_file, fallback_feature_prefix="DP", metadata=True):
    """
    Load an npz file storing features and, sometimes, metadata.

    The function will first search the .npz file for a metadata column called
    "Metadata_Model". If the field exists, the function uses this entry as the
    feature prefix. If it doesn't exist, use the fallback_feature_prefix.

    If the npz file does not exist, this function returns an empty dataframe.

    Parameters
    ----------
    npz_file : str
        file path to the compressed output (typically DeepProfiler output)
    fallback_feature_prefix :str
        a string to prefix all features [default: "DP"].

    Return
    ------
    df : pandas.core.frame.DataFrame
        pandas DataFrame of profiles
    """
    try:
        npz = np.load(npz_file, allow_pickle=True)
    except FileNotFoundError:
        return pd.DataFrame([])

    files = npz.files

    # Load features
    df = pd.DataFrame(npz["features"])

    if not metadata:
        return df

    # Load metadata
    if "metadata" in files:
        metadata = npz["metadata"].item()
        metadata_df = pd.DataFrame(metadata, index=range(0, df.shape[0]), dtype=str)
        metadata_df.columns = [
            f"Metadata_{x}" if not x.startswith("Metadata_") else x for x in metadata_df
        ]

        # Determine the appropriate metadata prefix
        if "Metadata_Model" in metadata_df.columns:
            feature_prefix = metadata_df.Metadata_Model.unique()[0]
        else:
            feature_prefix = fallback_feature_prefix
    else:
        feature_prefix = fallback_feature_prefix

    # Append feature prefix
    df.columns = [
        f"{feature_prefix}_{x}" if not str(x).startswith(feature_prefix) else x
        for x in df
    ]

    # Append metadata with features
    if "metadata" in files:
        df = metadata_df.merge(df, how="outer", left_index=True, right_index=True)

    return df


def load_npz_locations(npz_file, location_x_col_index=0, location_y_col_index=1):
    """
    Load an npz file storing locations and, sometimes, metadata.

    The function will first search the .npz file for a metadata column called
    "locations". If the field exists, the function uses this entry as the
    feature prefix.

    If the npz file does not exist, this function returns an empty dataframe.

    Parameters
    ----------
    npz_file : str
        file path to the compressed output (typically DeepProfiler output)
    location_x_col_index: int
        index of the x location column (which column in DP output has X coords)
    location_y_col_index: int
        index of the y location column (which column in DP output has Y coords)

    Return
    ------
    df : pandas.core.frame.DataFrame
        pandas DataFrame of profiles
    """
    try:
        npz = np.load(npz_file, allow_pickle=True)
    except FileNotFoundError:
        return pd.DataFrame([])

    # number of columns with data in the locations file
    num_location_cols = npz["locations"].shape[1]
    # throw error if user tries to index columns that don't exist
    if location_x_col_index >= num_location_cols:
        raise IndexError("OutOfBounds indexing via location_x_col_index")
    if location_y_col_index >= num_location_cols:
        raise IndexError("OutOfBounds indexing via location_y_col_index")

    df = pd.DataFrame(npz["locations"])
    df = df[[location_x_col_index, location_y_col_index]]
    df.columns = ["Location_Center_X", "Location_Center_Y"]
    return df
