import csv
import numpy as np
import pandas as pd


def infer_delim(file):
    """
    Sniff the delimiter in the given file

    Arguments:
    file - a string indicating file name

    Output:
    the delimiter used in the dataframe (typically either tab or commas)
    """
    with open(file) as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.readline())

    return(dialect.delimiter)


def load_profiles(profiles):
    """
    Unless a dataframe is provided, load the given profile dataframe from path or string

    Arguments:
    profiles - location or actual pandas dataframe of profiles

    Return:
    pandas DataFrame of profiles
    """
    if not isinstance(profiles, pd.DataFrame):
        try:
            delim = infer_delim(profiles)
            profiles = pd.read_csv(profiles, sep=delim)
        except FileNotFoundError:
            raise FileNotFoundError(f"{profiles} profile file not found")
    return profiles


def load_platemap(platemap, add_metadata_id=True):
    """
    Unless a dataframe is provided, load the given platemap dataframe from path or string

    Arguments:
    platemap - location or actual pandas dataframe of platemap file
    add_metadata_id - boolean if "Metadata_" should be appended to all platemap columns

    Return:
    pandas DataFrame of profiles
    """
    if not isinstance(platemap, pd.DataFrame):
        try:
            delim = infer_delim(platemap)
            platemap = pd.read_csv(platemap, sep=delim)
        except FileNotFoundError:
            raise FileNotFoundError(f"{platemap} platemap file not found")

    if add_metadata_id:
        platemap.columns = [
            f"Metadata_{x}" if not x.startswith("Metadata_") else x
            for x in platemap.columns
        ]
    return platemap


def load_npz(npz_file, feature_prefix="DP"):
    """
    Load an npz file storing features and, sometimes, metadata

    Arguments:
    npz_file - file path to the compressed output (typically DeepProfiler output)
    feature_prefix - a string to prefix all features [default: "DP"]
    """
    npz = np.load(npz_file)
    files = npz.files

    df = pd.DataFrame(npz["features"])
    df.columns = [
        f"{feature_prefix}{x}" if not str(x).startswith(feature_prefix) else x
        for x in df
    ]

    if "metadata" in files:
        metadata = npz["metadata"].item()
        metadata_df = pd.DataFrame(metadata, index=range(0, df.shape[0]))
        metadata_df.columns = [
            f"Metadata_{x}" if not x.startswith("Metadata_") else x for x in metadata_df
        ]

        df = metadata_df.merge(df, how="outer", left_index=True, right_index=True)

    return df
