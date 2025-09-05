"""
Module for loading profiles from files or dataframes.
"""

import csv
import gzip
import os
import pathlib
from importlib.metadata import version
from typing import Any, Optional, Union

import anndata as ad
import numpy as np
import pandas as pd
import zarr
from packaging.version import Version


def is_path_a_parquet_file(file: Union[str, pathlib.Path]) -> bool:
    """Checks if the provided file path is a parquet file.

    Identify parquet files by inspecting the file extensions.
    If the file does not end with `parquet`, this will return False, else True.

    Parameters
    ----------
    file : Union[str, pathlib.Path]
        path to parquet file

    Returns
    -------
    bool
        Returns True if the file path contains `.parquet`, else it will return
        False

    Raises
    ------
    FileNotFoundError
        Raised if the provided path in the `file` does not exist
    """

    try:
        # expand user tilde and environment variables
        path = pathlib.Path(os.path.expandvars(file)).expanduser()
        # strict=true tests if path exists
        path.resolve(strict=True)
    except FileNotFoundError:
        raise FileNotFoundError("load_profiles() didn't find the path.")
    except TypeError:
        print("Detected a non-str or non-path object in the `file` parameter.")
        return False

    # return boolean based on whether
    # file path is a parquet file
    return path.suffix.lower() == ".parquet"


def infer_delim(file: Union[str, pathlib.Path, Any]):
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
        with open(file) as csvfile:
            line = csvfile.readline()
    except UnicodeDecodeError:
        with gzip.open(file, "r") as gzipfile:
            line = gzipfile.readline().decode()

    dialect = csv.Sniffer().sniff(line)

    return dialect.delimiter


def is_anndata(
    path_or_anndata_object: Union[str, pathlib.Path, ad.AnnData],
) -> Optional[str]:
    """
    Return anndata type as str if
    path_or_anndata_object contains an AnnData dataset
    or object (H5AD, Zarr, or in-memory object).


    This function prefers using the AnnData readers directly:
    - in-memory AnnData objects are recognized directly.
    - H5AD files are opened in backed mode to avoid loading data into memory.
    - Zarr stores (directories or files like ``.zarr`` or ``.zip``) are read
      via :func:`anndata.read_zarr`.

    The function is conservative: on any read error (or if AnnData is not
    installed), it returns None.

    Args:
        path_or_anndata_object:
            File or directory to inspect.

    Returns:
        Str:
            If the path is an AnnData dataset, the type of store
            Otherwise, None.
    """

    # passthrough check if anndata in-memory object
    if isinstance(path_or_anndata_object, ad.AnnData):
        return "in-memory"

    # Expand user tilde and environment variables
    path = pathlib.Path(os.path.expandvars(path_or_anndata_object)).expanduser()
    try:
        # check that the path exists
        path.resolve(strict=True)
    except FileNotFoundError:
        return None

    # Zarr stores can be directories (common) or files (e.g., zipped stores).
    # Try Zarr first for directories; for files, try H5AD then Zarr.
    # Note: we use a zarr-based approach for now but in the future
    # we should explore the use of lazy loading zarr stores via
    # anndata.experimental.read_lazy and/or anndata.io.sparse_dataset.
    if path.is_dir() or path.suffix == ".zip":
        try:
            zarr_store = path
            # account for zipped zarr stores if zarr >= 3.0.0
            if path.suffix == ".zip" and Version(version("zarr")) >= Version("3"):
                zarr_store = zarr.storage.ZipStore(path)

            # try to open the zarr store
            group = zarr.open_group(zarr_store, mode="r")

            # check the group encoding-type attribute for anndata
            if group.attrs.get("encoding-type") == "anndata":
                return "zarr"

        # if we run into any error while attempting a read for zarr
        # return None
        except Exception:
            return None

    # File path: first try H5AD (backed)
    try:
        ad.read_h5ad(path, backed="r")
        return "h5ad"
    except Exception:
        return None


def load_profiles(
    profiles: Union[str, pathlib.Path, pd.DataFrame, ad.AnnData],
) -> pd.DataFrame:
    """
    Unless a dataframe is provided, load the given profile dataframe from path or string

    Parameters
    ----------
    profiles :
        {str, pathlib.Path, pandas.DataFrame, ad.AnnData}
        file location or actual pandas dataframe of profiles

    Return
    ------
    pandas DataFrame of profiles

    Raises:
    -------
    FileNotFoundError
        Raised if the provided profile does not exists
    """

    # If already a dataframe, return it
    if isinstance(profiles, pd.DataFrame):
        return profiles

    # Check if path exists and load depending on file type
    if is_path_a_parquet_file(profiles):
        return pd.read_parquet(profiles, engine="pyarrow")

    # Check if path is an AnnData file
    anndata_type = is_anndata(profiles)
    if anndata_type:
        if anndata_type == "h5ad":
            adata = ad.read_h5ad(profiles, backed="r")
        elif anndata_type == "zarr":
            adata = ad.read_zarr(profiles)
        elif anndata_type == "in-memory":
            adata = profiles
        else:
            raise ValueError("Unrecognized AnnData type")

        # Convert to dataframe
        if adata.isbacked:
            # if we're backed by a file, just read it directly
            df = adata.obs.join(adata.to_df(), how="left")
        else:
            # if we're working with an adata in-memory object, copy it
            df = adata.obs.join(adata.to_df().copy(), how="left")

        return df.reset_index(drop=True)

    # otherwise, assume its a csv/tsv file and infer the delimiter
    delim = infer_delim(profiles)
    # also expand user tilde and environment variables in order to load the file
    return pd.read_csv(
        pathlib.Path(os.path.expandvars(profiles)).expanduser(), sep=delim
    )


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
