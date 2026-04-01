"""
Module for loading profiles from files or dataframes.
"""

import csv
import gzip
import pathlib
from typing import Any, Optional, Union

import numpy as np
import pandas as pd

from pycytominer.cyto_utils.anndata_utils import AnnDataLike


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
        Raised if the provided path in the `file` does not exist.

    Notes
    -----
    If `file` is not a string or path-like object, the function prints a
    message and returns False rather than raising `TypeError`.
    """

    try:
        # strict=true tests if path exists
        path = pathlib.Path(file).resolve(strict=True)
    except FileNotFoundError:
        raise FileNotFoundError("load_profiles() didn't find the path.")
    except TypeError:
        print("Detected a non-str or non-path object in the `file` parameter.")
        return False

    # return boolean based on whether
    # file path is a parquet file
    return path.suffix.lower() == ".parquet"


def is_path_a_parquet_dataset_dir(file: Union[str, pathlib.Path]) -> bool:
    """Check whether a path is a parquet dataset directory.

    Parameters
    ----------
    file : Union[str, pathlib.Path]
        Path to inspect.

    Returns
    -------
    bool
        Returns True when the path is a directory, contains at least one direct
        file child, and all direct file children are parquet files.

    Raises
    ------
    FileNotFoundError
        Raised if the provided path in the `file` does not exist.

    Notes
    -----
    If `file` is not a string or path-like object, the function prints a
    message and returns False rather than raising `TypeError`.
    """

    try:
        path = pathlib.Path(file).resolve(strict=True)
    except FileNotFoundError:
        raise FileNotFoundError("load_profiles() didn't find the path.")
    except TypeError:
        print("Detected a non-str or non-path object in the `file` parameter.")
        return False

    if not path.is_dir():
        return False

    child_files = [child for child in path.iterdir() if child.is_file()]
    if not child_files:
        return False

    return all(child.suffix.lower() == ".parquet" for child in child_files)


def resolve_parquet_path(
    path_like: Union[str, pathlib.Path, pathlib.PurePath],
) -> Optional[pathlib.Path]:
    """Resolve file and dataset paths that pandas can read via parquet.

    Parameters
    ----------
    path_like : path-like
        Path to inspect.

    Returns
    -------
    pathlib.Path or None
        Resolved parquet file or dataset directory. Returns None when the path
        does not point to a parquet-backed source. This helper also resolves
        Iceberg-style table directories whose parquet data lives under a
        ``data/`` child directory, such as CytoTable warehouse tables.
    """

    try:
        path = pathlib.Path(path_like).resolve(strict=True)
    except FileNotFoundError:
        return None

    if path.is_file() and path.suffix.lower() == ".parquet":
        return path

    if is_path_a_parquet_dataset_dir(path):
        return path

    # Iceberg-style table directories typically store parquet fragments under
    # a sibling ``data/`` directory rather than at the table root itself.
    data_dir = path / "data"
    if data_dir.exists() and is_path_a_parquet_dataset_dir(data_dir):
        return data_dir

    return None


def resolve_cytotable_profiles_target(
    warehouse_path: Union[str, pathlib.Path, pathlib.PurePath],
) -> Optional[tuple[pathlib.Path, str, str]]:
    """Resolve a single profile table from a CytoTable-style warehouse.

    This helper only auto-resolves a target when exactly one parquet-backed
    profile table is present under the expected profile namespace layout. It
    does not infer which table to use based on downstream pycytominer
    operations or processing level; callers must be explicit when multiple
    profile tables are available.

    Parameters
    ----------
    warehouse_path : path-like
        Path to either the warehouse root or a project directory that contains
        a ``warehouse/`` directory.

    Returns
    -------
    tuple[pathlib.Path, str, str] or None
        Returns the resolved warehouse root path, namespace, and table name
        when exactly one parquet-backed profile table can be identified under
        the profile namespace. Returns None when the path does not expose a
        profile namespace in either ``<root>/profiles/<table>`` or
        ``<root>/warehouse/profiles/<table>`` form.

    Raises
    ------
    ValueError
        Raised when multiple parquet-backed profile tables are found and the
        intended target is ambiguous. This helper is only for the convenience
        case where a warehouse path exposes exactly one profile table. When
        multiple tables are present, use ``load_cytotable_profiles()`` with an
        explicit namespace and table name.
    """

    try:
        root = pathlib.Path(warehouse_path).resolve(strict=True)
    except FileNotFoundError:
        return None

    # Accept either the warehouse root itself or a parent project directory
    # that contains ``warehouse/``. This reflects the path shapes pycytominer
    # supports for local CytoTable-style fixtures; it is not meant to claim
    # that upstream tools always generate both forms.
    profile_roots = [root / "profiles", root / "warehouse" / "profiles"]

    resolved_targets = []
    ambiguous_roots = []

    for profile_root in profile_roots:
        if not profile_root.is_dir():
            continue

        candidates = [
            child
            for child in sorted(profile_root.iterdir())
            if child.is_dir() and resolve_parquet_path(child) is not None
        ]

        if len(candidates) == 1:
            # ``profiles`` is the CytoTable profile namespace/folder under the
            # warehouse root that load_cytotable_profiles() expects.
            resolved_targets.append((
                profile_root.parent,
                "profiles",
                candidates[0].name,
            ))

        if len(candidates) > 1:
            ambiguous_roots.append(profile_root)

    if len(resolved_targets) == 1:
        return resolved_targets[0]

    if len(resolved_targets) > 1 or ambiguous_roots:
        problem_roots = (
            resolved_targets if len(resolved_targets) > 1 else ambiguous_roots
        )
        raise ValueError(
            "Found multiple parquet-backed profile tables or candidates under "
            f"{problem_roots}. pycytominer only auto-resolves a warehouse path "
            "when exactly one profile table is available. Use "
            "load_cytotable_profiles() with an explicit namespace and "
            "table_name to select the target table."
        )

    return None


def load_cytotable_profiles(
    warehouse_path: Union[str, pathlib.Path, pathlib.PurePath],
    table_name: str = "joined_profiles",
    namespace: str = "profiles",
) -> pd.DataFrame:
    """Load a profile table from a CytoTable-style warehouse layout.

    This helper loads profile data stored as parquet fragments within an
    Iceberg-style table directory, typically under
    ``warehouse/<namespace>/<table_name>/data``, where namespace is typically
    ``profiles``. It is intended for CytoTable-style local outputs that
    organize tables by namespace and table name for
    downstream Pycytominer processing.

    Parameters
    ----------
    warehouse_path : path-like
        Path to either the warehouse root or the project directory that contains
        a `warehouse/` directory.
    table_name : str, default "joined_profiles"
        Table name to load from within the namespace. The default,
        ``joined_profiles``, is the conventional CytoTable table that joins
        object-level profile measurements across compartments into one profile
        table.
    namespace : str, default "profiles"
        Iceberg namespace that contains the table. For profile data this is
        typically `profiles`.

    Returns
    -------
    pd.DataFrame
        Loaded table as a pandas dataframe.

    Raises
    ------
    FileNotFoundError
        Raised when the requested table cannot be resolved to a parquet dataset.
    """

    root = pathlib.Path(warehouse_path).resolve(strict=True)
    candidate_paths = [
        root / namespace / table_name,
        root / "warehouse" / namespace / table_name,
    ]

    for candidate_path in candidate_paths:
        if not candidate_path.exists():
            continue
        if parquet_path := resolve_parquet_path(candidate_path):
            return pd.read_parquet(parquet_path, engine="pyarrow")

    raise FileNotFoundError(
        "Could not find a parquet-backed table for "
        f"namespace={namespace!r} and table_name={table_name!r} under {root}."
    )


def infer_delim(file: Union[str, pathlib.Path, Any]) -> str:
    """
    Sniff the delimiter in the given file

    Parameters
    ----------
    file : str
        File name

    Return
    ------
    str
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


def load_profiles(
    profiles: Union[str, pathlib.Path, pathlib.PurePath, pd.DataFrame, AnnDataLike],
) -> pd.DataFrame:
    """
    Unless a dataframe is provided, load the given profile dataframe from path or string.

    This loader supports direct files, parquet dataset directories, AnnData
    inputs, and unambiguous CytoTable-style warehouse roots that contain a
    single parquet-backed table under ``profiles/*/data``. This is the entry
    point used by higher-level functions such as ``normalize()`` and
    ``annotate()`` when they receive a path-like ``profiles`` input. If a
    warehouse path contains multiple profile tables, this loader will not guess
    which one to use; call ``load_cytotable_profiles()`` directly with an
    explicit ``table_name`` and ``namespace`` instead.

    Parameters
    ----------
    profiles :
        {str, pathlib.Path, pathlib.PurePath, pandas.DataFrame, ad.AnnData}
        File location, warehouse root, or in-memory profile data.

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
    if isinstance(profiles, (str, pathlib.Path, pathlib.PurePath)):
        if not pathlib.Path(profiles).exists():
            raise FileNotFoundError(
                f"load_profiles() didn't find the path: {profiles}."
            )

        parquet_path = resolve_parquet_path(profiles)
        if parquet_path is not None:
            return pd.read_parquet(parquet_path, engine="pyarrow")

        # Non-parquet paths may still be valid warehouse roots, AnnData inputs,
        # or delimited text files handled by the fallback branches below.
        cytotable_target = resolve_cytotable_profiles_target(profiles)
        if cytotable_target is not None:
            warehouse_root, namespace, table_name = cytotable_target
            return load_cytotable_profiles(
                warehouse_path=warehouse_root,
                table_name=table_name,
                namespace=namespace,
            )

    # Check if path is an AnnData file or object
    if (
        # do a check for anndata-like object
        all(hasattr(profiles, name) for name in ("X", "obs", "var"))
        or (
            # otherwise check for anndata file paths
            isinstance(profiles, (str, pathlib.Path, pathlib.PurePath))
            and pathlib.Path(profiles).suffix in [".zarr", ".zip", ".h5ad"]
        )
    ):
        # attempt an import of anndata and raise an error if not installed
        try:
            from pycytominer.cyto_utils.anndata_utils import is_anndata, read_anndata
        except ImportError:
            raise ImportError(
                """Optional dependency `anndata` is not installed.
                Please install the `anndata` optional dependency group:
                e.g. `pip install pycytominer[anndata]`
                """
            )
        if anndata_type := is_anndata(profiles):
            return read_anndata(profiles, anndata_type)

    # otherwise, assume its a csv/tsv file and infer the delimiter
    delim = infer_delim(profiles)
    return pd.read_csv(str(profiles), sep=delim)


def load_platemap(
    platemap: Union[str, pd.DataFrame], add_metadata_id=True
) -> pd.DataFrame:
    """
    Unless a dataframe is provided, load the given platemap dataframe from path or string

    Parameters
    ----------
    platemap : pd.DataFrame or str
        location or actual pd.DataFrame of platemap file

    add_metadata_id : bool
        boolean if "Metadata_" should be appended to all platemap columns

    Return
    ------
    platemap : pd.DataFrame
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
        platemap.columns = pd.Index([
            f"Metadata_{x}"
            if isinstance(x, str) and not x.startswith("Metadata_")
            else x
            for x in platemap.columns
        ])

    return platemap


def load_npz_features(
    npz_file: str, fallback_feature_prefix: str = "DP", metadata: bool = True
) -> pd.DataFrame:
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
    metadata : bool
        whether or not to load metadata [default: True]

    Return
    ------
    df : pd.DataFrame
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
        metadata_arr = npz["metadata"].item()
        metadata_df = pd.DataFrame(metadata_arr, index=range(0, df.shape[0]), dtype=str)
        metadata_df.columns = pd.Index([
            f"Metadata_{x}" if not x.startswith("Metadata_") else x
            for x in metadata_df.columns
        ])

        # Determine the appropriate metadata prefix
        if "Metadata_Model" in metadata_df.columns:
            feature_prefix = metadata_df.Metadata_Model.unique()[0]
        else:
            feature_prefix = fallback_feature_prefix
    else:
        feature_prefix = fallback_feature_prefix

    # Append feature prefix
    df.columns = pd.Index([
        f"{feature_prefix}_{x}" if not str(x).startswith(feature_prefix) else x
        for x in df.columns
    ])

    # Append metadata with features
    if "metadata" in files:
        df = metadata_df.merge(df, how="outer", left_index=True, right_index=True)

    return df


def load_npz_locations(
    npz_file: str, location_x_col_index: int = 0, location_y_col_index: int = 1
) -> pd.DataFrame:
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
    df : pd.DataFrame
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
    df.columns = pd.Index(["Location_Center_X", "Location_Center_Y"])
    return df
