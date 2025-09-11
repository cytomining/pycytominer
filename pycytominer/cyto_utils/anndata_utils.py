"""
Utilities for working with AnnData objects and files.
"""

import pathlib
from typing import Any, Literal, Optional, TypeVar, Union, cast

import pandas as pd


class AnnDataLike:
    """
    An interface for objects that behave like AnnData objects
    without loading the actual AnnData package.
    """

    X: Any
    obs: Any
    var: Any


# create a custom type variable for anndata-like objects
Type_AnnDataLike = TypeVar("Type_AnnDataLike", bound=AnnDataLike)


def is_anndata(
    path_or_anndata_object: Union[str, pathlib.Path, AnnDataLike],
) -> Optional[str]:
    """
    Return AnnData category as str if
    path_or_anndata_object contains an AnnData dataset,
    AnnData object (H5AD, Zarr, or in-memory object) or
    path to an AnnData dataset.


    This function prefers using the AnnData readers directly:
    - in-memory AnnData objects are recognized directly.
    - H5AD files are opened in backed mode to avoid loading data into memory.
    (note:  anndata.experimental.read_lazy is likely to be a better option
    in the future once stable)
    - Zarr stores (directories or files like ``.zarr`` or ``.zip``) are read
      via :func:`anndata.read_zarr`.

    Args:
        path_or_anndata_object:
            File or directory to inspect.

    Returns:
        Str:
            If the path is an AnnData dataset, the type of store
            Otherwise, None.
    """
    from importlib.metadata import version

    import anndata as ad
    import h5py
    import zarr
    from packaging.version import Version

    # passthrough check if anndata in-memory object
    if isinstance(path_or_anndata_object, ad.AnnData):
        return "in-memory"

    try:
        # check that the path exists
        path = pathlib.Path(str(path_or_anndata_object)).resolve(strict=True)
    except FileNotFoundError:
        return None

    # Zarr stores can be directories (common) or files (e.g., zipped stores).
    # Try Zarr first for directories; for files, try H5AD then Zarr.
    # Note: we use a zarr-based approach for now but in the future
    # we should explore the use of lazy loading zarr stores via
    # anndata.experimental.read_lazy and/or anndata.io.sparse_dataset.
    if path.is_dir() or path.suffix == ".zip":
        try:
            zarr_store: Union[pathlib.Path, zarr.storage.ZipStore] = path
            # account for zipped zarr stores if zarr >= 3.0.0
            if path.suffix == ".zip" and Version(version("zarr")) >= Version("3"):
                zarr_store = zarr.storage.ZipStore(str(path))

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
        with h5py.File(path, "r") as f:
            # check the file encoding-type attribute for anndata
            if "anndata" in f.attrs.get("encoding-type", ""):
                return "h5ad"
            # raise an error if not anndata
            raise ValueError("Not an AnnData H5AD file.")
    # if we run into any error while attempting a read for h5ad
    except Exception:
        return None


def read_anndata(
    profiles: Union[str, pathlib.Path, pd.DataFrame, AnnDataLike], anndata_category: str
) -> pd.DataFrame:
    """
    Read an AnnData object or file and return a pandas DataFrame.

    Parameters
    ----------
    profiles :
        {str, pathlib.Path, AnnDataLike}
        file location or actual AnnData object of profiles
    anndata_type : str
        Type of AnnData input. One of "in-memory", "h5ad", or "zarr".

    Returns
    -------
    pandas DataFrame of profiles
    """
    import anndata as ad
    import h5py
    import zarr

    # conditional import for read_elem because the
    # location changed between anndata versions
    try:
        # 0.12+
        from anndata.io import read_elem
    except Exception:
        # 0.10.x
        from anndata.experimental import read_elem

    # conditional import for ZarrGroup because the
    # location changed between zarr versions
    # note: used for typing only
    try:
        from zarr import Group as ZarrGroup  # zarr >= 3
    except Exception:
        from zarr.hierarchy import Group as ZarrGroup  # zarr < 3

    if anndata_category == "h5ad":
        with h5py.File(profiles, "r") as f:
            obs = read_elem(f["obs"])
            var = read_elem(f["var"])
            X = read_elem(f["X"])
    elif anndata_category == "zarr":
        z = cast(ZarrGroup, zarr.open(profiles, mode="r"))
        obs = read_elem(z["obs"])
        var = read_elem(z["var"])
        X = read_elem(z["X"])
    elif anndata_category == "in-memory":
        adata: ad.AnnData = profiles
        df_out = adata.obs.join(adata.to_df(), how="left")
        df_out.index = df_out.index.astype(int)
        return df_out
    else:
        raise ValueError("Unrecognized AnnData type.")

    # Normalize X to a dense ndarray if needed
    if hasattr(X, "toarray"):  # works for scipy sparse and others
        X = X.toarray()

    # Convert to DataFrame using obs and var for indices and columns
    X_df = pd.DataFrame(X, index=obs.index, columns=var.index)

    # join obs and X_df
    df_out = obs.join(X_df, how="left")

    # ensure the index lines up with pandas default behavior
    df_out.index = df_out.index.astype(int)

    return df_out


def write_anndata(
    df: pd.DataFrame,
    output_filename: str,
    output_type: Literal["anndata_h5ad", "anndata_zarr"] = "anndata_h5ad",
) -> str:
    """
    Construct an AnnData object from a single DataFrame and write it to disk.

    Numeric columns are stored in ``X`` (observations x variables).
    Non-numeric columns are stored in ``.obs``.

    Args:
        df:
            Input table with mixed dtypes (numeric + non-numeric).
            Index becomes observation names; numeric columns
            become variables.
        output_filename:
            Destination path for the AnnDat aobject.
        output_type:
            One of ``"anndata_h5ad"``  (default)
            or ``"anndata_zarr"``.

    Returns:
        The ``output_filename`` path (as a string).
    """
    import anndata as ad

    # Split numeric vs non-numeric
    numeric_cols = df.select_dtypes(include="number").columns
    nonnumeric_cols = df.columns.difference(numeric_cols)

    # Build X explicitly (handle zero-variable case)
    if len(numeric_cols):
        X = df[numeric_cols].to_numpy(dtype=float)
        var_names = numeric_cols.astype(str)
    else:
        # Use a zero-width, initialized array to match n_obs and
        # avoid shape errors at write time.
        X = None
        var_names = pd.Index([], dtype=object)

    # Prepare obs
    df_nonnumeric = df[nonnumeric_cols].copy()

    # Make all string-like columns categorical (robust for NA/None)
    for c in df_nonnumeric.columns:
        col = df_nonnumeric[c]
        if pd.api.types.is_string_dtype(col.dtype) or col.dtype == object:
            # Convert to object first so pd.NA stays as missing,
            # then to categorical (AnnData writes these nicely)
            df_nonnumeric[c] = pd.Categorical(col.astype("object"))

    # For any remaining object columns that aren't strings (e.g., mixed),
    # replace pd.NA with None to avoid h5py issues.
    for c in df_nonnumeric.columns:
        if df_nonnumeric[c].dtype == object and not isinstance(
            df_nonnumeric[c].dtype, pd.CategoricalDtype
        ):
            df_nonnumeric[c] = df_nonnumeric[c].where(~pd.isna(df_nonnumeric[c]), None)

    # Build AnnData
    # important: X and obs should be set at the same time to avoid
    # potential shape misalignment issues at write time.
    adata = ad.AnnData(X=X, obs=df_nonnumeric)
    adata.var_names = var_names
    adata.obs_names = df.index.astype(str)

    # Write
    path = str(pathlib.Path(output_filename))
    if output_type == "anndata_h5ad":
        adata.write_h5ad(path)
    elif output_type == "anndata_zarr":
        adata.write_zarr(path)
    else:
        raise ValueError(
            f'Unsupported output_type="{output_type}". '
            'Use "anndata_h5ad" or "anndata_zarr".'
        )
    return path
