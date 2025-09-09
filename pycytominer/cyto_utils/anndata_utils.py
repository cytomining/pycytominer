"""
Utilities for working with AnnData objects and files.
"""

import pathlib

from typing import Any, Optional, TypeVar, Union
import pandas as pd

class AnnDataLike:
    """
    An interface for objects that behave like AnnData objects
    without loading the actual AnnData package.
    """

    X: Any
    obs: Any
    var: Any

# create an anntada-like type variable
Type_AnnDataLike = TypeVar("Type_AnnDataLike", bound=AnnDataLike)

def is_anndata(
    path_or_anndata_object: Union[str, pathlib.Path, AnnDataLike],
) -> Optional[str]:
    """
    Return anndata type as str if
    path_or_anndata_object contains an AnnData dataset
    or object (H5AD, Zarr, or in-memory object).


    This function prefers using the AnnData readers directly:
    - in-memory AnnData objects are recognized directly.
    - H5AD files are opened in backed mode to avoid loading data into memory.
    (note:  anndata.experimental.read_lazy is likely to be a better option
    in the future once stable)
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
    from importlib.metadata import version

    import zarr
    import anndata as ad
    from packaging.version import Version

    # passthrough check if anndata in-memory object
    if isinstance(path_or_anndata_object, ad.AnnData):
        return "in-memory"

    try:
        # check that the path exists
        path = pathlib.Path(path_or_anndata_object).resolve(strict=True)
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
    

def read_anndata(profiles: Union[str, pathlib.Path, pd.DataFrame, AnnDataLike], anndata_type: str) -> pd.DataFrame:
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

    if anndata_type == "h5ad":
        adata = ad.read_h5ad(profiles, backed="r")
    elif anndata_type == "zarr":
        adata = ad.read_zarr(profiles)
    elif anndata_type == "in-memory":
        adata = profiles
    else:
        raise AssertionError("Unrecognized AnnData type")

    # Convert to dataframe
    if adata.isbacked:
        # if we're backed by a file, just read it directly
        df = adata.obs.join(adata.to_df(), how="left")
    else:
        # if we're working with an adata in-memory object, copy it
        df = adata.obs.join(adata.to_df().copy(), how="left")

    return df.reset_index(drop=True)