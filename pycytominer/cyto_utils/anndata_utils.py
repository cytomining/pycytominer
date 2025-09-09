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

    import anndata as ad
    import zarr
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


def read_anndata(
    profiles: Union[str, pathlib.Path, pd.DataFrame, AnnDataLike], anndata_type: str
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

    if anndata_type == "h5ad":
        with h5py.File(profiles, "r") as f:
            obs = read_elem(f["obs"])
            var = read_elem(f["var"])
            X = read_elem(f["X"])
    elif anndata_type == "zarr":
        z = zarr.open(profiles, mode="r")
        obs = read_elem(z["obs"])
        var = read_elem(z["var"])
        X = read_elem(z["X"])
    elif anndata_type == "in-memory":
        adata: ad.AnnData = profiles
        return adata.obs.join(adata.to_df(), how="left")
    else:
        raise ValueError("Unrecognized AnnData type.")

    # Normalize X to a dense ndarray if needed
    if hasattr(X, "toarray"):  # works for scipy sparse and others
        X = X.toarray()

    # Convert to DataFrame using obs and var for indices and columns
    X_df = pd.DataFrame(X, index=obs.index, columns=var.index)

    # join obs and X_df
    return obs.join(X_df, how="left")
