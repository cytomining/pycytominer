"""
Tests for cyto_utils/anndata_utils.py
"""

import pathlib
from typing import Union

import anndata as ad
import pytest
from test_load import (
    adata,
    output_data_adata_hda5,
    output_data_adata_zarr,
    output_data_adata_zarr_zip,
    output_data_parquet,
)

from pycytominer.cyto_utils.anndata_utils import is_anndata


@pytest.mark.parametrize(
    "path_or_anndata_object, expected_result",
    [
        # 1) In-memory AnnData passthrough
        (adata, "in-memory"),
        # 2) Nonexistent path
        ("does_not_exist.h5ad", None),
        # 3) a non-Anndata file
        (output_data_parquet, None),
        # 4) H5AD file
        (output_data_adata_hda5, "h5ad"),
        # 5) Zarr anndata directory
        (output_data_adata_zarr, "zarr"),
        # 5b) Zarr anndata zipped directory
        (output_data_adata_zarr_zip, "zarr"),
        # 6) Empty directory
        ("empty_dir", None),
    ],
)
def test_is_anndata(
    path_or_anndata_object: Union[str, ad.AnnData],
    expected_result: Union[str, None],
    tmp_path: pathlib.Path,
) -> None:
    """
    Tests for is_anndata
    """

    if (
        isinstance(path_or_anndata_object, str)
        and path_or_anndata_object == "empty_dir"
    ):
        empty_dir = tmp_path / "empty_dir"
        empty_dir.mkdir()
        kind = is_anndata(empty_dir)
        assert kind is expected_result
    else:
        kind = is_anndata(path_or_anndata_object)
        assert kind == expected_result
