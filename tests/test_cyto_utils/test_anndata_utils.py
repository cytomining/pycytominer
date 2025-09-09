"""
Tests for cyto_utils/anndata_utils.py
"""

import pathlib
from typing import Literal, Union

import anndata as ad
import numpy as np
import pandas as pd
import pytest
from test_load import (
    adata,
    output_data_adata_hda5,
    output_data_adata_zarr,
    output_data_adata_zarr_zip,
    output_data_parquet,
)

from pycytominer.cyto_utils.anndata_utils import is_anndata, write_anndata


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


@pytest.mark.parametrize(
    "case, output_type, expect_format",
    [
        # mixed numeric + non-numeric
        ("mixed", "anndata_h5ad", "h5ad"),
        ("mixed", "anndata_zarr", "zarr"),
        # only numeric columns
        ("all_numeric", "anndata_h5ad", "h5ad"),
        # only non-numeric columns
        ("no_numeric", "anndata_h5ad", "h5ad"),
        # invalid output type should raise
        ("mixed", "bad_type", "error"),
    ],
)
def test_write_anndata_parametrized(
    case: Literal["mixed", "all_numeric", "no_numeric"],
    output_type: str,
    expect_format: Literal["h5ad", "zarr", "error"],
    tmp_path: pathlib.Path,
) -> None:
    # ---- build input DF per scenario ----
    idx = pd.Index([f"cell{i}" for i in range(4)], name="obs_id")

    if case == "mixed":
        df = pd.DataFrame(
            {
                "feat1": [1.0, 2.0, 3.0, 4.0],
                "feat2": [0.1, 0.2, 0.3, 0.4],
                "cell_type": ["A", "B", "A", "C"],
                "well": pd.Series(["A01", "A02", "A03", "A04"], dtype="string"),
            },
            index=idx,
        )
    elif case == "all_numeric":
        df = pd.DataFrame(
            {"f1": [1.0, 2.0, 3.0, 4.0], "f2": [5.0, 6.0, 7.0, 8.0]},
            index=idx,
        )
    elif case == "no_numeric":
        df = pd.DataFrame(
            {"cell_type": ["A", "B", "A", "C"], "batch": ["b1", "b1", "b2", "b2"]},
            index=idx,
        )
    else:
        raise AssertionError("unexpected case")

    # ---- choose output path by format ----
    if expect_format == "h5ad":
        out_path = tmp_path / "out.h5ad"
    elif expect_format == "zarr":
        out_path = tmp_path / "out.zarr"
    else:
        out_path = tmp_path / "out.h5ad"  # dummy when expecting error

    # ---- run & assert ----
    if expect_format == "error":
        with pytest.raises(ValueError):
            write_anndata(df=df, output_filename=str(out_path), output_type=output_type)  # type: ignore[arg-type]
        return

    # happy path
    ret = write_anndata(df=df, output_filename=str(out_path), output_type=output_type)  # type: ignore[arg-type]
    assert ret == str(out_path)

    # file/dir existence
    if expect_format == "h5ad":
        assert out_path.is_file()
        adata = ad.read_h5ad(str(out_path))
    else:
        assert out_path.is_dir()
        adata = ad.read_zarr(str(out_path))

    # basic integrity checks (kept simple)
    numeric_cols = df.select_dtypes(include="number").columns
    nonnumeric_cols = df.columns.difference(numeric_cols)

    # obs names preserved
    assert list(adata.obs_names) == list(df.index.astype(str))

    # var names match numeric columns (order)
    assert list(adata.var_names) == list(numeric_cols.astype(str))

    # X shape matches numeric slice
    assert adata.n_obs == df.shape[0]
    assert adata.n_vars == len(numeric_cols)

    # obs columns match non-numeric set (order may differ)
    assert set(adata.obs.columns) == set(nonnumeric_cols.astype(str))

    # spot-check X values if there are numeric cols (avoid heavy checks)
    if len(numeric_cols):
        X = adata.X
        if hasattr(X, "toarray"):
            X = X.toarray()
        np.testing.assert_allclose(
            X[:2, :], df[numeric_cols].to_numpy()[:2, :], rtol=1e-7, atol=0
        )
