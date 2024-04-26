"""This tests the output from a DeepProfiler run (May 2021)."""

import os
import pathlib
import random

import numpy.testing as npt
import pandas as pd
import pytest

from pycytominer import normalize
from pycytominer.cyto_utils import infer_cp_features
from pycytominer.cyto_utils.DeepProfiler_processing import (
    AggregateDeepProfiler,
    DeepProfilerData,
    SingleCellDeepProfiler,
)

ROOT_DIR = pathlib.Path(__file__).parents[2]
random.seed(42)


@pytest.fixture(scope="session")
def deep_profiler_data(tmp_path_factory):
    """This fixture returns the DeepProfilerData object and the output folder."""
    example_project_dir = ROOT_DIR / "tests" / "test_data" / "DeepProfiler_example_data"
    profile_dir = example_project_dir / "outputs" / "results" / "features"
    index_file = example_project_dir / "inputs" / "metadata" / "test_index.csv"

    output_folder = tmp_path_factory.mktemp("DeepProfiler")

    deep_data = DeepProfilerData(
        index_file=index_file,
        profile_dir=profile_dir,
    )

    return (deep_data, output_folder)


@pytest.fixture(scope="session")
def single_cell_deep_profiler(deep_profiler_data):
    """This fixture returns the single cell data and the SingleCellDeepProfiler object."""
    deep_data, output_folder = deep_profiler_data

    # compile single cell data from DP run
    single_cells_DP = SingleCellDeepProfiler(deep_data=deep_data)
    single_cells = single_cells_DP.get_single_cells(output=True)

    return single_cells, single_cells_DP, output_folder


def test_single_cell(single_cell_deep_profiler):
    """Test output from SingleCellDeepProfiler.get_single_cells()."""
    single_cells, single_cells_DP, output_folder = single_cell_deep_profiler

    meta_cols = [x for x in single_cells.columns if x.startswith("Location_")]
    assert meta_cols.index("Location_Center_X") == 0
    assert meta_cols.index("Location_Center_Y") == 1
    assert single_cells.shape == (10132, 6418)
    assert not single_cells.isnull().values.any()

    # Random value check
    npt.assert_almost_equal(single_cells.efficientnet_5.loc[5], -0.2235049)


def test_single_cell_normalize(single_cell_deep_profiler):
    """Test output from SingleCellDeepProfiler.normalize_deep_single_cells()."""
    single_cells, single_cells_DP, output_folder = single_cell_deep_profiler

    # normalize single cell data with DP processing
    output_file = output_folder / "normalized.csv"
    single_cells_normalized = single_cells_DP.normalize_deep_single_cells(
        output_file=output_file
    )

    # Build the expected normalized single cell data
    # extract metadata prior to normalization
    metadata_cols = infer_cp_features(single_cells, metadata=True)
    # locations are not automatically inferred with cp features
    metadata_cols.append("Location_Center_X")
    metadata_cols.append("Location_Center_Y")
    derived_features = [
        x for x in single_cells.columns.tolist() if x not in metadata_cols
    ]

    # wrapper for pycytominer.normalize() function
    expected_single_cell_normalize = normalize(
        profiles=single_cells,
        features=derived_features,
    )
    x_locations = single_cells["Location_Center_X"]
    expected_single_cell_normalize.insert(0, "Location_Center_X", x_locations)
    y_locations = single_cells["Location_Center_Y"]
    expected_single_cell_normalize.insert(1, "Location_Center_Y", y_locations)

    meta_cols = [
        x for x in single_cells_normalized.columns if x.startswith("Location_")
    ]
    assert meta_cols.index("Location_Center_X") == 0
    assert meta_cols.index("Location_Center_Y") == 1
    assert single_cells_normalized.shape == (10132, 6418)
    assert not single_cells_normalized.isnull().values.any()
    assert output_file.exists()
    pd.testing.assert_frame_equal(
        single_cells_normalized, expected_single_cell_normalize
    )
    # Random value check
    npt.assert_almost_equal(single_cells_normalized.efficientnet_3.loc[2], -0.70791286)


def test_aggregate(deep_profiler_data):
    deep_data, output_folder = deep_profiler_data

    # calculating the dataframe for each depth
    site_class = AggregateDeepProfiler(
        deep_data=deep_data,
        aggregate_operation="median",
        aggregate_on="site",
    )
    df_site = site_class.aggregate_deep()

    well_class = AggregateDeepProfiler(
        deep_data=deep_data,
        aggregate_operation="median",
        aggregate_on="well",
        output_file=output_folder,
    )
    df_well = well_class.aggregate_deep()

    plate_class = AggregateDeepProfiler(
        deep_data=deep_data,
        aggregate_operation="median",
        aggregate_on="plate",
        output_file=output_folder,
    )
    df_plate = plate_class.aggregate_deep()

    assert df_site.shape == (36, 6418)
    assert df_well.shape == (4, 6412)
    assert df_plate.shape == (2, 6406)

    aggregated_dfs = {"Site": df_site, "Well": df_well, "Plate": df_plate}

    for aggregate, df in aggregated_dfs.items():
        meta_cols = [x for x in df.columns if x.startswith("Metadata_")]
        model = df.Metadata_Model.unique()[0]
        profile_cols = [x for x in df.columns if x.startswith(f"{model}_")]
        assert profile_cols.index(f"{model}_6399")
        assert len(profile_cols) == 6400
        assert meta_cols.index(f"Metadata_{aggregate}_Position")
        assert not df.isnull().values.any()

    # Random value check
    npt.assert_almost_equal(df_plate.efficientnet_4.loc[1], -0.09470577538013458)
    npt.assert_almost_equal(df_well.efficientnet_0.loc[3], -0.16986790299415588)
    npt.assert_almost_equal(df_site.efficientnet_2.loc[14], -0.14057332277297974)


def test_output(single_cell_deep_profiler):
    single_cells, single_cells_DP, output_folder = single_cell_deep_profiler

    files = os.listdir(output_folder)
    files_should_be = [
        "normalized.csv",
        "SQ00014812.csv",
        "SQ00014813.csv",
        "SQ00014812_A01.csv",
        "SQ00014812_A07.csv",
        "SQ00014813_A02.csv",
        "SQ00014813_C05.csv",
    ]
    assert set(files) == set(files_should_be)
