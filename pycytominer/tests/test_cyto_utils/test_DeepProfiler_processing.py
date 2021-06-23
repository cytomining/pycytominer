"""
This tests the output from a DeepProfiler run (May 2021)
"""
import os
import random
import pytest
import numpy as np
import pandas as pd
import sys
import tempfile
import numpy.testing as npt


from pycytominer.cyto_utils.DeepProfiler_processing import AggregateDeepProfiler


tmpdir = tempfile.gettempdir()
random.seed(42)
# setting the file locations
example_project_dir = os.path.join(
    os.path.dirname(__file__), "..", "test_data", "DeepProfiler_example_data"
)

profile_dir = os.path.join(example_project_dir, "outputs", "results", "features")

index_file = os.path.join(example_project_dir, "inputs", "metadata", "test_index.csv")

output_folder = os.path.join(tmpdir, "DeepProfiler")


# calculating the dataframe for each depth
site_class = AggregateDeepProfiler(
    index_file=index_file,
    profile_dir=profile_dir,
    aggregate_operation="median",
    aggregate_on="site",
)
df_site = site_class.aggregate_deep()

well_class = AggregateDeepProfiler(
    index_file=index_file,
    profile_dir=profile_dir,
    aggregate_operation="median",
    aggregate_on="well",
    output_file=output_folder,
)
df_well = well_class.aggregate_deep()

plate_class = AggregateDeepProfiler(
    index_file=index_file,
    profile_dir=profile_dir,
    aggregate_operation="median",
    aggregate_on="plate",
    output_file=output_folder,
)
df_plate = plate_class.aggregate_deep()


def test_output_size():
    assert df_site.shape == (36, 6418)
    assert df_well.shape == (4, 6412)
    assert df_plate.shape == (2, 6406)


def test_output_files():
    files = os.listdir(output_folder)
    files_should_be = [
        "SQ00014812.csv",
        "SQ00014813.csv",
        "SQ00014812_A01.csv",
        "SQ00014812_A07.csv",
        "SQ00014813_A02.csv",
        "SQ00014813_C05.csv",
    ]
    assert set(files) == set(files_should_be)


def test_columns():
    meta_cols = [x for x in df_site.columns if x.startswith("Metadata_")]
    assert meta_cols.index("Metadata_Site_Position")

    meta_cols = [x for x in df_well.columns if x.startswith("Metadata_")]
    assert meta_cols.index("Metadata_Well_Position")

    meta_cols = [x for x in df_plate.columns if x.startswith("Metadata_")]
    assert meta_cols.index("Metadata_Plate_Position")

    for df in [df_site, df_well, df_plate]:
        model = df.Metadata_Model.unique()[0]
        profile_cols = [x for x in df.columns if x.startswith(f"{model}_")]
        assert profile_cols.index(f"{model}_6399")
        assert len(profile_cols) == 6400


def test_for_nan():
    for df in [df_site, df_well, df_plate]:
        assert not df.isnull().values.any()


def test_exact_values():
    # random value checks
    npt.assert_almost_equal(df_plate.efficientnet_4.loc[1], -0.09470577538013458)
    npt.assert_almost_equal(df_well.efficientnet_0.loc[3], -0.16986790299415588)
    npt.assert_almost_equal(df_site.efficientnet_2.loc[14], -0.14057332277297974)
