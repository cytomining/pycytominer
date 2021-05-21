"""
This tests the output from a DeepProfiler run (May 2021)
"""
import os
import random
import pytest
import numpy as np
import pandas as pd
import sys
import numpy.testing as npt

from pycytominer.cyto_utils.DeepProfiler_processing import AggregateDeepProfiler


random.seed(42)
# setting the file locations
example_project_dir = os.path.join(
    os.path.dirname(__file__), "..", "test_data", "DeepProfiler_example_data"
)

profile_dir = os.path.join(example_project_dir, "outputs", "results", "features")

index_file = os.path.join(example_project_dir, "inputs", "metadata", "test_index.csv")

# calculating the dataframe for each depth
site_class = AggregateDeepProfiler(
    index_file=index_file, profile_dir=profile_dir, aggregate_operation='median', aggregate_on="site",
)
df_site = site_class.annotate_deep()

well_class = AggregateDeepProfiler(
    index_file=index_file, profile_dir=profile_dir, aggregate_operation='median', aggregate_on="well"
)
df_well = well_class.annotate_deep()
#
plate_class = AggregateDeepProfiler(
    index_file=index_file, profile_dir=profile_dir, aggregate_operation='median', aggregate_on="plate",
)
df_plate = plate_class.annotate_deep()


def test_output_size():
    assert df_site.shape == (36, 6417)
    assert df_well.shape == (4, 6416)
    assert df_plate.shape == (2, 6417)


def test_columns():
    meta_cols = [x for x in df_site.columns if x.startswith('Metadata_')]
    assert meta_cols.index('Metadata_Site_Position')
    assert len(meta_cols) == 17

    meta_cols = [x for x in df_well.columns if x.startswith('Metadata_')]
    assert meta_cols.index('Metadata_Well_Position')
    assert len(meta_cols) == 16

    meta_cols = [x for x in df_plate.columns if x.startswith('Metadata_')]
    assert meta_cols.index('Metadata_Plate_Position')
    assert len(meta_cols) == 17

    for df in [df_site, df_well, df_plate]:
        profile_cols = [x for x in df.columns if x.startswith('efficientnet_')]
        assert profile_cols.index('efficientnet_6399')
        assert len(profile_cols) == 6400


def test_for_nan():
    for df in [df_site, df_well, df_plate]:
        assert df.isnull().values.any()


def test_exact_values():
    # random value checks
    npt.assert_almost_equal(df_plate.efficientnet_4.loc[1], -0.09470577538013458)
    npt.assert_almost_equal(df_well.efficientnet_0.loc[3], -0.16986790299415588)
    npt.assert_almost_equal(df_site.efficientnet_2.loc[14], -0.14057332277297974)
