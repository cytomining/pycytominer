import os
import random
import pytest
import pathlib
import tempfile
import numpy as np
import pandas as pd
from pycytominer.cyto_utils import (
    load_profiles,
    load_platemap,
    load_npz_features,
    load_npz_locations,
)
from pycytominer.cyto_utils.load import infer_delim, is_path_a_parquet_file

random.seed(123)

# Get temporary directory
tmpdir = tempfile.gettempdir()

# Set file paths for data-to-be-loaded
output_data_file = os.path.join(tmpdir, "test_data.csv")
output_data_comma_file = os.path.join(tmpdir, "test_data_comma.csv")
output_data_parquet = os.path.join(tmpdir, "test_parquet.parquet")
output_data_gzip_file = "{}.gz".format(output_data_file)
output_platemap_file = os.path.join(tmpdir, "test_platemap.csv")
output_platemap_comma_file = os.path.join(tmpdir, "test_platemap_comma.csv")
output_platemap_file_gzip = "{}.gz".format(output_platemap_file)
output_npz_file = os.path.join(tmpdir, "test_npz.npz")
output_npz_with_model_file = os.path.join(tmpdir, "test_npz_withmodel.npz")
output_npz_without_metadata_file = os.path.join(tmpdir, "test_npz_withoutmetadata.npz")


# Example .npz file with real data
example_npz_file = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "data",
    "DeepProfiler_example_data",
    "Week1_22123_B02_s1.npz",
)

example_npz_file_locations = os.path.join(
    os.path.dirname(__file__),
    "..",
    "test_data",
    "DeepProfiler_example_data",
    "outputs",
    "results",
    "features",
    "SQ00014812",
    "A01_1.npz",
)

# Build data to use in tests
data_df = pd.concat(
    [
        pd.DataFrame(
            {
                "Metadata_Well": ["A01", "A02", "A03"],
                "x": [1, 3, 8],
                "y": [5, 3, 1],
            }
        ),
        pd.DataFrame(
            {
                "Metadata_Well": ["B01", "B02", "B03"],
                "x": [1, 3, 5],
                "y": [8, 3, 1],
            }
        ),
    ]
).reset_index(drop=True)

platemap_df = pd.DataFrame(
    {
        "well_position": ["A01", "A02", "A03", "B01", "B02", "B03"],
        "gene": ["x", "y", "z"] * 2,
    }
).reset_index(drop=True)

npz_metadata_dict = {"Plate": "PlateA", "Well": "A01", "Site": 2}
npz_model_key = {"Model": "cnn"}
npz_feats = data_df.drop("Metadata_Well", axis="columns").values

# Write to temp files
data_df.to_csv(output_data_file, sep="\t", index=False)
data_df.to_csv(output_data_comma_file, sep=",", index=False)
data_df.to_csv(output_data_gzip_file, sep="\t", index=False, compression="gzip")
data_df.to_parquet(output_data_parquet, engine="pyarrow")

platemap_df.to_csv(output_platemap_file, sep="\t", index=False)
platemap_df.to_csv(output_platemap_comma_file, sep=",", index=False)
platemap_df.to_csv(output_platemap_file_gzip, sep="\t", index=False, compression="gzip")

# Write npz temp files
key_values = {k: npz_metadata_dict[k] for k in npz_metadata_dict.keys()}
npz_metadata_dict.update(npz_model_key)
key_with_model_values = {k: npz_metadata_dict[k] for k in npz_metadata_dict.keys()}

np.savez_compressed(output_npz_file, features=npz_feats, metadata=key_values)
np.savez_compressed(
    output_npz_with_model_file,
    features=npz_feats,
    metadata=key_with_model_values,
)
np.savez_compressed(output_npz_without_metadata_file, features=npz_feats)


def test_infer_delim():
    delim = infer_delim(output_platemap_file)
    assert delim == "\t"

    delim = infer_delim(output_platemap_comma_file)
    assert delim == ","

    delim = infer_delim(output_platemap_file_gzip)
    assert delim == "\t"


def test_load_profiles():
    profiles = load_profiles(output_data_file)
    pd.testing.assert_frame_equal(data_df, profiles)

    profiles_gzip = load_profiles(output_data_gzip_file)
    pd.testing.assert_frame_equal(data_df, profiles_gzip)

    profiles_from_frame = load_profiles(data_df)
    pd.testing.assert_frame_equal(data_df, profiles_from_frame)

    profiles_from_parquet = load_profiles(output_data_parquet)
    pd.testing.assert_frame_equal(data_df, profiles_from_parquet)


def test_load_platemap():
    platemap = load_platemap(output_platemap_file, add_metadata_id=False)
    pd.testing.assert_frame_equal(platemap, platemap_df)

    platemap = load_platemap(output_platemap_comma_file, add_metadata_id=False)
    pd.testing.assert_frame_equal(platemap, platemap_df)

    platemap = load_platemap(output_platemap_file_gzip, add_metadata_id=False)
    pd.testing.assert_frame_equal(platemap, platemap_df)

    platemap_with_annotation = load_platemap(output_platemap_file, add_metadata_id=True)
    platemap_df.columns = [f"Metadata_{x}" for x in platemap_df.columns]
    pd.testing.assert_frame_equal(platemap_with_annotation, platemap_df)


def test_load_npz():
    npz_df = load_npz_features(output_npz_file)
    npz_custom_prefix_df = load_npz_features(
        output_npz_file, fallback_feature_prefix="test"
    )
    npz_with_model_df = load_npz_features(output_npz_with_model_file)
    npz_no_meta_df = load_npz_features(output_npz_without_metadata_file)
    real_data_df = load_npz_features(example_npz_file)
    real_locations_df = load_npz_locations(example_npz_file_locations)

    core_cols = ["Metadata_Plate", "Metadata_Well", "Metadata_Site"]

    assert npz_df.shape == (6, 5)
    assert npz_df.columns.tolist() == core_cols + ["DP_0", "DP_1"]

    assert npz_custom_prefix_df.shape == (6, 5)
    assert npz_custom_prefix_df.columns.tolist() == core_cols + [
        "test_0",
        "test_1",
    ]

    assert npz_with_model_df.shape == (6, 6)
    assert npz_with_model_df.columns.tolist() == core_cols + [
        "Metadata_Model",
        "cnn_0",
        "cnn_1",
    ]

    assert npz_no_meta_df.shape == (6, 2)
    assert npz_no_meta_df.columns.tolist() == ["DP_0", "DP_1"]

    pd.testing.assert_frame_equal(
        npz_df.drop(core_cols, axis="columns"), npz_no_meta_df
    )

    # Check real data
    assert real_data_df.shape == (206, 54)
    assert all([x in real_data_df.columns for x in core_cols + ["Metadata_Model"]])
    assert len(real_data_df.Metadata_Model.unique()) == 1
    assert real_data_df.Metadata_Model.unique()[0] == "cnn"
    assert real_data_df.drop(
        core_cols + ["Metadata_Model"], axis="columns"
    ).columns.tolist() == [f"cnn_{x}" for x in range(0, 50)]

    # Check locations data
    assert real_locations_df.shape == (229, 2)
    assert real_locations_df.columns.tolist() == [
        "Location_Center_X",
        "Location_Center_Y",
    ]

    # Check that column locations out of bounds throw error
    with pytest.raises(
        IndexError, match="OutOfBounds indexing via location_x_col_index"
    ):
        load_npz_locations(
            example_npz_file_locations,
            location_x_col_index=2,
            location_y_col_index=1,
        )
    with pytest.raises(
        IndexError, match="OutOfBounds indexing via location_y_col_index"
    ):
        load_npz_locations(
            example_npz_file_locations,
            location_x_col_index=0,
            location_y_col_index=2,
        )


def test_is_path_a_parquet_file():
    # checking parquet file
    check_pass = is_path_a_parquet_file(output_data_parquet)
    check_fail = is_path_a_parquet_file(output_data_file)

    # checking if the correct booleans are returned
    assert (check_pass, True)
    assert (check_fail, False)

    # loading in pandas dataframe from parquet file
    parquet_df = pd.read_parquet(output_data_parquet)
    parquet_profile_test = load_profiles(output_data_parquet)
    pd.testing.assert_frame_equal(parquet_profile_test, parquet_df)

    # loading csv file with new load_profile()
    csv_df = pd.read_csv(output_data_comma_file)
    csv_profile_test = load_profiles(output_data_comma_file)
    pd.testing.assert_frame_equal(csv_profile_test, csv_df)

    # checking if the same df is produced from parquet and csv files
    pd.testing.assert_frame_equal(parquet_profile_test, csv_profile_test)


def test_load_profiles_file_path_input():
    """
    The `load_profiles()` function will work input file arguments that resolve.
    This test confirms that different input file types work as expected.
    """
    # All paths should resolve and result in the same data being loaded
    data_file_os: str = os.path.join(tmpdir, "test_data.csv")
    data_file_path: pathlib.Path = pathlib.Path(tmpdir, "test_data.csv")
    data_file_purepath: pathlib.PurePath = pathlib.PurePath(tmpdir, "test_data.csv")

    profiles_os = load_profiles(data_file_os)
    profiles_path = load_profiles(data_file_path)
    profiles_purepath = load_profiles(data_file_purepath)

    pd.testing.assert_frame_equal(profiles_os, profiles_path)
    pd.testing.assert_frame_equal(profiles_purepath, profiles_path)

    # Testing non-existing file paths should result in expected behavior
    data_file_not_exist: pathlib.Path = pathlib.Path(tmpdir, "file_not_exist.csv")
    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        load_profiles(data_file_not_exist)
