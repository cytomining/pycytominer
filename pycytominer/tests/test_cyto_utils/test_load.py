import os
import random
import pytest
import tempfile
import numpy as np
import pandas as pd
from pycytominer.cyto_utils import load_profiles, load_platemap, load_npz
from pycytominer.cyto_utils.load import infer_delim

random.seed(123)

# Get temporary directory
tmpdir = tempfile.gettempdir()

# Set file paths for data-to-be-loaded
output_data_file = os.path.join(tmpdir, "test_data.csv")
output_data_comma_file = os.path.join(tmpdir, "test_data_comma.csv")
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

# Build data to use in tests
data_df = pd.concat(
    [
        pd.DataFrame(
            {"Metadata_Well": ["A01", "A02", "A03"], "x": [1, 3, 8], "y": [5, 3, 1]}
        ),
        pd.DataFrame(
            {"Metadata_Well": ["B01", "B02", "B03"], "x": [1, 3, 5], "y": [8, 3, 1]}
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
platemap_df.to_csv(output_platemap_file, sep="\t", index=False)
platemap_df.to_csv(output_platemap_comma_file, sep=",", index=False)
platemap_df.to_csv(output_platemap_file_gzip, sep="\t", index=False, compression="gzip")

# Write npz temp files
key_values = {k: npz_metadata_dict[k] for k in npz_metadata_dict.keys()}
npz_metadata_dict.update(npz_model_key)
key_with_model_values = {k: npz_metadata_dict[k] for k in npz_metadata_dict.keys()}

np.savez_compressed(output_npz_file, features=npz_feats, metadata=key_values)
np.savez_compressed(
    output_npz_with_model_file, features=npz_feats, metadata=key_with_model_values
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

    platemap = load_platemap(output_data_comma_file, add_metadata_id=False)
    pd.testing.assert_frame_equal(data_df, profiles)

    profiles_from_frame = load_profiles(data_df)
    pd.testing.assert_frame_equal(data_df, profiles_from_frame)


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
    npz_df = load_npz(output_npz_file)
    npz_custom_prefix_df = load_npz(output_npz_file, fallback_feature_prefix="test")
    npz_with_model_df = load_npz(output_npz_with_model_file)
    npz_no_meta_df = load_npz(output_npz_without_metadata_file)
    real_data_df = load_npz(example_npz_file)

    core_cols = ["Metadata_Plate", "Metadata_Well", "Metadata_Site"]

    assert npz_df.shape == (6, 5)
    assert npz_df.columns.tolist() == core_cols + ["DP_0", "DP_1"]

    assert npz_custom_prefix_df.shape == (6, 5)
    assert npz_custom_prefix_df.columns.tolist() == core_cols + ["test_0", "test_1"]

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
