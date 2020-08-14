import os
import random
import pytest
import tempfile
import pandas as pd
from pycytominer.cyto_utils.load import infer_delim, load_profiles, load_platemap

random.seed(123)

# Get temporary directory
tmpdir = tempfile.gettempdir()

# Lauch a sqlite connection
output_data_file = os.path.join(tmpdir, "test_data.csv")
output_data_comma_file = os.path.join(tmpdir, "test_data_comma.csv")
output_platemap_file = os.path.join(tmpdir, "test_platemap.csv")
output_platemap_comma_file = os.path.join(tmpdir, "test_platemap_comma.csv")

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

# Write to temp files
data_df.to_csv(output_data_file, sep="\t", index=False)
data_df.to_csv(output_data_comma_file, sep=",", index=False)
platemap_df.to_csv(output_platemap_file, sep="\t", index=False)
platemap_df.to_csv(output_platemap_comma_file, sep=",", index=False)


def test_infer_delim():
    delim = infer_delim(output_platemap_file)
    assert delim == "\t"

    delim = infer_delim(output_platemap_comma_file)
    assert delim == ","


def test_load_profiles():

    profiles = load_profiles(output_data_file)
    pd.testing.assert_frame_equal(data_df, profiles)

    platemap = load_platemap(output_data_comma_file, add_metadata_id=False)
    pd.testing.assert_frame_equal(data_df, profiles)

    profiles_from_frame = load_profiles(data_df)
    pd.testing.assert_frame_equal(data_df, profiles_from_frame)


def test_load_platemap():

    platemap = load_platemap(output_platemap_file, add_metadata_id=False)
    pd.testing.assert_frame_equal(platemap, platemap_df)

    platemap = load_platemap(output_platemap_comma_file, add_metadata_id=False)
    pd.testing.assert_frame_equal(platemap, platemap_df)

    platemap_with_annotation = load_platemap(output_platemap_file, add_metadata_id=True)
    platemap_df.columns = [f"Metadata_{x}" for x in platemap_df.columns]
    pd.testing.assert_frame_equal(platemap_with_annotation, platemap_df)
