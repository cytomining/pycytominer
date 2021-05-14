import os
import tempfile
import random
import pandas as pd
from pycytominer.annotate import annotate

random.seed(123)

# Get temporary directory
tmpdir = tempfile.gettempdir()

# Setup a testing file
output_file = os.path.join(tmpdir, "test.csv")

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


def test_annotate():
    result = annotate(
        profiles=data_df,
        platemap=platemap_df,
        join_on=["Metadata_well_position", "Metadata_Well"],
    )

    expected_result = platemap_df.merge(
        data_df, left_on="Metadata_well_position", right_on="Metadata_Well"
    ).drop(["Metadata_well_position"], axis="columns")

    pd.testing.assert_frame_equal(result, expected_result)


def test_annotate_write():
    _ = annotate(
        profiles=data_df,
        platemap=platemap_df,
        join_on=["Metadata_well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
        output_file=output_file,
    )

    result = annotate(
        profiles=data_df,
        platemap=platemap_df,
        join_on=["Metadata_well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
        output_file="none",
    )
    expected_result = pd.read_csv(output_file)

    pd.testing.assert_frame_equal(result, expected_result)


def test_annotate_compress():
    compress_file = os.path.join(tmpdir, "test_annotate_compress.csv.gz")
    _ = annotate(
        profiles=data_df,
        platemap=platemap_df,
        join_on=["Metadata_well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
        output_file=compress_file,
        compression_options={"method": "gzip"},
    )

    result = annotate(
        profiles=data_df,
        platemap=platemap_df,
        join_on=["Metadata_well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
        output_file="none",
    )
    expected_result = pd.read_csv(compress_file)
    pd.testing.assert_frame_equal(result, expected_result)
