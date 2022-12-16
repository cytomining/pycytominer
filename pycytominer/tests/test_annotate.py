import pathlib
import random
import tempfile

import pandas as pd
from pycytominer.annotate import annotate

random.seed(123)

# Get temporary directory
TMPDIR = tempfile.gettempdir()

# Setup a testing file
OUTPUT_FILE = pathlib.Path(f"{TMPDIR}/test.csv")

# Build data to use in tests
DATA_DF = pd.concat(
    [
        pd.DataFrame(
            {"Metadata_Well": ["A01", "A02", "A03"], "x": [1, 3, 8], "y": [5, 3, 1]}
        ),
        pd.DataFrame(
            {"Metadata_Well": ["B01", "B02", "B03"], "x": [1, 3, 5], "y": [8, 3, 1]}
        ),
    ]
).reset_index(drop=True)

PLATEMAP_DF = pd.DataFrame(
    {
        "well_position": ["A01", "A02", "A03", "B01", "B02", "B03"],
        "gene": ["x", "y", "z"] * 2,
    }
).reset_index(drop=True)


def test_annotate():

    # create expected result prior to annotate to distinguish modifications
    # performed by annotate to provided dataframes.
    expected_result = (
        PLATEMAP_DF.merge(DATA_DF, left_on="well_position", right_on="Metadata_Well")
        .rename(columns={"gene": "Metadata_gene"})
        .drop("well_position", axis="columns")
    )

    result = annotate(
        profiles=DATA_DF,
        platemap=PLATEMAP_DF,
        join_on=["Metadata_well_position", "Metadata_Well"],
    )

    pd.testing.assert_frame_equal(result, expected_result)


def test_annotate_platemap_naming():

    # Test annotate with the same column name in platemap and data.
    platemap_modified_df = PLATEMAP_DF.copy().rename(
        columns={"well_position": "Metadata_Well"}
    )

    expected_result = platemap_modified_df.merge(
        DATA_DF, left_on="Metadata_Well", right_on="Metadata_Well"
    ).rename(columns={"gene": "Metadata_gene"})

    result = annotate(
        profiles=DATA_DF,
        platemap=platemap_modified_df,
        join_on=["Metadata_Well", "Metadata_Well"],
    )

    pd.testing.assert_frame_equal(result, expected_result)


def test_annotate_output():

    annotate(
        profiles=DATA_DF,
        platemap=PLATEMAP_DF,
        join_on=["well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
        output_file=OUTPUT_FILE,
    )

    result = annotate(
        profiles=DATA_DF,
        platemap=PLATEMAP_DF,
        join_on=["well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
        output_file="none",
    )
    expected_result = pd.read_csv(OUTPUT_FILE)

    pd.testing.assert_frame_equal(result, expected_result)


def test_annotate_output_compress():

    compress_file = pathlib.Path(f"{TMPDIR}/test_annotate_compress.csv.gz")
    annotate(
        profiles=DATA_DF,
        platemap=PLATEMAP_DF,
        join_on=["well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
        output_file=compress_file,
        compression_options={"method": "gzip"},
    )

    result = annotate(
        profiles=DATA_DF,
        platemap=PLATEMAP_DF,
        join_on=["well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
        output_file="none",
    )
    expected_result = pd.read_csv(compress_file)
    pd.testing.assert_frame_equal(result, expected_result)
