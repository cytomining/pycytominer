import pathlib
import random
import tempfile

import pandas as pd
from pycytominer.annotate import annotate

random.seed(123)

# Get temporary directory
TMPDIR = tempfile.gettempdir()

# Setup a testing file
OUTPUT_FILE1 = pathlib.Path(f"{TMPDIR}/test.csv")
OUTPUT_FILE2 = pathlib.Path(f"{TMPDIR}/test.parquet")

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

EXTERNAL_METADATA_DF = pd.DataFrame(
    {"gene": ["x", "y", "z"], "pathway": ["a", "b", "c"], "time_h": [48] * 3}
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


def test_annotate_merge():
    # Test to ensure that the "_platemap" merge suffix is applied to the platemap columns when there is a name collision
    platemap_modified_df = PLATEMAP_DF.copy(deep=True)
    platemap_modified_df["x"] = [1, 2, 3, 4, 5, 6]

    expected_result = platemap_modified_df.merge(
        DATA_DF,
        left_on="well_position",
        right_on="Metadata_Well",
        suffixes=("_platemap", None),
    ).drop("well_position", axis="columns")[
        ["Metadata_Well", "gene", "x_platemap", "x", "y"]
    ]

    result = annotate(
        profiles=DATA_DF,
        platemap=platemap_modified_df,
        join_on=["well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
    )

    pd.testing.assert_frame_equal(result, expected_result)


def test_annotate_external():
    # Test that the external_metadata
    expected_result = (
        DATA_DF.merge(
            PLATEMAP_DF, left_on="Metadata_Well", right_on="well_position", how="left"
        )
        .merge(EXTERNAL_METADATA_DF, left_on="gene", right_on="gene", how="left")
        .rename(
            columns={
                "gene": "Metadata_gene",
                "pathway": "Metadata_pathway",
                "time_h": "Metadata_time_h",
            }
        )[
            [
                "Metadata_gene",
                "Metadata_Well",
                "Metadata_pathway",
                "Metadata_time_h",
                "x",
                "y",
            ]
        ]
    )

    result = annotate(
        profiles=DATA_DF,
        platemap=PLATEMAP_DF,
        external_metadata=EXTERNAL_METADATA_DF,
        join_on=["Metadata_well_position", "Metadata_Well"],
        external_join_left=["Metadata_gene"],
        external_join_right=["Metadata_gene"],
        add_metadata_id_to_platemap=True,
    )

    pd.testing.assert_frame_equal(result, expected_result)


def test_annotate_output():
    annotate(
        profiles=DATA_DF,
        platemap=PLATEMAP_DF,
        join_on=["well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
        output_file=OUTPUT_FILE1,
    )

    result = annotate(
        profiles=DATA_DF,
        platemap=PLATEMAP_DF,
        join_on=["well_position", "Metadata_Well"],
        add_metadata_id_to_platemap=False,
        output_file=None,
    )
    expected_result = pd.read_csv(OUTPUT_FILE1)

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
        output_file=None,
    )
    expected_result = pd.read_csv(compress_file)
    pd.testing.assert_frame_equal(result, expected_result)


def test_output_type():
    # dictionary with the output name associated with the file type
    output_dict = {"csv": OUTPUT_FILE1, "parquet": OUTPUT_FILE2}

    # test both output types available with output function
    for _type, outname in output_dict.items():
        # Test output
        annotate(
            profiles=DATA_DF,
            platemap=PLATEMAP_DF,
            join_on=["Metadata_well_position", "Metadata_Well"],
            output_file=outname,
            output_type=_type,
        )

    # read files in with pandas
    csv_df = pd.read_csv(OUTPUT_FILE1)
    parquet_df = pd.read_parquet(OUTPUT_FILE2)

    # check to make sure the files were read in corrrectly as a pd.Dataframe
    assert type(csv_df) == pd.DataFrame
    assert type(parquet_df) == pd.DataFrame
