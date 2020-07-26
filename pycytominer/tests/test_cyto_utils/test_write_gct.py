import os
import csv
import tempfile
import pytest
import pandas as pd
from pycytominer.cyto_utils import write_gct

# Build data to use in tests
data_replicate_df = pd.concat(
    [
        pd.DataFrame(
            {
                "Metadata_g": "a",
                "Metadata_t": "t",
                "Cells_x": [1, 1, -1],
                "Cytoplasm_y": [5, 5, -5],
                "Nuclei_z": [2, 2, -2],
            }
        ),
        pd.DataFrame(
            {
                "Metadata_g": "b",
                "Metadata_t": "u",
                "Cells_x": [1, 3, 5],
                "Cytoplasm_y": [8, 3, 1],
                "Nuclei_z": [5, -2, 1],
            }
        ),
    ]
).reset_index(drop=True)

replicate_columns = ["g", "h"]
data_replicate_df = data_replicate_df.assign(Metadata_h=["c", "c", "c", "d", "d", "d"])


data_nocpfeatures_df = pd.concat(
    [
        pd.DataFrame({"g": "a", "x": [1, 1, -1], "y": [5, 5, -5], "z": [2, 2, -2]}),
        pd.DataFrame({"g": "b", "x": [1, 3, 5], "y": [8, 3, 1], "z": [5, -2, 1]}),
    ]
).reset_index(drop=True)

data_nocpfeatures_df = data_nocpfeatures_df.assign(h=["c", "c", "c", "d", "d", "d"])

# Get temporary directory
tmpdir = tempfile.gettempdir()


def test_write_gct():
    output_filename = os.path.join(tmpdir, "test_gct.gct")
    write_gct(
        profiles=data_replicate_df,
        output_file=output_filename,
        features="infer",
        version="#1.3",
    )
    gct_row_list = []
    with open(output_filename, "r") as gct_file:
        gctreader = csv.reader(gct_file, delimiter="\t")
        for row in gctreader:
            gct_row_list.append(row)

    assert gct_row_list[0] == ["#1.3"]
    assert gct_row_list[1] == ["3", "6", "1", "3"]
    assert gct_row_list[2] == [
        "id",
        "cp_feature_name",
        "SAMPLE_0",
        "SAMPLE_1",
        "SAMPLE_2",
        "SAMPLE_3",
        "SAMPLE_4",
        "SAMPLE_5",
    ]
    assert gct_row_list[3] == ["g", "nan", "a", "a", "a", "b", "b", "b"]
    assert gct_row_list[4] == ["t", "nan", "t", "t", "t", "u", "u", "u"]
    assert gct_row_list[5] == ["h", "nan", "c", "c", "c", "d", "d", "d"]
    assert gct_row_list[6] == ["Cells_x", "Cells_x", "1", "1", "-1", "1", "3", "5"]
    assert gct_row_list[7] == [
        "Cytoplasm_y",
        "Cytoplasm_y",
        "5",
        "5",
        "-5",
        "8",
        "3",
        "1",
    ]
    assert gct_row_list[8] == ["Nuclei_z", "Nuclei_z", "2", "2", "-2", "5", "-2", "1"]


def test_write_gct_infer_features():
    output_filename = os.path.join(tmpdir, "test_gct_nocp.gct")
    features = ["x", "y", "z"]
    meta_features = ["g", "h"]

    write_gct(
        profiles=data_nocpfeatures_df,
        output_file=output_filename,
        features=features,
        meta_features=meta_features,
        version="#1.3",
    )
    gct_row_list = []
    with open(output_filename, "r") as gct_file:
        gctreader = csv.reader(gct_file, delimiter="\t")
        for row in gctreader:
            gct_row_list.append(row)

    assert gct_row_list[0] == ["#1.3"]
    assert gct_row_list[1] == ["3", "6", "1", "2"]
    assert gct_row_list[2] == [
        "id",
        "cp_feature_name",
        "SAMPLE_0",
        "SAMPLE_1",
        "SAMPLE_2",
        "SAMPLE_3",
        "SAMPLE_4",
        "SAMPLE_5",
    ]
    assert gct_row_list[3] == ["g", "nan", "a", "a", "a", "b", "b", "b"]
    assert gct_row_list[4] == ["h", "nan", "c", "c", "c", "d", "d", "d"]
    assert gct_row_list[5] == ["x", "x", "1", "1", "-1", "1", "3", "5"]
    assert gct_row_list[6] == ["y", "y", "5", "5", "-5", "8", "3", "1"]
    assert gct_row_list[7] == ["z", "z", "2", "2", "-2", "5", "-2", "1"]


def test_write_gct_with_feature_metadata():
    output_filename = os.path.join(tmpdir, "test_gct_feature_meta.gct")

    feature_metadata = pd.DataFrame(
        {
            "id": ["color", "shape"],
            "Cells_x": ["blue", "triangle"],
            "Cytoplasm_y": ["red", "square"],
            "Nuclei_z": ["green", "oval"],
        }
    ).transpose()

    write_gct(
        profiles=data_replicate_df,
        output_file=output_filename,
        features="infer",
        meta_features="infer",
        feature_metadata=feature_metadata,
        version="#1.3",
    )
    gct_row_list = []
    with open(output_filename, "r") as gct_file:
        gctreader = csv.reader(gct_file, delimiter="\t")
        for row in gctreader:
            gct_row_list.append(row)

    assert gct_row_list[0] == ["#1.3"]
    assert gct_row_list[1] == ["3", "6", "2", "3"]
    assert gct_row_list[2] == [
        "id",
        "color",
        "shape",
        "SAMPLE_0",
        "SAMPLE_1",
        "SAMPLE_2",
        "SAMPLE_3",
        "SAMPLE_4",
        "SAMPLE_5",
    ]
    assert gct_row_list[3] == ["g", "nan", "nan", "a", "a", "a", "b", "b", "b"]
    assert gct_row_list[4] == ["t", "nan", "nan", "t", "t", "t", "u", "u", "u"]
    assert gct_row_list[5] == ["h", "nan", "nan", "c", "c", "c", "d", "d", "d"]
    assert gct_row_list[6] == [
        "Cells_x",
        "blue",
        "triangle",
        "1",
        "1",
        "-1",
        "1",
        "3",
        "5",
    ]
    assert gct_row_list[7] == [
        "Cytoplasm_y",
        "red",
        "square",
        "5",
        "5",
        "-5",
        "8",
        "3",
        "1",
    ]
    assert gct_row_list[8] == [
        "Nuclei_z",
        "green",
        "oval",
        "2",
        "2",
        "-2",
        "5",
        "-2",
        "1",
    ]


def test_write_gct_assert_error():
    with pytest.raises(AssertionError) as ae:
        output_filename = os.path.join(tmpdir, "test_gct_feature_meta_fail.gct")
        feature_metadata = pd.DataFrame(
            {
                "Cells_x": ["blue", "triangle"],
                "Cytoplasm_y": ["red", "square"],
                "Nuclei_z": ["green", "oval"],
            }
        ).transpose()

        write_gct(
            profiles=data_replicate_df,
            output_file=output_filename,
            features="infer",
            meta_features="infer",
            feature_metadata=feature_metadata,
            version="#1.3",
        )
    assert "make sure feature metadata has row named 'id'" in str(ae.value)
