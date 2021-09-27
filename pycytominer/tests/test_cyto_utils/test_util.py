import os
import random
import pytest
import tempfile
import warnings
import pandas as pd
from pycytominer.cyto_utils.util import (
    check_compartments,
    get_default_compartments,
    load_known_metadata_dictionary,
    get_pairwise_correlation,
    check_correlation_method,
    check_aggregate_operation,
    check_consensus_operation,
    check_fields_of_view,
    check_fields_of_view_format,
    check_image_features,
    extract_image_features,
)

tmpdir = tempfile.gettempdir()


def test_check_compartments():
    valid = ["cells"]
    assert check_compartments(valid) is None

    valid = ["CeLLs"]
    assert check_compartments(valid) is None

    valid = "cells"
    assert check_compartments(valid) is None

    valid = "CeLLs"
    assert check_compartments(valid) is None

    valid = ["cells", "nuclei", "cytoplasm"]
    assert check_compartments(valid) is None

    valid = ["CeLLs", "nucLEI", "CYTOplasm"]
    assert check_compartments(valid) is None


def test_check_compartments_not_valid():
    warn_expected_string = "Non-canonical compartment detected: something"
    warnings.simplefilter("always")
    with warnings.catch_warnings(record=True) as w:
        not_valid = ["SOMETHING"]
        output = check_compartments(not_valid)
    assert issubclass(w[-1].category, UserWarning)
    assert warn_expected_string in str(w[-1].message)

    with warnings.catch_warnings(record=True) as w:
        not_valid = "SOMETHING"  # Also works with strings
        output = check_compartments(not_valid)
    assert issubclass(w[-1].category, UserWarning)
    assert warn_expected_string in str(w[-1].message)

    with warnings.catch_warnings(record=True) as w:
        not_valid = ["CelLs", "CytopLasM", "SOMETHING"]
        output = check_compartments(not_valid)
    assert issubclass(w[-1].category, UserWarning)
    assert warn_expected_string in str(w[-1].message)

    with warnings.catch_warnings(record=True) as w:
        not_valid = ["CelLs", "CytopLasM", "SOMETHING", "NOTHING"]
        output = check_compartments(not_valid)
    assert issubclass(w[-1].category, UserWarning)
    assert "{x}, nothing".format(x=warn_expected_string) in str(w[-1].message)


def test_get_default_compartments():
    default_comparments = get_default_compartments()
    assert ["cells", "cytoplasm", "nuclei"] == default_comparments


def test_load_known_metadata_dictionary():
    meta_cols = ["ObjectNumber", "ImageNumber", "TableNumber"]
    meta_df = pd.DataFrame(
        {
            "compartment": ["cells"] * 3 + ["nuclei"] * 3 + ["cytoplasm"] * 3,
            "feature": meta_cols * 3,
        }
    )

    metadata_file = os.path.join(tmpdir, "metadata_temp.txt")
    meta_df.to_csv(metadata_file, sep="\t", index=False)

    result = load_known_metadata_dictionary(metadata_file)

    expected_result = {
        "cells": meta_cols,
        "nuclei": meta_cols,
        "cytoplasm": meta_cols,
    }
    assert result == expected_result


def test_check_correlation_method():
    method = check_correlation_method(method="PeaRSon")
    expected_method = "pearson"

    assert method == expected_method

    with pytest.raises(AssertionError) as nomethod:
        method = check_correlation_method(method="DOES NOT EXIST")

    assert "not supported, select one of" in str(nomethod.value)


def test_check_aggregate_operation_method():
    operation = check_aggregate_operation(operation="MEAn")
    expected_op = "mean"

    assert operation == expected_op

    with pytest.raises(AssertionError) as nomethod:
        method = check_aggregate_operation(operation="DOES NOT EXIST")

    assert "not supported, select one of" in str(nomethod.value)


def test_check_consensus_operation_method():
    for test_operation in ["MeaN", "meDIAN", "modZ"]:
        operation = check_consensus_operation(operation=test_operation)
        expected_op = test_operation.lower()

        assert operation == expected_op

    with pytest.raises(AssertionError) as nomethod:
        method = check_consensus_operation(operation="DOES NOT EXIST")

    assert "not supported, select one of" in str(nomethod.value)


def test_get_pairwise_correlation():
    data_df = pd.concat(
        [
            pd.DataFrame({"x": [1, 3, 8], "y": [5, 3, 1]}),
            pd.DataFrame({"x": [1, 3, 5], "y": [8, 3, 1]}),
        ]
    ).reset_index(drop=True)

    cor_df, pair_df = get_pairwise_correlation(data_df, method="pearson")

    pd.testing.assert_frame_equal(cor_df, data_df.corr(method="pearson"))

    expected_result = -0.8
    x_y_cor = pair_df.query("correlation != 0").round(1).correlation.values[0]
    assert x_y_cor == expected_result


def test_check_fields_of_view():
    data_fields_of_view = [1, 3, 4, 5]

    valid_input_fields_of_view = [1, 3, 4]
    assert check_fields_of_view(data_fields_of_view, valid_input_fields_of_view) is None

    valid_input_fields_of_view = [5, 4, 1]
    assert check_fields_of_view(data_fields_of_view, valid_input_fields_of_view) is None

    valid_input_fields_of_view = [4, 3, 1, 5]
    assert check_fields_of_view(data_fields_of_view, valid_input_fields_of_view) is None

    invalid_input_fields_of_view = [2, 6, 7]
    with pytest.raises(ValueError) as err:
        check_fields_of_view(data_fields_of_view, invalid_input_fields_of_view)
        assert (
            str(err)
            == "Some of the input fields of view are not present in the image table."
        )


def test_check_fields_of_view_format():
    valid_input_fields_of_view = "all"
    assert (
        check_fields_of_view_format(valid_input_fields_of_view)
        == valid_input_fields_of_view
    )

    valid_input_fields_of_view = ["1", 3, 4]
    assert check_fields_of_view_format(valid_input_fields_of_view) == [1, 3, 4]

    valid_input_fields_of_view = ["3", "1", "5"]  # valid but not recommended
    assert check_fields_of_view_format(valid_input_fields_of_view) == [3, 1, 5]

    invalid_input_fields_of_view = 1
    with pytest.raises(TypeError) as err:
        check_fields_of_view_format(invalid_input_fields_of_view)
        assert (
            str(err)
            == f"Variable of type list expected, however type {type(invalid_input_fields_of_view)} was passed."
        )

    invalid_input_fields_of_view = ["test", 2, 3]
    with pytest.raises(TypeError) as err:
        check_fields_of_view_format(invalid_input_fields_of_view)
        assert (
            str(err)
            == "Variables of type int expected, however some of the input fields of view are not integers."
        )


def test_check_image_features():
    data_image_cols = [
        "Count_Cells",
        "Granularity_1_Mito",
        "Texture_Variance_RNA_20_00",
        "Texture_InfoMeas2_DNA_5_02",
    ]

    valid_image_feature_groups = ["Count", "Granularity"]
    assert check_image_features(valid_image_feature_groups, data_image_cols) is None

    valid_image_feature_groups = ["Count", "Granularity", "Texture"]
    assert check_image_features(valid_image_feature_groups, data_image_cols) is None

    invalid_image_feature_groups = ["Count", "IncorrectFeatureGroup"]
    with pytest.raises(ValueError) as err:
        check_image_features(invalid_image_feature_groups, data_image_cols)
        assert (
            str(err)
            == "Some of the input image features are not present in the image table."
        )


def test_extract_image_features():
    image_df = pd.DataFrame(
        {
            "TableNumber": ["x_hash", "y_hash"],
            "ImageNumber": ["x", "y"],
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A01"],
            "Count_Cells": [50, 50],
            "Granularity_1_Mito": [3.0, 4.0],
            "Texture_Variance_RNA_20_00": [12.0, 14.0],
            "Texture_InfoMeas2_DNA_5_02": [5.0, 1.0],
        }
    )

    image_feature_categories = ["count", "Granularity"]
    expected_image_feature_categories = ["Count", "Granularity"]

    expected_result = pd.DataFrame(
        {
            "TableNumber": ["x_hash", "y_hash"],
            "ImageNumber": ["x", "y"],
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A01"],
            "Metadata_Count_Cells": [50, 50],
            "Image_Granularity_1_Mito": [3.0, 4.0],
        }
    )

    result, corrected_image_feature_categories = extract_image_features(
        image_feature_categories,
        image_df,
        ["TableNumber", "ImageNumber"],
        ["Metadata_Plate", "Metadata_Well"],
    )

    pd.testing.assert_frame_equal(
        expected_result.sort_index(axis=1), result.sort_index(axis=1)
    )

    assert expected_image_feature_categories == corrected_image_feature_categories
