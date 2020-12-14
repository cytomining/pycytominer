import os
import random
import pytest
import tempfile
import pandas as pd
from pycytominer.cyto_utils.util import (
    check_compartments,
    get_default_compartments,
    load_known_metadata_dictionary,
    get_pairwise_correlation,
    check_correlation_method,
    check_aggregate_operation,
    check_consensus_operation,
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
    with pytest.raises(AssertionError) as ae:
        not_valid = ["SOMETHING"]
        output = check_compartments(not_valid)
    assert "compartment not supported" in str(ae.value)

    with pytest.raises(AssertionError) as ae:
        not_valid = "SOMETHING"
        output = check_compartments(not_valid)
    assert "compartment not supported" in str(ae.value)

    with pytest.raises(AssertionError) as ae:
        not_valid = ["Cells", "Cytoplasm", "SOMETHING"]
        output = check_compartments(not_valid)
    assert "compartment not supported" in str(ae.value)


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
