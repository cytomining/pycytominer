import os
import random
import pytest
import tempfile
import pandas as pd
from pycytominer.cyto_utils.util import (
    check_compartments,
    load_known_metadata_dictionary,
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
