import os
import random
import pytest
import pandas as pd
from pycytominer.cyto_utils.util import check_compartments


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
