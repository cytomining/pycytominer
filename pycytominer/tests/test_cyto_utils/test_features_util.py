import os
import random
import pytest
import pandas as pd
from pycytominer.cyto_utils.features import convert_compartment_format_to_list


def test_convert_compartment_format_to_list():
    compartments = convert_compartment_format_to_list(["cells", "CYTOplasm", "nuclei"])
    assert compartments == ["cells", "cytoplasm", "nuclei"]

    compartments = convert_compartment_format_to_list("FoO")
    assert compartments == ["foo"]
