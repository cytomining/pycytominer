from pycytominer.cyto_utils.features import convert_compartment_format_to_list


def test_convert_compartment_format_to_list():
    compartments = convert_compartment_format_to_list([
        "cells",
        "CYTOplasm",
        "nuclei",
        "MyoD",
    ])
    assert compartments == ["cells", "CYTOplasm", "nuclei", "MyoD"]

    compartments = convert_compartment_format_to_list("FoO")
    assert compartments == ["FoO"]
