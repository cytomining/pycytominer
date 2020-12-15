from collections import Counter
from pycytominer.cyto_utils import check_compartments, get_default_compartments


def get_default_linking_cols():
    """Define the standard experiment linking columns between tables

    :return: Dictionary of compartment-specific column names used to link compartments across tables
    :rtype: dict

    .. note::
        every dictionary pair has a 1 to 1 correspondence (e.g. cytoplasm-cells and cells-cytoplasm both must exist)
    """
    linking_cols = {
        "cytoplasm": {
            "cells": "Cytoplasm_Parent_Cells",
            "nuclei": "Cytoplasm_Parent_Nuclei",
        },
        "cells": {"cytoplasm": "ObjectNumber"},
        "nuclei": {"cytoplasm": "ObjectNumber"},
    }

    return linking_cols


def assert_linking_cols_complete(linking_cols="default", compartments="default"):
    """Confirm that the linking cols and compartments are compatible

    :return: Dictionary of compartment-specific column names used to link compartments across tables
    :rtype: dict

    .. note::
        assert_linking_cols_complete() does not check if columns are present
    """
    if linking_cols == "default":
        linking_cols = get_default_linking_cols()

    if compartments == "default":
        compartments = get_default_compartments()

    comp_err = "compartment not found. Check the specified compartments"

    linking_check = []
    unique_linking_cols = []
    for x in linking_cols:
        unique_linking_cols.append(x)
        assert x in compartments, "{com} {err}".format(com=x, err=comp_err)
        for y in linking_cols[x]:
            unique_linking_cols.append(y)
            assert y in compartments, "{com} {err}".format(com=y, err=comp_err)
            linking_check.append("-".join(sorted([x, y])))

    # Make sure that each combination has been specified exactly twice
    linking_counter = Counter(linking_check)
    for combo in linking_counter:
        assert (
            linking_counter[combo] == 2
        ), "Missing column identifier between {combo}".format(combo=combo)

    # Confirm that every compartment has been specified in the linking_cols
    unique_linking_cols = sorted(list(set(unique_linking_cols)))
    diff_column = set(compartments).difference(unique_linking_cols)
    assert unique_linking_cols == sorted(
        compartments
    ), "All compartments must be specified in the linking_cols, {miss} is missing".format(
        miss=diff_column
    )
