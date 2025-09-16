"""
Modules for handling CellProfiler single-cell data ingestion
"""

from collections import Counter
from typing import Literal, Union

from pycytominer.cyto_utils.util import get_default_compartments


def get_default_linking_cols() -> dict[str, dict[str, str]]:
    """Define the standard experiment linking columns between tables

    Returns
    -------
    linking_cols, dict
        A dictionary mapping columns that links together CellProfiler objects

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


def assert_linking_cols_complete(
    linking_cols: Union[Literal["default"], dict[str, dict[str, str]]] = "default",
    compartments: Union[Literal["default"], list[str]] = "default",
):
    """Confirm that the linking cols and compartments are compatible

    Parameters
    ----------
    linking_cols : str or dict, default "default"
        Specify how to link objects
    compartments : str or list, default "default"
        Which compartments used in the experiment.

    Returns
    -------
    None
        Asserts linking columns are appropriately defined

    .. note::
        assert_linking_cols_complete() does not check if columns are present
    """
    if linking_cols == "default":
        linking_cols = get_default_linking_cols()

    if compartments == "default":
        compartments = get_default_compartments()

    comp_err = "compartment not found. Check the specified compartments"

    # check that we have the right types
    if not isinstance(linking_cols, dict):
        raise ValueError("linking_cols must be a dictionary or 'default'")
    if not isinstance(compartments, list):
        raise ValueError("compartments must be a list or 'default'")

    linking_check = []
    unique_linking_cols = []
    for x in linking_cols:
        unique_linking_cols.append(x)
        if x not in compartments:
            raise ValueError(f"{x} {comp_err}")
        for y in linking_cols[x]:
            unique_linking_cols.append(y)
            if y not in compartments:
                raise ValueError(f"{y} {comp_err}")
            linking_check.append("-".join(sorted([x, y])))

    # Make sure that each combination has been specified exactly twice
    linking_counter = Counter(linking_check)
    for combo in linking_counter:
        if not linking_counter[combo] == 2:
            raise ValueError(f"Missing column identifier between {combo}")

    # Confirm that every compartment has been specified in the linking_cols
    unique_linking_cols = sorted(set(unique_linking_cols))
    diff_column = set(compartments).difference(unique_linking_cols)

    if not unique_linking_cols == sorted(compartments):
        raise ValueError(
            f"All compartments must be specified in the linking_cols, {diff_column} is missing"
        )


def provide_linking_cols_feature_name_update(
    linking_cols: Union[Literal["default"], dict[str, dict[str, str]]] = "default",
):
    """Output a dictionary to use to update pandas dataframe column names. The linking
    cols must be Metadata.

    Parameters
    ----------
    linking_cols : str or dict, default "default"
        Specify how to link objects

    Returns
    -------
    update_name, dict
        Dictionary of the linking column names to update after they are used
    """
    if linking_cols == "default":
        linking_cols = get_default_linking_cols()

    metadata_update_cols = []
    for col in linking_cols:
        for right_col in linking_cols[col]:
            metadata_update_cols.append(linking_cols[col][right_col])

    update_name = dict(
        zip(
            metadata_update_cols,
            [f"Metadata_{y}" for y in metadata_update_cols],
        )
    )
    return update_name
