from collections import Counter

from pycytominer.cyto_utils.util import get_default_compartments


def get_default_linking_cols():
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


def get_alternative_linking_cols():
    """
    Returns a dictionary defining alternative linking columns between compartments
    for single cell morphological profiles.

    The returned dictionary specifies, for each compartment, the linking columns
    used to connect to other compartments (typically as used in CellProfiler output).

    Returns
    -------
    dict
        Dictionary with compartment names as keys and sub-dictionaries indicating
        the linking columns to other compartments.
        Example:
            {
                "cells": {
                    "cytoplasm": "ObjectNumber",
                    "nuclei": "Cells_Parent_Nuclei",
                },
                ...
            }
    """
    alternative_linking_cols = {
        "cells": {
            "cytoplasm": "ObjectNumber",
            "nuclei": "Cells_Parent_Nuclei",
        },
        "cytoplasm": {
            "cells": "Cytoplasm_Parent_Cells",
            "nuclei": "Cytoplasm_Parent_Nuclei",
        },
        "nuclei": {
            "cytoplasm": "ObjectNumber",
            "cells": "ObjectNumber",
        },
    }
    return alternative_linking_cols


def get_linking_cols_from_compartments(compartments):
    """
    Given a list of compartments, returns an appropriate linking columns dictionary.

    For single compartment, links to itself via "ObjectNumber".
    For two compartments, uses alternative linking columns. Only if they belong to default_compartments.
    For three or more compartments, falls back to the default linking cols.

    Parameters
    ----------
    compartments : list of str
        List of compartment names to link (e.g., ["cells"], ["cells", "nuclei"], etc.)

    Returns
    -------
    dict
        Dictionary mapping compartments to their linking columns.
    """
    if len(compartments) == 1:
        return {}
    elif len(compartments) == 2:
        alt = get_alternative_linking_cols()
        linking_cols = {}
        for comp in compartments:
            subdict = {k: v for k, v in alt[comp].items() if k in compartments}
            if subdict:
                linking_cols[comp] = subdict
        return linking_cols
    else:
        return get_default_linking_cols()


def assert_linking_cols_complete(linking_cols="default", compartments="default"):
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

    if not len(compartments) == 1:
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


def provide_linking_cols_feature_name_update(linking_cols="default"):
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
