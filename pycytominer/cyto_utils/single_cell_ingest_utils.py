from collections import Counter
from pycytominer.cyto_utils import check_compartments, get_default_compartments


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

def get_non_default_linking_cols(compartments):
    """Define the standard experiment linking columns between tables for object-object combination of compartments or single compartment:
    
    nuclei-cells, nuclei-cytoplasm, cells-cytoplasm, or only one of the three compartments

    Returns
    -------
    linking_cols, dict
        A dictionary mapping columns that links together canonical CellProfiler objects but in different combinations or for a single object

    .. note::
        every dictionary pair has a 1 to 1 correspondence if two objects are given. If only one object is given, only Object_Number column defines the linking.
    """
    compartment_linking_cols_cells_nuclei={ 
    "cells": {
        "nuclei": "Cells_Parent_Nuclei"
    },
        "nuclei": {"cells": "ObjectNumber"},
    }
    compartment_linking_cols_cyto_nuclei={ 
        "cytoplasm": {
            "nuclei": "Cytoplasm_Parent_Nuclei"
        },
        "nuclei": {"cytoplasm": "ObjectNumber"},
        }
    compartment_linking_cols_cyto_cells={
        "cytoplasm": {
            "cells": "Cytoplasm_Parent_Cells"
        },
        "cells": {"cytoplasm": "ObjectNumber"},
        }
    
    linking_cols = dict()
    if len(compartments) == 2:
        if "cells" and "nuclei" in compartments:
            linking_cols = compartment_linking_cols_cells_nuclei
        elif "cytoplasm" and "nuclei" in compartments:
            linking_cols = compartment_linking_cols_cyto_nuclei
        elif "cytoplasm" and "cells" in compartments:
            linking_cols = compartment_linking_cols_cyto_cells
    elif len(compartments) == 1:
        linking_cols[compartments[0]] = {compartments[0]:"ObjectNumber"}

    return linking_cols


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

    linking_check = []
    unique_linking_cols = []
    for x in linking_cols:
        unique_linking_cols.append(x)
        assert x in compartments, "{com} {err}".format(com=x, err=comp_err)
        for y in linking_cols[x]:
            unique_linking_cols.append(y)
            assert y in compartments, "{com} {err}".format(com=y, err=comp_err)
            linking_check.append("-".join(sorted([x, y])))
   
    # Make sure that each combination has been specified exactly twice. Don't check if only one object is given
    if not len(compartments) == 1:
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
            ["Metadata_{x}".format(x=y) for y in metadata_update_cols],
        )
    )
    return update_name
