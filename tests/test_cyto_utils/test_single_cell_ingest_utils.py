import pytest
from pycytominer.cyto_utils import (
    get_default_linking_cols,
    get_default_compartments,
    assert_linking_cols_complete,
    provide_linking_cols_feature_name_update,
)

default_compartments = get_default_compartments()
default_linking_cols = {
    "cytoplasm": {
        "cells": "Cytoplasm_Parent_Cells",
        "nuclei": "Cytoplasm_Parent_Nuclei",
    },
    "cells": {"cytoplasm": "ObjectNumber"},
    "nuclei": {"cytoplasm": "ObjectNumber"},
}


def test_default_linking_cols():
    linking_cols = get_default_linking_cols()
    assert linking_cols == default_linking_cols


def test_assert_linking_cols_complete():
    assert_linking_cols_complete()
    assert_linking_cols_complete(
        linking_cols=default_linking_cols, compartments=default_compartments
    )

    with pytest.raises(AssertionError) as err:
        assert_linking_cols_complete(
            linking_cols=default_linking_cols, compartments=["cells", "cytoplasm"]
        )

    assert "nuclei compartment not found." in str(err.value)

    error_linking_cols = {
        "cytoplasm": {"cells": "Cytoplasm_Parent_Cells"},
        "cells": {"cytoplasm": "ObjectNumber"},
        "nuclei": {"cytoplasm": "ObjectNumber"},
    }
    with pytest.raises(AssertionError) as err:
        assert_linking_cols_complete(
            linking_cols=error_linking_cols, compartments=default_compartments
        )
    assert "Missing column identifier between cytoplasm-nuclei" in str(err.value)

    with pytest.raises(AssertionError) as err:
        assert_linking_cols_complete(
            linking_cols=default_linking_cols,
            compartments=["cells", "cytoplasm", "nuclei", "sandwich"],
        )
    assert (
        "All compartments must be specified in the linking_cols, {'sandwich'} is missing"
        in str(err.value)
    )


def test_provide_linking_cols_feature_name_update():
    expected_result = {
        "Cytoplasm_Parent_Cells": "Metadata_Cytoplasm_Parent_Cells",
        "Cytoplasm_Parent_Nuclei": "Metadata_Cytoplasm_Parent_Nuclei",
        "ObjectNumber": "Metadata_ObjectNumber",
    }

    result = provide_linking_cols_feature_name_update()
    assert result == expected_result

    new_linking_cols = get_default_linking_cols()
    new_linking_cols["cytoplasm"]["new"] = "Cytoplasm_Parent_New"
    new_linking_cols["new"] = {"cytoplasm": "ObjectNumber"}
    result = provide_linking_cols_feature_name_update(new_linking_cols)

    expected_result = {
        "Cytoplasm_Parent_Cells": "Metadata_Cytoplasm_Parent_Cells",
        "Cytoplasm_Parent_Nuclei": "Metadata_Cytoplasm_Parent_Nuclei",
        "Cytoplasm_Parent_New": "Metadata_Cytoplasm_Parent_New",
        "ObjectNumber": "Metadata_ObjectNumber",
    }

    assert result == expected_result
