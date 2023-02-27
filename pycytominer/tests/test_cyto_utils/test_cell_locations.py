"""This tests the output from CellLocation class"""

import os
import pandas as pd
from pycytominer.cyto_utils.cell_locations import CellLocation
import pytest


@pytest.mark.parametrize("cell_loc", ["cell_loc1", "cell_loc2", "cell_loc3"])
def test_shape_and_columns(cell_loc, request):
    cell_loc = request.getfixturevalue(cell_loc)

    # check the shape of the data
    assert cell_loc.shape == (2, 28)

    # verify that the Nuclear_Location_Center_X and Nuclear_Location_Center_Y columns are present
    assert "Nuclei_Location_Center_X" in cell_loc.columns
    assert "Nuclei_Location_Center_Y" in cell_loc.columns


@pytest.mark.parametrize("cell_loc", ["cell_loc1", "cell_loc2", "cell_loc3"])
def test_shape_and_columns(cell_loc, request):
    cell_loc = request.getfixturevalue(cell_loc)

    # check the shape of the data
    assert cell_loc.shape == (2, 28)

    # verify that the Nuclear_Location_Center_X and Nuclear_Location_Center_Y columns are present
    assert "Nuclei_Location_Center_X" in cell_loc.columns
    assert "Nuclei_Location_Center_Y" in cell_loc.columns


@pytest.mark.parametrize("cell_loc", ["cell_loc1", "cell_loc2", "cell_loc3"])
def test_values(cell_loc, request):
    cell_loc = request.getfixturevalue(cell_loc)

    # verify that the values in the Nuclear_Location_Center_X and Nuclear_Location_Center_Y columns are correct
    assert cell_loc["Nuclei_Location_Center_X"].values[0] == [
        943.512129380054,
        65.5980176211454,
        790.798319327731,
        798.1744,
        657.246344206974,
        778.97604035309,
        322.763649425287,
        718.11819235226,
        109.785065590313,
        325.727799227799,
    ]

    assert cell_loc["Nuclei_Location_Center_Y"].values[0] == [
        182.789757412399,
        294.24449339207,
        338.886554621849,
        387.1376,
        402.2272215973,
        406.378310214376,
        413.334051724138,
        469.506373117034,
        474.240161453078,
        497.608108108108,
    ]
