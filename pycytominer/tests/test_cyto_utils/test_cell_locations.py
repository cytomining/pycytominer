"""This tests the output from CellLocation class"""

import os
import pandas as pd
from pycytominer.cyto_utils.cell_locations import CellLocation
import pytest
import sqlite3

# local files
example_project_dir = os.path.join(
    os.path.dirname(__file__), "..", "test_data", "cell_locations_example_data"
)

metadata_input = os.path.join(
    example_project_dir, "load_data_with_illum_subset.parquet"
)

single_cell_input = os.path.join(example_project_dir, "BR00126114_subset.sqlite")

# inputs are files
cell_loc_obj1 = CellLocation(
    metadata_input=metadata_input,
    single_cell_input=single_cell_input,
)

# inputs are in-memory objects
cell_loc_obj2 = CellLocation(
    metadata_input=pd.read_parquet(metadata_input),
    single_cell_input=sqlite3.connect(single_cell_input),
)

# inputs are S3 paths

# don't run this test if running on GitHub Actions
# because the S3 bucket is not public

if "GITHUB_WORKFLOW" in os.environ:
    pytest.skip("Skipping S3 test", allow_module_level=True)


example_s3_project_dir = "s3://cellpainting-gallery/test-cpg0016-jump/source_4/workspace/{workspace_folder}/2021_08_23_Batch12/BR00126114/"

metadata_input_s3 = os.path.join(
    example_s3_project_dir.format(workspace_folder="load_data_csv"),
    "load_data_with_illum_subset.parquet",
)

single_cell_input_s3 = os.path.join(example_project_dir, "BR00126114_subset.sqlite")

cell_loc_obj3 = CellLocation(
    metadata_input=metadata_input_s3,
    single_cell_input=single_cell_input_s3,
)

cell_loc3 = cell_loc_obj3.add_cell_location()


# load the data
cell_loc1 = cell_loc_obj1.add_cell_location()

cell_loc2 = cell_loc_obj2.add_cell_location()

cell_loc_l = [cell_loc1, cell_loc2]


@pytest.mark.parametrize("cell_loc", cell_loc_l)
def test_shape_and_columns(cell_loc):
    # check the shape of the data
    assert cell_loc.shape == (2, 28)

    # verify that the Nuclear_Location_Center_X and Nuclear_Location_Center_Y columns are present
    assert "Nuclei_Location_Center_X" in cell_loc.columns
    assert "Nuclei_Location_Center_Y" in cell_loc.columns


@pytest.mark.parametrize("cell_loc", cell_loc_l)
def test_shape_and_columns(cell_loc):
    # check the shape of the data
    assert cell_loc.shape == (2, 28)

    # verify that the Nuclear_Location_Center_X and Nuclear_Location_Center_Y columns are present
    assert "Nuclei_Location_Center_X" in cell_loc.columns
    assert "Nuclei_Location_Center_Y" in cell_loc.columns


@pytest.mark.parametrize("cell_loc", cell_loc_l)
def test_values(cell_loc):
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
