"""This tests the output from CellLocation class"""

import pandas as pd
import pytest
import sqlalchemy
from typing import Type
from _pytest.fixtures import FixtureRequest


@pytest.mark.parametrize("cell_loc", ["cell_loc1", "cell_loc2", "cell_loc3"])
def test_output_shape_and_required_columns(
    cell_loc: str,
    metadata_input_dataframe: pd.DataFrame,
    request: Type[FixtureRequest],
):
    """
    This tests the shape of the output from CellLocation class and verifies that the required columns are present
    """

    cell_loc = request.getfixturevalue(cell_loc)

    # check the shape of the data
    assert cell_loc.shape == (
        metadata_input_dataframe.shape[0],
        metadata_input_dataframe.shape[1] + 2,
    )

    # verify that the Nuclear_Location_Center_X and Nuclear_Location_Center_Y columns are present
    assert "Nuclei_Location_Center_X" in cell_loc["CellCenters"][0][0].keys()
    assert "Nuclei_Location_Center_Y" in cell_loc["CellCenters"][0][0].keys()


@pytest.mark.parametrize("cell_loc", ["cell_loc1", "cell_loc2", "cell_loc3"])
def test_output_value_correctness(
    cell_loc: str,
    metadata_input_dataframe: pd.DataFrame,
    single_cell_input_file: str,
    request: Type[FixtureRequest],
):
    """
    This tests the correctness of the values in the output from CellLocation class by comparing the values in the output to the values in the input
    """
    cell_loc = request.getfixturevalue(cell_loc)

    # if we restrict the columns of cell_loc to the ones in metadata_input_dataframe, we should get the same dataframe
    assert (
        cell_loc[metadata_input_dataframe.columns]
        .reset_index(drop=True)
        .equals(metadata_input_dataframe.reset_index(drop=True))
    )

    engine = sqlalchemy.create_engine(f"sqlite:///{single_cell_input_file}")

    nuclei_query = "SELECT ImageNumber, ObjectNumber, Nuclei_Location_Center_X, Nuclei_Location_Center_Y FROM Nuclei;"

    nuclei_df = pd.read_sql_query(nuclei_query, engine)

    # get the values in the Nuclear_Location_Center_X and Nuclear_Location_Center_Y columns
    # for the rows in nuclei_df that have ImageNumber == 1

    nuclei_df_row1 = nuclei_df[nuclei_df["ImageNumber"] == "1"]

    observed_x = [x["Nuclei_Location_Center_X"] for x in cell_loc.CellCenters[0]]
    observed_y = [x["Nuclei_Location_Center_Y"] for x in cell_loc.CellCenters[0]]

    expected_x = nuclei_df_row1["Nuclei_Location_Center_X"].tolist()
    expected_x = [float(x) for x in expected_x]

    expected_y = nuclei_df_row1["Nuclei_Location_Center_Y"].tolist()
    expected_y = [float(x) for x in expected_y]

    # verify that the values in the Nuclear_Location_Center_X and Nuclear_Location_Center_Y columns are correct
    assert observed_x == expected_x
    assert observed_y == expected_y
