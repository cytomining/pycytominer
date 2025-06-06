"""This tests the output from CellLocation class"""

import pandas as pd
import pytest
from _pytest.fixtures import FixtureRequest

from pycytominer.cyto_utils.cell_locations import CellLocation


def get_metadata_input_dataframe(cell_loc: CellLocation) -> pd.DataFrame:
    """
    Gathers the metadata input dataframe given various conditions
    from a CellLocation object.
    """

    # return a dataframe if it is already a dataframe
    if isinstance(cell_loc.metadata_input, pd.DataFrame):
        return cell_loc.metadata_input

    # try to process a string-based path
    if isinstance(cell_loc.metadata_input, str):
        storage_opts = (
            {"anon": True} if cell_loc.metadata_input.startswith("s3://") else None
        )
        return (
            # read from parquet if we have a parquet object path
            pd.read_parquet(
                path=cell_loc.metadata_input,
                # set storage options if we have an s3 path
                storage_options=storage_opts,
            )
            if cell_loc.metadata_input.endswith(".parquet")
            # read from csv if we have a csv object path
            else (
                pd.read_csv(
                    filepath_or_buffer=cell_loc.metadata_input,
                    # set storage options if we have an s3 path
                    storage_options=storage_opts,
                )
            )
        )
    else:
        # otherwise raise an error as we don't have a supported format
        raise ValueError("Unsupported metadata_input type")


@pytest.mark.large_data_tests
@pytest.mark.parametrize(
    "cell_loc_param",
    [
        "cell_loc_obj1",
        "cell_loc_obj2",
        "cell_loc_obj3",
    ],
)
def test_output_shape_and_required_columns(
    cell_loc_param: list[str],
    request: type[FixtureRequest],
):
    """
    This tests the shape of the output from CellLocation class and verifies that the required columns are present
    """

    cls_cell_loc = request.getfixturevalue(cell_loc_param)
    cell_loc = cls_cell_loc.add_cell_location()
    metadata_input_dataframe = get_metadata_input_dataframe(cell_loc=cls_cell_loc)

    # check the shape of the data
    # cell_loc will have 3 extra columns: TableNumber, ImageNumber, CellCenters
    assert cell_loc.shape == (
        metadata_input_dataframe.shape[0],
        metadata_input_dataframe.shape[1] + 3,
    )

    assert isinstance(cell_loc["CellCenters"][0][0], dict)
    # verify that the Nuclear_Location_Center_X and Nuclear_Location_Center_Y dictionary keys are present
    assert "Nuclei_Location_Center_X" in cell_loc["CellCenters"][0][0]
    assert "Nuclei_Location_Center_Y" in cell_loc["CellCenters"][0][0]


@pytest.mark.large_data_tests
@pytest.mark.parametrize(
    "cell_loc_param",
    [
        "cell_loc_obj1",
        "cell_loc_obj2",
        "cell_loc_obj3",
    ],
)
def test_output_value_correctness(
    cell_loc_param: list[str],
    request: type[FixtureRequest],
):
    """
    This tests the correctness of the values in the output from CellLocation class by comparing the values in the output to the values in the input
    """

    cls_cell_loc = request.getfixturevalue(cell_loc_param)
    cell_loc = cls_cell_loc.add_cell_location()
    metadata_input_dataframe = get_metadata_input_dataframe(cell_loc=cls_cell_loc)

    # Cast cell_loc columns to the data types of metadata_input_dataframe columns
    # (observed metadata_site as having different types)
    for col in metadata_input_dataframe.columns:
        cell_loc[col] = cell_loc[col].astype(metadata_input_dataframe[col].dtype)

    # if we restrict the columns of cell_loc to the ones in metadata_input_dataframe, we should get the same dataframe
    pd.testing.assert_frame_equal(
        cell_loc[metadata_input_dataframe.columns].reset_index(drop=True),
        metadata_input_dataframe.reset_index(drop=True),
    )

    # gather an engine from the cell_loc class
    _, engine = cls_cell_loc._get_single_cell_engine()

    nuclei_query = "SELECT TableNumber, ImageNumber, ObjectNumber, Nuclei_Location_Center_X, Nuclei_Location_Center_Y FROM Nuclei;"

    nuclei_df = pd.read_sql_query(nuclei_query, engine)

    # get the values in the Nuclear_Location_Center_X and Nuclear_Location_Center_Y columns
    # for the rows in nuclei_df that have ImageNumber == 1

    # note: we cast to "int64" type to ensure all cell_loc_obj's are treated the same
    # (some include ImageNumber's of type obj, others are int64)
    nuclei_df_row1 = nuclei_df[nuclei_df["ImageNumber"].astype("int64") == 1]

    observed_x = [x["Nuclei_Location_Center_X"] for x in cell_loc.CellCenters[0]]
    observed_y = [x["Nuclei_Location_Center_Y"] for x in cell_loc.CellCenters[0]]

    expected_x = nuclei_df_row1["Nuclei_Location_Center_X"].tolist()
    expected_x = [float(x) for x in expected_x]

    expected_y = nuclei_df_row1["Nuclei_Location_Center_Y"].tolist()
    expected_y = [float(x) for x in expected_y]

    # verify that the values in the Nuclear_Location_Center_X and Nuclear_Location_Center_Y columns are correct
    assert observed_x == expected_x
    assert observed_y == expected_y
