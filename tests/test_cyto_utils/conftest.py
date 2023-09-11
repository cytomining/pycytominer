"""
conftest.py for pytest
"""

import os
import pandas as pd
import pathlib
import pytest
import sqlalchemy
from pycytominer.cyto_utils.cell_locations import CellLocation


@pytest.fixture(name="data_dir_cell_locations")
def fixture_data_dir_cell_locations() -> str:
    """
    Provide a data directory for cell_locations test data
    """

    return (
        f"{pathlib.Path(__file__).parent.parent}/test_data/cell_locations_example_data"
    )


@pytest.fixture(name="metadata_input_file")
def fixture_metadata_input_file(data_dir_cell_locations: str) -> str:
    """
    Provide a metadata input file for cell_locations test data
    """
    return os.path.join(
        data_dir_cell_locations, "test_BR00126114_load_data_with_illum.parquet"
    )


@pytest.fixture(name="single_cell_input_file")
def fixture_single_cell_input_file(data_dir_cell_locations: str) -> str:
    """
    Provide a single cell input file for cell_locations test data
    """
    return os.path.join(data_dir_cell_locations, "test_BR00126114.sqlite")


@pytest.fixture(name="metadata_input_file_s3")
def fixture_metadata_input_file_s3() -> str:
    """
    Provide a metadata input file for cell_locations test data
    """
    return "s3://cellpainting-gallery/test-cpg0016-jump/source_4/workspace/load_data_csv/2021_08_23_Batch12/BR00126114/test_BR00126114_load_data_with_illum.parquet"


@pytest.fixture(name="single_cell_input_file_s3")
def fixture_single_cell_input_file_s3() -> str:
    """
    Provide a single cell input file for cell_locations test data
    """
    return "s3://cellpainting-gallery/test-cpg0016-jump/source_4/workspace/backend/2021_08_23_Batch12/BR00126114/test_BR00126114.sqlite"


@pytest.fixture(name="metadata_input_dataframe")
def fixture_metadata_input_dataframe(metadata_input_file: str) -> pd.DataFrame:
    """
    Provide a metadata input file for cell_locations test data
    """
    return pd.read_parquet(metadata_input_file)


@pytest.fixture(name="single_cell_input_engine")
def fixture_single_cell_input_engine(
    single_cell_input_file: str,
) -> sqlalchemy.engine.Engine:
    """
    Provide a single cell input file for cell_locations test data
    """
    return sqlalchemy.create_engine(f"sqlite:///{single_cell_input_file}")


@pytest.fixture(name="cell_loc_obj1")
def fixture_cell_loc_obj1(
    metadata_input_file: str,
    single_cell_input_file: str,
) -> CellLocation:
    """
    Provide a CellLocation object with file inputs
    """
    return CellLocation(
        metadata_input=metadata_input_file,
        single_cell_input=single_cell_input_file,
    )


@pytest.fixture(name="cell_loc_obj2")
def fixture_cell_loc_obj2(
    metadata_input_dataframe: pd.DataFrame,
    single_cell_input_engine: sqlalchemy.engine.Engine,
) -> CellLocation:
    """
    Provide a CellLocation object with in-memory inputs
    """
    return CellLocation(
        metadata_input=metadata_input_dataframe,
        single_cell_input=single_cell_input_engine,
    )


@pytest.fixture(name="cell_loc_obj3")
def fixture_cell_loc_obj3(
    metadata_input_file_s3: str,
    single_cell_input_file_s3: str,
) -> CellLocation:
    """
    Provide a CellLocation object with s3 inputs
    """
    return CellLocation(
        metadata_input=metadata_input_file_s3,
        single_cell_input=single_cell_input_file_s3,
    )


@pytest.fixture(name="cell_loc1")
def fixture_cell_loc1(cell_loc_obj1: CellLocation) -> pd.DataFrame:
    """
    Provide the output of running CellLocation.add_cell_location
    """
    return cell_loc_obj1.add_cell_location()


@pytest.fixture(name="cell_loc2")
def fixture_cell_loc2(cell_loc_obj2: CellLocation) -> pd.DataFrame:
    """
    Provide the output of running CellLocation.add_cell_location
    """
    return cell_loc_obj2.add_cell_location()


@pytest.fixture(name="cell_loc3")
def fixture_cell_loc3(cell_loc_obj3: CellLocation) -> pd.DataFrame:
    """
    Provide the output of running CellLocation.add_cell_location
    """
    return cell_loc_obj3.add_cell_location()
