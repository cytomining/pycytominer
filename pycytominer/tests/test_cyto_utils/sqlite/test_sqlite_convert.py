""" 
Tests for: pycytominer.cyto_utils.sqlite.convert
"""

import os
import tempfile

import numpy as np
import pandas as pd
from prefect.executors import LocalExecutor
from prefect.storage import Local
from pycytominer.cyto_utils.sqlite.convert import (
    flow_convert_sqlite_to_parquet,
    multi_to_single_parquet,
    nan_data_fill,
    sql_table_to_pd_dataframe,
    table_concat_to_parquet,
    to_unique_parquet,
)


def test_sql_select_distinct_join_chunks(database_distinct_join_chunks):
    """
    Testing sql_select_distinct_join_chunks
    """
    assert database_distinct_join_chunks == [
        [{"TableNumber": 1, "ImageNumber": 1}],
        [{"TableNumber": 2, "ImageNumber": 2}],
    ]


def test_sql_table_to_pd_dataframe(
    database_engine_for_conversion_testing,
    database_distinct_join_chunks,
    database_column_data,
):
    """
    Testing sql_table_to_pd_dataframe
    """

    # test image demo table
    df = sql_table_to_pd_dataframe.run(
        sql_engine=database_engine_for_conversion_testing,
        table_name="Image",
        prepend_tablename_to_cols=True,
        avoid_prepend_for=["TableNumber", "ImageNumber"],
        chunk_list_dicts=database_distinct_join_chunks[0],
        column_data=database_column_data,
    )

    assert df.to_dict(orient="dict") == {
        "TableNumber": {0: 1},
        "ImageNumber": {0: 1},
        "Image_ImageData": {0: 1},
        "Image_RandomDate": {0: "123-123"},
    }

    # test cytoplasm demo table
    df = sql_table_to_pd_dataframe.run(
        sql_engine=database_engine_for_conversion_testing,
        table_name="Cytoplasm",
        prepend_tablename_to_cols=True,
        avoid_prepend_for=["TableNumber", "ImageNumber"],
        chunk_list_dicts=database_distinct_join_chunks[0],
        column_data=database_column_data,
    )
    assert df.to_dict(orient="dict") == {
        "TableNumber": {0: 1},
        "ImageNumber": {0: 1},
        "Cytoplasm_ObjectNumber": {0: 6},
        "Cytoplasm_Cytoplasm_Parent_Cells": {0: 2},
        "Cytoplasm_Cytoplasm_Parent_Nuclei": {0: 4},
        "Cytoplasm_CytoplasmData": {0: 1},
    }


def test_nan_data_fill():
    """
    Testing nan_data_fill
    """

    image_df = pd.DataFrame(
        data={
            "TableNumber": {0: 1},
            "ImageNumber": {0: 1},
            "Image_ImageData": {0: 1},
            "Image_RandomDate": {0: "123-123"},
        }
    )
    cyto_df = pd.DataFrame(
        data={
            "TableNumber": {0: 1},
            "ImageNumber": {0: 1},
            "Cytoplasm_ObjectNumber": {0: 6},
            "Cytoplasm_Cytoplasm_Parent_Cells": {0: 2},
            "Cytoplasm_Cytoplasm_Parent_Nuclei": {0: 4},
            "Cytoplasm_CytoplasmData": {0: 1},
        }
    )

    # testing filling into image_df from cyto_df
    pd.testing.assert_frame_equal(
        left=nan_data_fill.run(fill_into=image_df, fill_from=cyto_df),
        right=pd.DataFrame(
            data={
                "TableNumber": {0: 1},
                "ImageNumber": {0: 1},
                "Image_ImageData": {0: 1},
                "Image_RandomDate": {0: "123-123"},
                "Cytoplasm_ObjectNumber": {0: np.nan},
                "Cytoplasm_Cytoplasm_Parent_Cells": {0: np.nan},
                "Cytoplasm_Cytoplasm_Parent_Nuclei": {0: np.nan},
                "Cytoplasm_CytoplasmData": {0: np.nan},
            }
        ),
    )

    # testing filling into cyto_df from image_df
    pd.testing.assert_frame_equal(
        left=nan_data_fill.run(fill_into=cyto_df, fill_from=image_df),
        right=pd.DataFrame(
            data={
                "TableNumber": {0: 1},
                "ImageNumber": {0: 1},
                "Cytoplasm_ObjectNumber": {0: 6},
                "Cytoplasm_Cytoplasm_Parent_Cells": {0: 2},
                "Cytoplasm_Cytoplasm_Parent_Nuclei": {0: 4},
                "Cytoplasm_CytoplasmData": {0: 1},
                "Image_ImageData": {0: np.nan},
                "Image_RandomDate": {0: None},
            }
        ),
    )


def test_table_concat_to_parquet(
    database_engine_for_conversion_testing,
    database_distinct_join_chunks,
    database_column_data,
):

    # parquet column order isn't guaranteed
    # so we store that here for comparisons below.
    _test_cols_order = [
        "TableNumber",
        "ImageNumber",
        "Image_ImageData",
        "Image_RandomDate",
        "Cells_ObjectNumber",
        "Cells_CellsData",
        "Nuclei_ObjectNumber",
        "Nuclei_NucleiData",
        "Cytoplasm_ObjectNumber",
        "Cytoplasm_Cytoplasm_Parent_Cells",
        "Cytoplasm_Cytoplasm_Parent_Nuclei",
        "Cytoplasm_CytoplasmData",
    ]
    # build a temporary dir for landing the chunked parquet file
    with tempfile.TemporaryDirectory() as temp_dir:
        chunk_file_path = table_concat_to_parquet.run(
            sql_engine=database_engine_for_conversion_testing,
            column_data=database_column_data,
            prepend_tablename_to_cols=True,
            avoid_prepend_for=["TableNumber", "ImageNumber"],
            chunk_list_dicts=database_distinct_join_chunks[0],
            filename=f"{temp_dir}/sqlite_convert_chunk_test",
        )
        # compare two dictionaries of data based on
        # parquet chunk output
        pd.testing.assert_frame_equal(
            # dataframe as dictionary data for assertion
            pd.read_parquet(chunk_file_path)[_test_cols_order]
            .sort_values(by=_test_cols_order)
            .reset_index(drop=True)
            .replace(np.nan, None),
            # data should look like this
            pd.DataFrame(
                data={
                    "Cells_CellsData": {0: np.nan, 1: 1.0, 2: np.nan, 3: np.nan},
                    "Cells_ObjectNumber": {0: np.nan, 1: 2.0, 2: np.nan, 3: np.nan},
                    "Cytoplasm_CytoplasmData": {
                        0: np.nan,
                        1: np.nan,
                        2: np.nan,
                        3: 1.0,
                    },
                    "Cytoplasm_Cytoplasm_Parent_Cells": {
                        0: np.nan,
                        1: np.nan,
                        2: np.nan,
                        3: 2.0,
                    },
                    "Cytoplasm_Cytoplasm_Parent_Nuclei": {
                        0: np.nan,
                        1: np.nan,
                        2: np.nan,
                        3: 4.0,
                    },
                    "Cytoplasm_ObjectNumber": {0: np.nan, 1: np.nan, 2: np.nan, 3: 6.0},
                    "ImageNumber": {0: 1, 1: 1, 2: 1, 3: 1},
                    "Image_ImageData": {0: 1.0, 1: np.nan, 2: np.nan, 3: np.nan},
                    "Image_RandomDate": {0: "123-123", 1: None, 2: None, 3: None},
                    "Nuclei_NucleiData": {0: np.nan, 1: np.nan, 2: 1.0, 3: np.nan},
                    "Nuclei_ObjectNumber": {0: np.nan, 1: np.nan, 2: 4.0, 3: np.nan},
                    "TableNumber": {0: 1, 1: 1, 2: 1, 3: 1},
                }
            )[_test_cols_order]
            .sort_values(by=_test_cols_order)
            .reset_index(drop=True)
            .replace(np.nan, None),
        )


def test_to_unique_parquet():
    """
    Testing to_unique_parquet
    """
    df = pd.DataFrame(
        data={
            "TableNumber": {0: 1},
            "ImageNumber": {0: 1},
            "Image_ImageData": {0: 1},
            "Image_RandomDate": {0: "123-123"},
        }
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        unique_file_path1 = to_unique_parquet.run(
            df=df, filename=f"{temp_dir}/unique_pq_file_test1"
        )
        # intentionally leave the same filename for testing
        # uniqueness in filename
        unique_file_path2 = to_unique_parquet.run(
            df=df, filename=f"{temp_dir}/unique_pq_file_test1"
        )

        assert unique_file_path1 != unique_file_path2
        assert os.path.isfile(unique_file_path1)


def test_multi_to_single_parquet():
    """
    Test multi_to_single_parquet
    """

    df = pd.DataFrame(
        data={
            "TableNumber": {0: 1},
            "ImageNumber": {0: 1},
            "Image_ImageData": {0: 1},
            "Image_RandomDate": {0: "123-123"},
        }
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        pq_files = [
            to_unique_parquet.run(df=df, filename=f"{temp_dir}/unique_pq_file_test1"),
            to_unique_parquet.run(df=df, filename=f"{temp_dir}/unique_pq_file_test1"),
        ]

        multi_file_path = multi_to_single_parquet.run(
            pq_files=pq_files, filename="multi_pq_file_test"
        )

        assert os.path.isfile(multi_file_path)
        assert len(pd.read_parquet(multi_file_path)) == 2


def test_flow_convert_sqlite_to_parquet(database_engine_for_conversion_testing):
    """
    Tests flow_convert_sqlite_to_parquet
    """

    with tempfile.TemporaryDirectory() as temp_dir:
        result_file_path = flow_convert_sqlite_to_parquet(
            sql_engine=database_engine_for_conversion_testing,
            flow_executor=LocalExecutor(),
            flow_storage=Local(),
            sql_tbl_basis="Image",
            sql_join_keys=["TableNumber", "ImageNumber"],
            sql_chunk_size=1,
            pq_filename="combined_test",
        )

        df = pd.read_parquet(result_file_path)
        assert os.path.isfile(result_file_path)

        # test overall row length
        assert len(df) == 8
        # test that all columns exist as they need to
        assert list(df.columns.sort_values()) == [
            "Cells_CellsData",
            "Cells_ObjectNumber",
            "Cytoplasm_CytoplasmData",
            "Cytoplasm_Cytoplasm_Parent_Cells",
            "Cytoplasm_Cytoplasm_Parent_Nuclei",
            "Cytoplasm_ObjectNumber",
            "ImageNumber",
            "Image_ImageData",
            "Image_RandomDate",
            "Nuclei_NucleiData",
            "Nuclei_ObjectNumber",
            "TableNumber",
        ]
        # test that despite our row length we have
        # correct number of unique join keys per what
        # was contained within the test database
        assert len(df["TableNumber"].unique()) == 2
        assert len(df["ImageNumber"].unique()) == 2
