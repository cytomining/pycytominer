"""
Pytest conftest file for sqlite series of tests.
"""

import os
import tempfile
from typing import Dict, List

import pytest
from pycytominer.cyto_utils.sqlite.convert import sql_select_distinct_join_chunks
from pycytominer.cyto_utils.sqlite.meta import collect_columns
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


@pytest.fixture
def database_engine_for_testing() -> Engine:
    """
    A database engine for testing as a fixture.
    """

    # get temporary directory
    tmpdir = tempfile.gettempdir()

    # create a temporary sqlite connection
    sql_path = f"sqlite:///{tmpdir}/test_sqlite.sqlite"

    engine = create_engine(sql_path)

    # statements for creating database with simple structure
    create_stmts = [
        "drop table if exists tbl_a;",
        """
        create table tbl_a (
        col_integer INTEGER NOT NULL
        ,col_text TEXT
        ,col_blob BLOB
        ,col_real REAL
        );
        """,
        "drop table if exists tbl_b;",
        """
        create table tbl_b (
        col_integer INTEGER
        ,col_text TEXT
        ,col_blob BLOB
        ,col_real REAL
        );
        """,
    ]

    insert_vals = [1, "sample", b"sample_blob", 0.5]

    with engine.begin() as connection:
        for stmt in create_stmts:
            connection.execute(stmt)

        # insert statement with some simple values
        # note: we use SQLAlchemy's parameters to insert data properly, esp. BLOB
        connection.execute(
            (
                "INSERT INTO tbl_a (col_integer, col_text, col_blob, col_real)"
                "VALUES (?, ?, ?, ?);"
            ),
            insert_vals,
        )
        connection.execute(
            (
                "INSERT INTO tbl_b (col_integer, col_text, col_blob, col_real)"
                "VALUES (?, ?, ?, ?);"
            ),
            insert_vals,
        )

    return engine


@pytest.fixture
def database_engine_for_conversion_testing() -> Engine:
    """
    A database engine for conversion work testing as a fixture.
    """

    # get temporary directory
    tmpdir = tempfile.gettempdir()

    sql_filepath = f"{tmpdir}/test_conversion_sqlite.sqlite"

    # remove db if it exists
    if os.path.exists(f"{tmpdir}/test_sqlite.sqlite"):
        os.remove(f"{tmpdir}/test_sqlite.sqlite")

    # create a temporary sqlite connection
    sql_path = f"sqlite:///{sql_filepath}"

    engine = create_engine(sql_path)

    # statements for creating database with simple structure
    # intended to very roughly match existing cytomining
    # community SQLite files.
    create_stmts = [
        "drop table if exists Image;",
        """
        create table Image (
        TableNumber INTEGER
        ,ImageNumber INTEGER
        ,ImageData INTEGER
        ,RandomDate DATETIME
        );
        """,
        "drop table if exists Cells;",
        """
        create table Cells (
        TableNumber INTEGER
        ,ImageNumber INTEGER
        ,ObjectNumber INTEGER
        ,CellsData INTEGER
        );
        """,
        "drop table if exists Nuclei;",
        """
        create table Nuclei (
        TableNumber INTEGER
        ,ImageNumber INTEGER
        ,ObjectNumber INTEGER
        ,NucleiData INTEGER
        );
        """,
        "drop table if exists Cytoplasm;",
        """
        create table Cytoplasm (
        TableNumber INTEGER
        ,ImageNumber INTEGER
        ,ObjectNumber INTEGER
        ,Cytoplasm_Parent_Cells INTEGER
        ,Cytoplasm_Parent_Nuclei INTEGER
        ,CytoplasmData INTEGER
        );
        """,
    ]

    with engine.begin() as connection:
        for stmt in create_stmts:
            connection.execute(stmt)

        # images
        connection.execute(
            "INSERT INTO Image VALUES (?, ?, ?, ?);",
            [1, 1, 1, "123-123"],
        )
        connection.execute(
            "INSERT INTO Image VALUES (?, ?, ?, ?);",
            [2, 2, 2, "123-123"],
        )

        # cells
        connection.execute(
            "INSERT INTO Cells VALUES (?, ?, ?, ?);",
            [1, 1, 2, 1],
        )
        connection.execute(
            "INSERT INTO Cells VALUES (?, ?, ?, ?);",
            [2, 2, 3, 1],
        )

        # Nuclei
        connection.execute(
            "INSERT INTO Nuclei VALUES (?, ?, ?, ?);",
            [1, 1, 4, 1],
        )
        connection.execute(
            "INSERT INTO Nuclei VALUES (?, ?, ?, ?);",
            [2, 2, 5, 1],
        )

        # cytoplasm
        connection.execute(
            "INSERT INTO Cytoplasm VALUES (?, ?, ?, ?, ?, ?);",
            [1, 1, 6, 2, 4, 1],
        )
        connection.execute(
            "INSERT INTO Cytoplasm VALUES (?, ?, ?, ?, ?, ?);",
            [2, 2, 7, 3, 5, 1],
        )

    return engine


@pytest.fixture
def database_distinct_join_chunks(
    database_engine_for_conversion_testing,
) -> List[List[Dict]]:
    """
    Fixture for database chunks
    """

    return sql_select_distinct_join_chunks.run(
        sql_engine=database_engine_for_conversion_testing,
        table_name="Image",
        join_keys=["TableNumber", "ImageNumber"],
        chunk_size=1,
    )


@pytest.fixture
def database_column_data(
    database_engine_for_conversion_testing,
) -> List[List[Dict]]:
    """
    Fixture for column data from database
    """

    return collect_columns(sql_engine=database_engine_for_conversion_testing)
