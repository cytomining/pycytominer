""" Tests for pycytominer.cyto_utils.sqlite """

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from pycytominer.cyto_utils.sqlite import contains_conflicting_aff_strg_class


@pytest.fixture
def database_engine_for_testing() -> Engine:
    """
    A database engine for testing as a fixture to be passed
    to other tests within this file.
    """

    # set our path to an in-memory database and create the engine
    sql_path = ":memory:"
    engine = create_engine(f"sqlite:///{sql_path}")

    # statements for creating database with simple structure
    create_stmts = [
        """
    drop table if exists tbl_a;
    """,
        """
    create table tbl_a (
    col_integer INTEGER
    ,col_text TEXT
    ,col_blob BLOB
    ,col_real REAL
    );
    """,
        """
    create table tbl_b (
    col_integer INTEGER
    ,col_text TEXT
    ,col_blob BLOB
    ,col_real REAL
    );
    """,
    ]

    # insert statement with some simple values
    # note: we use SQLAlchemy's parameters to insert data properly, esp. BLOB
    insert_stmt_a = """
    INSERT INTO tbl_a (col_integer, col_text, col_blob, col_real)
    VALUES (?, ?, ?, ?);
    """
    insert_stmt_b = """
    INSERT INTO tbl_b (col_integer, col_text, col_blob, col_real)
    VALUES (?, ?, ?, ?);
    """
    insert_vals = [1, "sample", b"sample_blob", 0.5]

    with engine.connect() as e:
        for stmt in create_stmts:
            e.execute(stmt)
        e.execute(insert_stmt_a, insert_vals)
        e.execute(insert_stmt_b, insert_vals)

    return engine


def test_contains_conflicting_aff_strg_class(database_engine_for_testing):
    """
    Testing contains_conflicting_aff_strg_class
    """

    # test string-based sql_path and empty database (no schema should mean no conflict)
    assert contains_conflicting_aff_strg_class(sql_engine=":memory:") == False

    engine = database_engine_for_testing

    # test non-conflicting database
    assert contains_conflicting_aff_strg_class(engine) == False
    # test non-conlicting database single table
    assert contains_conflicting_aff_strg_class(engine, table_name="tbl_a") == False
    # test non-conlicting database single table and single column
    assert (
        contains_conflicting_aff_strg_class(
            engine, table_name="tbl_a", column_name="col_integer"
        )
        == False
    )

    # add a conflicting row of values for tbl_a
    with engine.connect() as e:
        e.execute(
            """
        INSERT INTO tbl_a (col_integer, col_text, col_blob, col_real)
        VALUES ('nan', 'another', 'example', 0.5);
        """
        )

    # test conflicting database
    assert contains_conflicting_aff_strg_class(engine) == True
    # test conflicting database single table, conflicting table
    assert contains_conflicting_aff_strg_class(engine, table_name="tbl_a") == True
    # test conflicting database single table, non-conflicting table
    assert contains_conflicting_aff_strg_class(engine, table_name="tbl_b") == False
    # test conflicting database single table and single conflicting column
    assert (
        contains_conflicting_aff_strg_class(
            engine, table_name="tbl_a", column_name="col_integer"
        )
        == True
    )
    # test conflicting database single table and single non-conflicting column
    assert (
        contains_conflicting_aff_strg_class(
            engine, table_name="tbl_a", column_name="col_text"
        )
        == False
    )
