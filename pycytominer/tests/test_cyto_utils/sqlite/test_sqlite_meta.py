""" 
Tests for: pycytominer.cyto_utils.sqlite.meta
"""

from pycytominer.cyto_utils.sqlite.meta import collect_columns, engine_from_str
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def test_engine_from_str():
    """
    Testing engine_from_str
    """
    # test str functionality
    engine = engine_from_str(":memory:")
    assert isinstance(engine, Engine)
    assert engine.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

    # test sqlalchemy engine
    engine = engine_from_str(create_engine("sqlite:///:memory:"))
    assert isinstance(engine, Engine)
    assert engine.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_collect_columns(database_engine_for_testing):
    """
    Testing collect_columns
    """

    # test full database columns collected
    assert len(collect_columns(database_engine_for_testing)) == 8

    # test single database table collected
    assert collect_columns(database_engine_for_testing, table_name="tbl_a") == [
        ("tbl_a", "col_integer", "INTEGER", 1),
        ("tbl_a", "col_text", "TEXT", 0),
        ("tbl_a", "col_blob", "BLOB", 0),
        ("tbl_a", "col_real", "REAL", 0),
    ]

    # test single column from single table collected
    assert collect_columns(
        database_engine_for_testing,
        table_name="tbl_b",
        column_name="col_integer",
    ) == [("tbl_b", "col_integer", "INTEGER", 0)]
