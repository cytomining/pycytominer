""" 
Tests for: pycytominer.cyto_utils.sqlite.clean
"""

import pytest
from pycytominer.cyto_utils.sqlite.clean import (
    clean_like_nulls,
    contains_conflicting_aff_storage_class,
    contains_str_like_null,
    update_columns_to_nullable,
    update_values_like_null_to_null,
)
from pycytominer.cyto_utils.sqlite.meta import LIKE_NULLS
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import IntegrityError


def test_contains_conflicting_aff_storage_class(database_engine_for_testing):
    """
    Testing contains_conflicting_aff_storage_class
    """

    # test string-based sql_path and empty database (no schema should mean no conflict)
    assert contains_conflicting_aff_storage_class(sql_engine=":memory:") is False

    # test non-conflicting database
    assert contains_conflicting_aff_storage_class(database_engine_for_testing) is False
    # test non-conlicting database single table
    assert (
        contains_conflicting_aff_storage_class(
            database_engine_for_testing, table_name="tbl_a"
        )
        is False
    )
    # test non-conlicting database single table and single column
    assert (
        contains_conflicting_aff_storage_class(
            database_engine_for_testing, table_name="tbl_a", column_name="col_integer"
        )
        is False
    )

    # add a conflicting row of values for tbl_a
    with database_engine_for_testing.begin() as connection:
        connection.execute(
            """
            INSERT INTO tbl_a (col_integer, col_text, col_blob, col_real)
            VALUES ('nan', 'None', 'example', 0.5);
            """
        )

    # test conflicting database
    assert contains_conflicting_aff_storage_class(database_engine_for_testing) is True
    # test conflicting database single table, conflicting table
    assert (
        contains_conflicting_aff_storage_class(
            database_engine_for_testing, table_name="tbl_a"
        )
        is True
    )
    # test conflicting database single table, non-conflicting table
    assert (
        contains_conflicting_aff_storage_class(
            database_engine_for_testing, table_name="tbl_b"
        )
        is False
    )
    # test conflicting database single table and single conflicting column
    assert (
        contains_conflicting_aff_storage_class(
            database_engine_for_testing, table_name="tbl_a", column_name="col_integer"
        )
        is True
    )
    # test conflicting database single table and single non-conflicting column
    assert (
        contains_conflicting_aff_storage_class(
            database_engine_for_testing, table_name="tbl_a", column_name="col_text"
        )
        is False
    )


def test_contains_str_like_null(database_engine_for_testing):
    """
    Testing contains_str_like_null
    """

    # assert no strs like nulls in full database
    assert contains_str_like_null(database_engine_for_testing) is False

    # add a str like null
    with database_engine_for_testing.begin() as connection:
        connection.execute(
            """
            INSERT INTO tbl_a (col_integer, col_text, col_blob, col_real)
            VALUES ('NaN', 'NULL', 'nan', 'None');
            """
        )

    # assert strs like nulls in specific cols
    assert (
        contains_str_like_null(
            database_engine_for_testing, table_name="tbl_a", column_name="col_integer"
        )
        and contains_str_like_null(
            database_engine_for_testing, table_name="tbl_a", column_name="col_text"
        )
        and contains_str_like_null(
            database_engine_for_testing, table_name="tbl_a", column_name="col_blob"
        )
        and contains_str_like_null(
            database_engine_for_testing, table_name="tbl_a", column_name="col_real"
        )
    ) is True

    # assert no strs like nulls in specific table
    assert (
        contains_str_like_null(database_engine_for_testing, table_name="tbl_b") is False
    )


def test_update_columns_to_nullable(database_engine_for_testing):
    """
    Testing update_columns_to_nullable
    """

    # test updating whole database
    updated_engine = update_columns_to_nullable(
        sql_engine=database_engine_for_testing, inplace=False
    )

    # test return type as sqlalchemy
    assert isinstance(updated_engine, Engine)
    # test whole database changed correct column
    assert (
        updated_engine.execute(
            "SELECT [notnull] FROM pragma_table_info('tbl_a') WHERE name = 'col_integer';"
        ).fetchone()[0]
        == 0
    )
    # check that we didn't update inplace
    assert updated_engine.url != database_engine_for_testing.url

    # test updating table with no changes
    updated_engine = update_columns_to_nullable(
        sql_engine=database_engine_for_testing, table_name="tbl_b"
    )
    # check that tbl_a not null column is still not null
    assert (
        updated_engine.execute(
            "SELECT [notnull] FROM pragma_table_info('tbl_a') WHERE name = 'col_integer';"
        ).fetchone()[0]
        == 1
    )

    # test updating inplace
    updated_engine = update_columns_to_nullable(
        sql_engine=database_engine_for_testing, inplace=True
    )
    assert updated_engine.url == database_engine_for_testing.url


def test_update_values_like_null_to_null(database_engine_for_testing):
    """
    Testing update_values_like_null_to_null
    """

    # test updating tbl_b
    updated_engine = update_values_like_null_to_null(
        sql_engine=database_engine_for_testing
    )

    # test return type as sqlalchemy
    assert isinstance(updated_engine, Engine)

    # add a conflicting row of values for tbl_a
    with database_engine_for_testing.begin() as connection:
        connection.execute(
            """
            INSERT INTO tbl_a (col_integer, col_text, col_blob, col_real)
            VALUES ('nan', 'None', 'Null', 0.5);
            """
        )

    # test updating only tbl_a col_text
    updated_engine = update_values_like_null_to_null(
        sql_engine=database_engine_for_testing,
        table_name="tbl_a",
        column_name="col_text",
    )
    sql_stmt = """
    SELECT EXISTS(
        SELECT 1 FROM tbl_a 
        WHERE col_text='None'
        );
    """
    assert updated_engine.execute(sql_stmt).fetchone()[0] == 0

    # test updating only tbl_a col_blob
    updated_engine = update_values_like_null_to_null(
        sql_engine=database_engine_for_testing,
        table_name="tbl_a",
        column_name="col_blob",
    )
    sql_stmt = """
    SELECT EXISTS(
        SELECT 1 FROM tbl_a 
        WHERE col_blob='Null'
        );
    """
    assert updated_engine.execute(sql_stmt).fetchone()[0] == 0

    # test updating only tbl_a col_integer
    # should raise exception due to not null constraint
    with pytest.raises(IntegrityError):
        updated_engine = update_values_like_null_to_null(
            sql_engine=database_engine_for_testing,
            table_name="tbl_a",
            column_name="col_integer",
        )


def test_clean_like_nulls(database_engine_for_testing):
    """
    Testing clean_like_nulls
    """

    # gather schema_version
    schema_version = database_engine_for_testing.execute(
        "PRAGMA schema_version;"
    ).fetchall()[0][0]

    # gather database url
    database_url = str(database_engine_for_testing.url)

    cleaned_database = clean_like_nulls(database_engine_for_testing)
    # test that the schema version has not changed
    # (no changes necessary, so the engine is passed back as-is)
    assert (
        cleaned_database.execute("PRAGMA schema_version;").fetchall()[0][0]
        == schema_version
    )

    # assert that the database url did not change
    assert str(cleaned_database.url) == database_url

    # add a conflicting row of values for tbl_a
    with database_engine_for_testing.begin() as connection:
        connection.execute(
            """
            INSERT INTO tbl_a (col_integer, col_text, col_blob, col_real)
            VALUES ('nan', 'None', 'Null', 'NaN');
            """
        )

    # clean the like nulls for single table without nulls
    cleaned_database = clean_like_nulls(database_engine_for_testing, table_name="tbl_b")

    # test that the schema version has not changed
    assert (
        cleaned_database.execute("PRAGMA schema_version;").fetchall()[0][0]
        == schema_version
    )

    # clean the like nulls for single table with nulls
    cleaned_database = clean_like_nulls(
        database_engine_for_testing, table_name="tbl_a", column_name="col_text"
    )

    # test that the schema version has not changed
    assert (
        cleaned_database.execute("PRAGMA schema_version;").fetchall()[0][0]
        == schema_version
    )

    # build sql to check for the like_nulls
    like_nulls_str_list = ",".join([f"'{x}'" for x in LIKE_NULLS])
    select_stmt = f"""
    SELECT EXISTS(
        SELECT 1 FROM tbl_a 
        WHERE LOWER(col_text) in ({like_nulls_str_list})
        );
    """
    # check that there are no like nulls any longer within the table
    assert cleaned_database.execute(select_stmt).fetchall()[0][0] == 0

    # clean the like nulls for single table with nulls
    cleaned_database = clean_like_nulls(
        database_engine_for_testing,
        table_name="tbl_a",
        inplace=False,
    )

    # test that the schema version has changed
    assert (
        cleaned_database.execute("PRAGMA schema_version;").fetchall()[0][0]
        != schema_version
    )

    # assert that the database url did not change
    assert str(cleaned_database.url) != database_url

    select_stmt = f"""
    SELECT EXISTS(
        SELECT 1 FROM tbl_a 
        WHERE LOWER(col_integer) in ({like_nulls_str_list})
        OR LOWER(col_text) in ({like_nulls_str_list})
        OR LOWER(col_blob) in ({like_nulls_str_list})
        OR LOWER(col_real) in ({like_nulls_str_list})
        );
    """
    # check that there are no like nulls any longer within the table
    assert cleaned_database.execute(select_stmt).fetchall()[0][0] == 0
