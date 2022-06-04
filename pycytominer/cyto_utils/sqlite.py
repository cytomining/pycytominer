"""
Pycytominer SQLite utilities
"""

import logging
import os
import sqlite3
from typing import Optional, Union

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

# pylint: disable=consider-using-f-string

logger = logging.getLogger(__name__)

# A reference dictionary for SQLite affinity and storage class types
# See more here: https://www.sqlite.org/datatype3.html#affinity_name_examples
SQLITE_AFF_REF = {
    "INTEGER": [
        "INT",
        "INTEGER",
        "TINYINT",
        "SMALLINT",
        "MEDIUMINT",
        "BIGINT",
        "UNSIGNED BIG INT",
        "INT2",
        "INT8",
    ],
    "TEXT": [
        "CHARACTER",
        "VARCHAR",
        "VARYING CHARACTER",
        "NCHAR",
        "NATIVE CHARACTER",
        "NVARCHAR",
        "TEXT",
        "CLOB",
    ],
    "BLOB": ["BLOB"],
    "REAL": [
        "REAL",
        "DOUBLE",
        "DOUBLE PRECISION",
        "FLOAT",
    ],
    "NUMERIC": [
        "NUMERIC",
        "DECIMAL",
        "BOOLEAN",
        "DATE",
        "DATETIME",
    ],
}


def engine_from_str(sql_engine: Union[str, Engine]) -> Engine:
    """
    Helper function to create engine from a string or return the engine
    if it's already been created.

    Parameters
    ----------
    sql_engine: str | sqlalchemy.engine.base.Engine
        filename of the SQLite database or existing sqlalchemy engine
    Returns
    -------
    sqlalchemy.engine.base.Engine
        A SQLAlchemy engine
    """

    # check the type of sql_engine passed and create engine if we have a str
    if isinstance(sql_engine, str):
        # if we don't already have the sqlite filestring, add it
        if "sqlite:///" not in sql_engine:
            sql_engine = "sqlite:///{}".format(sql_engine)
        engine = create_engine(sql_engine)
    else:
        engine = sql_engine

    return engine


def collect_columns(
    sql_engine: Union[str, Engine],
    table_name: Optional[str] = None,
    column_name: Optional[str] = None,
) -> list:
    """
    Collect a list of columns from the given engine's
    database using optional table or column level
    specification.
    """

    # create column list for return result
    column_list = []

    # create an engine
    engine = engine_from_str(sql_engine)

    with engine.connect() as connection:
        if table_name is None:
            # if no table name is provided, we assume all tables must be scanned
            tables = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table';"
            ).fetchall()
        else:
            # otherwise we will focus on just the table name provided
            tables = [(table_name,)]

        for table in tables:
            if column_name is None:
                # if no column name is specified we will focus on all columns within the table
                sql_stmt = "SELECT :table_name, name, type FROM pragma_table_info(:table_name);"
            else:
                # otherwise we will focus on only the column name provided
                sql_stmt = """
                SELECT :table_name, name, type FROM pragma_table_info(:table_name) WHERE name = :col_name;
                """

            # append to column list the results
            column_list += connection.execute(
                sql_stmt,
                {"table_name": str(table[0]), "col_name": str(column_name)},
            ).fetchall()

    return column_list


def contains_conflicting_aff_strg_class(
    sql_engine: Union[str, Engine],
    table_name: Optional[str] = None,
    column_name: Optional[str] = None,
) -> bool:
    """
    Detect conflicting column affinity vs data storage class for
    entire SQLite database, a specific table, or a specific column
    within a specific table. See the following for more details on
    affinity vs storage class typing within SQLite:
    https://www.sqlite.org/datatype3.html

    Parameters
    ----------
    sql_engine: str | sqlalchemy.engine.base.Engine
        filename of the SQLite database or existing sqlalchemy engine
    table_name: str
        optional specific table name to check within database
    column_name: str
        optional specific column name to check within database

    Returns
    -------
    bool
        Returns True if conflicting storage class values were detected
        in database provided, else returns False.
    """

    logger.info(
        (
            "Determining if SQLite database contains conflicting column ",
            "affinity vs storage class values.",
        )
    )

    # create an engine
    engine = engine_from_str(sql_engine)

    # gather columns to be used below
    columns = collect_columns(engine, table_name, column_name)

    with engine.connect() as connection:
        for col in columns:
            # the sql below seeks to efficiently detect existence of values which
            # do not match the column affinity type (for ex. a string in an integer column).
            sql_stmt = """
            SELECT
                EXISTS(
                    SELECT 1 FROM {table_name} 
                    WHERE TYPEOF({col_name}) NOT IN ({col_types})
                );
            """
            # join formatted string for use with sql query in {col_types} var
            col_types = ",".join(
                ["'{}'".format(x.lower()) for x in SQLITE_AFF_REF[col[2]]]
            )

            # there are challenges with using sqlalchemy vars in the same manner as above
            # so we use format here along with nosec
            result = connection.execute(
                sql_stmt.format(
                    table_name=col[0],
                    col_name=col[1],
                    col_types=col_types,
                )
            ).fetchall()[0][
                0
            ]  # nosec
            if result > 0:
                # if our result is greater than 0 it means values with conflicting storage
                # class existed within the focus column and as a result, we return False
                logger.warning(
                    "Discovered conflicting %s column %s affinity type and storage class.",
                    col[0],
                    col[1],
                )
                return True

    # return false if we did not find conflicting affinity vs storage class values
    logger.info(
        "Found no conflicting affinity vs storage class data within provided database."
    )

    return False


def update_columns_to_nullable(
    sql_engine: Union[str, Engine],
    dest_path: str = None,
    table_name: Optional[str] = None,
    inplace: bool = True,
) -> Engine:
    """
    Update SQLite database columns to nullable where appropriate.
    Use a backup database to avoid data corruption issues and
    roughly follow 9-step procedure outlined by SQLite docs here:
    https://www.sqlite.org/lang_altertable.html#making_other_kinds_of_table_schema_changes

    Special notes:
    - We take advantage of Python >= 3.7 sqlite3 api backup capabilities
    to keep to a standard implementation for backup portion of this work.

    Parameters
    ----------
    sql_engine: str | sqlalchemy.engine.base.Engine
        filename of the SQLite database or existing sqlalchemy engine
    dest_path: str
        the destination of the updated database with nullable columns.
    table_name: str
        optional specific table name to update within database
    inplace: bool
        whether to replace the source sql database

    Returns
    -------
    sqlalchemy.engine.base.Engine
        A SQLAlchemy engine for the changed database
    """

    logger.info("Updating database columns to nullable for provided database.")

    # setup a source database connection
    if isinstance(sql_engine, Engine):
        # gather sqlalchemy engine url replacing incompatible strings
        src_sql_url = str(sql_engine.url).replace("sqlite:///", "")
    else:
        src_sql_url = sql_engine

    source_engine = sqlite3.connect(src_sql_url)

    # add a default destination path which is separate from our source
    if dest_path is None:
        dest_path = src_sql_url + "_column_update"

    # setup a destination database connection
    dest_engine = sqlite3.connect(dest_path)

    # create backup of database using sqlite3 api
    source_engine.backup(dest_engine)

    # gather schema_version for later update
    schema_version = dest_engine.execute("PRAGMA schema_version;").fetchall()[0][0]

    # gather existing table(s) sql later update
    if table_name is not None:
        # if we have a table name provided, target only that table for the modifications
        sql_stmt = (
            "SELECT name, sql FROM sqlite_master "
            "WHERE type = 'table' and UPPER(name) = UPPER(:table_name)"
        )
    else:
        # else we target all tables within the database
        sql_stmt = "SELECT name, sql FROM sqlite_master WHERE type = 'table';"

    table_sql_fetch = dest_engine.execute(
        sql_stmt, {"table_name": table_name}
    ).fetchall()

    # prepare table sql with removed not null columns
    table_sql_mod = {
        table[0]: table[1].replace("NOT NULL", "") for table in table_sql_fetch
    }

    if len(table_sql_mod) > 0:
        # check that we have sql to modify

        # disallow autocommit and create cursor for transaction
        dest_engine.isolation_level = None
        cursor = dest_engine.cursor()
        try:
            # begin transaction
            cursor.execute("begin")

            # enable schema writes
            cursor.execute("PRAGMA writable_schema=ON")

            # prepare update statement which will perform the table sql update
            sql_stmt = """
            UPDATE sqlite_schema SET sql = :modified_sql 
            WHERE type = 'table' AND UPPER(name) = UPPER(:table_name);
            """
            for name, modified_sql in table_sql_mod.items():
                cursor.execute(
                    sql_stmt, {"table_name": name, "modified_sql": modified_sql}
                )
            # increment the schema version to track the change
            cursor.execute(
                "PRAGMA schema_version={new_version};".format(
                    new_version=schema_version + 1
                ),
            )

            # disable schema writes
            cursor.execute("PRAGMA writable_schema=OFF")

            # check the integrity of the database as advised by SQLite docs
            if cursor.execute("PRAGMA integrity_check").fetchall()[0][0] != "ok":
                raise sqlite3.IntegrityError(
                    "Detected integrity issue within database after modifications."
                )

            # commit the changes
            cursor.execute("commit;")

        except sqlite3.Error as err:
            logger.error(err)
            cursor.execute("rollback;")

    if inplace:
        # backup our destination into the source engine, overwriting our original
        dest_engine.backup(source_engine)

    # close our connections
    source_engine.close()
    dest_engine.close()

    if inplace:
        # remove working backup
        os.remove(dest_path.replace("sqlite:///", ""))

        # return source database
        return engine_from_str(src_sql_url)

    # return copied and modified destination database
    return engine_from_str(dest_path)


def update_columns_nan_to_null(
    sql_engine: Union[str, Engine],
    table_name: Optional[str] = None,
    column_name: Optional[str] = None,
) -> Engine:

    logger.info("Updating columns with str 'nan' to NULL values.")

    # create an engine
    engine = engine_from_str(sql_engine)

    # gather columns to be used below
    columns = collect_columns(engine, table_name, column_name)

    with engine.begin() as connection:
        for col in columns:
            # sql to update nan strings to sqlite nulls
            sql_stmt = """
            UPDATE {table_name} SET {col_name}=NULL 
            WHERE {col_name}='nan'
            AND EXISTS(SELECT 1 FROM {table_name}
                WHERE {col_name}='nan'
            )
            """
            connection.execute(
                statement=sql_stmt.format(
                    table_name=col[0],
                    col_name=col[1],
                )
            )  # nosec

    return engine
