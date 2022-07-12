"""
Pycytominer SQLite utilities - meta database and data management
"""

import logging
from typing import Optional, Union

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

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

# strings which may represent null values
LIKE_NULLS = ("null", "none", "nan")


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
            sql_engine = f"sqlite:///{sql_engine}"
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

    Parameters
    ----------
    sql_engine: str | sqlalchemy.engine.base.Engine
        filename of the SQLite database or existing sqlalchemy engine
    table_name: str
        optional specific table name to check within database, by default None
    column_name: str
        optional specific column name to check within database, by default None

    Returns
    -------
    list
        Returns list, and if populated, contains tuples with values
        similar to the following. These may also be accessed by name
        similar to dictionaries, as they are SQLAlchemy Row objects.
        [('table_name', 'column_name', 'column_type', 'notnull'),...]
    """

    # create column list for return result
    column_list = []

    # create an engine
    engine = engine_from_str(sql_engine)

    with engine.connect() as connection:
        if table_name is None:
            # if no table name is provided, we assume all tables must be scanned
            tables = connection.execute(
                "SELECT name as table_name FROM sqlite_master WHERE type = 'table';"
            ).fetchall()
        else:
            # otherwise we will focus on just the table name provided
            tables = [{"table_name": table_name}]

        for table in tables:

            # if no column name is specified we will focus on all columns within the table
            sql_stmt = """
            SELECT :table_name as table_name,
                    name as column_name,
                    type as column_type,
                    [notnull]
            FROM pragma_table_info(:table_name)
            """

            if column_name is not None:
                # otherwise we will focus on only the column name provided
                sql_stmt = f"{sql_stmt} WHERE name = :col_name;"

            # append to column list the results
            column_list += connection.execute(
                sql_stmt,
                {"table_name": str(table["table_name"]), "col_name": str(column_name)},
            ).fetchall()

    return column_list
