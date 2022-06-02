"""
Pycytominer SQLite utilities
"""

import logging
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

    # check the type of sql_engine passed and create engine if we have a str
    if isinstance(sql_engine, str):
        engine = create_engine(f"sqlite:///{sql_engine}")
    else:
        engine = sql_engine

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
                    sql_stmt = "SELECT name, type FROM pragma_table_info(:table_name);"
                else:
                    # otherwise we will focus on only the column name provided
                    sql_stmt = """
                    SELECT name, type FROM pragma_table_info(:table_name) WHERE name = :col_name;
                    """

                cols = connection.execute(
                    sql_stmt,
                    {"table_name": str(table[0]), "col_name": str(column_name)},
                ).fetchall()

                for col in cols:
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
                        ["'{}'".format(x.lower()) for x in SQLITE_AFF_REF[col[1]]]
                    )

                    # there are challenges with using sqlalchemy vars in the same manner as above
                    # so we use format here along with nosec
                    result = connection.execute(
                        sql_stmt.format(
                            table_name=table[0],
                            col_name=col[0],
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
                            table[0],
                            col[0],
                        )
                        return True

    # return false if we did not find conflicting affinity vs storage class values
    logger.info(
        "Found no conflicting affinity vs storage class data within provided database."
    )
    return False
