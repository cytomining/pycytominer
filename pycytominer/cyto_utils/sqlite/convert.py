"""
Pycytominer SQLite utilities - conversion work for sqlite databases
"""

import itertools
import logging
import pathlib
import uuid
from typing import Dict, List, Union

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from prefect import Flow, Parameter, task, unmapped
from prefect.executors import Executor
from prefect.storage import Storage
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from .meta import collect_columns

logger = logging.getLogger(__name__)


@task
def sql_select_distinct_join_chunks(
    sql_engine: str, table_name: str, join_keys: List[str], chunk_size: int
) -> List[List[Dict]]:
    """
    Selects distinct chunks of values from SQLite.

    Parameters
    ----------
    sql_engine: str:
        SQLite database engine url url
    table_name: str:
        Name of table to reference for this function
    join_keys: List[str]:
        Keys to use for building unique chunk sets
    chunk_size: int:
        Size of chunk sets to use

    Returns
    -------
    List[List[Dict]]
        A list of lists with dictionaries for sets of
        unique join keys.
    """

    # form string for sql query based on join keys for chunking
    join_keys_str = ", ".join(join_keys)

    # select distinct results from database based on join keys
    sql_stmt = f"""
    select distinct {join_keys_str} from {table_name}
    """

    # gather a dictionary from pandas dataframe based on results from query
    result_dicts = pd.read_sql(
        sql_stmt,
        create_engine(sql_engine),
    ).to_dict(orient="records")

    # build chunked result dict list
    chunked_result_dicts = [
        result_dicts[i : i + chunk_size]
        for i in range(0, len(result_dicts), chunk_size)
    ]

    return chunked_result_dicts


@task
def sql_table_to_pd_dataframe(
    sql_engine: str,
    table_name: str,
    prepend_tablename_to_cols: bool,
    avoid_prepend_for: List[str],
    chunk_list_dicts: list,
    column_data: List[dict],
) -> pd.DataFrame:
    """

    Parameters
    ----------
    sql_engine: str:
        SQLite database engine url
    table_name: str:
        Name of table to reference for this function.
        Examples for this parameter: "Image", "Cells", "Cytoplasm", "Nuclei"
    prepend_tablename_to_cols: bool:
        Determines whether we prepend table name
        to column name.
    avoid_prepend_for: List[str]:
        List of column names to avoid tablename prepend
    chunk_list_dicts: list:
        List of dictionaries for chunked querying
    column_data: List[dict]:
        Column metadata extracted from database.

    Returns
    -------
    pd.DataFrame
        DataFrame with results of query.

    Examples
    --------

    .. code-block:: python

        from prefect import Flow, Parameter, task
        from sqlalchemy.engine import create_engine

        from pycytominer.cyto_utils.sqlite.convert import sql_table_to_pd_dataframe
        from pycytominer.cyto_utils.sqlite.meta import collect_columns

        sql_path = "test_SQ00014613.sqlite"
        sql_url = f"sqlite:///{sql_path}"

        with Flow("Example Flow") as flow:
            param_sql_engine = Parameter("sql_engine", default="")

            # form prefect task from sqlite meta util
            task_collect_columns = task(collect_columns)

            # gather sql column and table data flow operations
            column_data = task_collect_columns(sql_engine=param_sql_engine)

            dataframe_result = sql_table_to_pd_dataframe(
                sql_engine=param_sql_engine,
                table_name="Image",
                prepend_tablename_to_cols=True,
                avoid_prepend_for=["TableNumber", "ImageNumber"],
                chunk_list_dicts=[
                    {"TableNumber": "dd77885d07028e67dc9bcaaba4df34c6", "ImageNumber": "1"},
                    {"TableNumber": "1e5d8facac7508cfd4086f3e3e950182", "ImageNumber": "2"},
                ],
                column_data=column_data,
            )

        # run the flow as outlined above
        flow_state = flow.run(parameters=dict(sql_engine=sql_url))

        # access the dataframe result of the flow
        df = flow_state.result[dataframe_result].result

        # print info from dataframe result
        print(df.info())

    """

    # adds the tablename to the front of column name for query
    if prepend_tablename_to_cols:
        colnames = [
            coldata["column_name"]
            for coldata in column_data
            if coldata["table_name"] == table_name
        ]
        # build aliased column names with table name prepended
        colstring = ",".join(
            [
                f"{colname} as '{table_name}_{colname}'"
                if colname not in avoid_prepend_for
                else colname
                for colname in colnames
            ]
        )
        sql_stmt = f"select {colstring} from {table_name}"
    else:
        sql_stmt = f"select * from {table_name}"

    # build sql query where clause based on chunk_list_dicts
    chunk_list_dicts_str = " OR ".join(
        [
            f"({where_group})"
            for where_group in [
                " AND ".join([f"{key} = '{val}'" for key, val in list_dict.items()])
                for list_dict in chunk_list_dicts
            ]
        ]
    )

    # append the where clause to the query
    sql_stmt += f" where {chunk_list_dicts_str}"

    # return the sql query as pandas dataframe
    return pd.read_sql(sql_stmt, create_engine(sql_engine))


@task
def nan_data_fill(fill_into: pd.DataFrame, fill_from: pd.DataFrame) -> pd.DataFrame:
    """
    Add columns with nan data where missing for fill_into
    dataframe.

    Parameters
    ----------
    fill_into: pd.DataFrame:
        Dataframe to fill into.
    fill_from: pd.DataFrame:
        Dataframe to reference for fill.

    Returns
    -------
    pd.DataFrame
        New fill_into pd.Dataframe with added column(s)
        and nan data.
    """

    # gather columns and dtype missing in fill_into based on fill_from
    colnames_and_types = {
        # note: we replace int64 with float64 to accommodate np.nan
        colname: str(fill_from[colname].dtype).replace("int64", "float64")
        for colname in fill_from.columns
        if colname not in fill_into.columns
    }

    # append all columns not in fill_into table into fill_into
    fill_into = pd.concat(
        [
            fill_into,
            # generate a dataframe with proper row-length and
            # new columns (+matching datatype) which do not yet
            # exist within fill_into based on fill_from
            pd.DataFrame(
                {
                    colname: pd.Series(
                        data=np.nan,
                        index=fill_into.index,
                        dtype=coltype,
                    )
                    for colname, coltype in colnames_and_types.items()
                },
                index=fill_into.index,
            ),
        ],
        axis=1,
    )

    return fill_into


@task
def table_concat_to_parquet(
    sql_engine: str,
    column_data: List[dict],
    prepend_tablename_to_cols: bool,
    avoid_prepend_for: list,
    chunk_list_dicts: list,
    filename: str,
):
    """
    Concatenate chunk of database tables together as
    single dataframe, adding any missing columns along
    the way, and then dumping to uniquely named parquet
    file using a filename prefix.

    Parameters
    ----------
    sql_engine: str:
        SQLite database engine url
    column_data: List[dict]:
        Column metadata from database
    prepend_tablename_to_cols: bool:
        Determines whether we prepend table name
        to column name.
    avoid_prepend_for: list:
        List of column names to avoid tablename prepend
    chunk_list_dicts: list:
        List of dictionaries for chunked querying
    filename: str:
        Filename to be used for parquet export

    Returns
    -------
    str
        Filename of parquet file created.
    """

    # gather a set of the table names
    # note: coldata contains repeated tablenames and we gather unique
    # tablenames only via set from the list.
    table_list = set([coldata["table_name"] for coldata in column_data])

    # build empty dataframe which will become the basis of concat operations
    concatted = pd.DataFrame()

    for table in table_list:
        # query data from table by join key chunks, ensuring unique column names where necessary
        to_concat = sql_table_to_pd_dataframe.run(
            sql_engine=sql_engine,
            table_name=table,
            prepend_tablename_to_cols=prepend_tablename_to_cols,
            avoid_prepend_for=avoid_prepend_for,
            chunk_list_dicts=chunk_list_dicts,
            column_data=column_data,
        )

        # if concatted is empty, we set it to the first dataframe
        if len(concatted) == 0:
            concatted = to_concat

        # else we concat the existing dataframe with the new one
        else:
            # both the already concatted and target are prepared with matching columns nan's
            # for data they do not contain.
            concatted, to_concat = list(
                itertools.starmap(
                    nan_data_fill.run, [[concatted, to_concat], [to_concat, concatted]]
                )
            )

            # bring the prepared dataframes together as the new concatted
            concatted = pd.concat([concatted, to_concat])

    # export concatted result from all tables to a uniquely name parquet file
    filename_uuid = to_unique_parquet.run(df=concatted, filename=filename)

    # return the filename generated from to_unique_parquet
    return filename_uuid


@task
def to_unique_parquet(df: pd.DataFrame, filename: str) -> str:
    """
    Write a uniquely named parquet from provided dataframe.

    Parameters
    ----------
    df: pd.DataFrame:
        Dataframe to use for parquet write
    filename: str:
        Filename to use along with uuid for unique filenames

    Returns
    -------
    str
        Unique filename for parquet file created.
    """

    # gather unique id for filename
    file_uuid = str(uuid.uuid4().hex)

    # build a unique filename string
    unique_filename = f"{filename}-{file_uuid}"

    # export the dataframe based on the unique filename
    df.to_parquet(unique_filename, compression=None)

    # return the unique filename generated
    return unique_filename


@task
def multi_to_single_parquet(
    pq_files: List[str],
    filename: str,
) -> str:
    """
    Take a list of parquet file paths and write them
    as one single parquet file. Assumes exact same
    data schema for all files.

    Parameters
    ----------
    pq_files: List[str]:
        List of parquet file paths
    filename: str:
        Filename to use for the parquet file.

    Returns
    -------
    str
        Filename of the single parquet file.
    """

    # if there's already a file remove it
    path = pathlib.Path(filename)
    if path.exists():
        path.unlink()

    # build a parquet file writer which will be used to append files from pq_files
    # as a single concatted parquet file, referencing the first file's schema
    # (all must be the same schema)
    writer = pq.ParquetWriter(filename, pq.read_table(pq_files[0]).schema)

    for tbl in pq_files:
        # read the file from the list and write to the concatted parquet file
        writer.write_table(pq.read_table(tbl))
        # remove the file which was written in the concatted parquet file (we no longer need it)
        pathlib.Path(tbl).unlink()

    # close the single concatted parquet file writer
    writer.close()

    # return the concatted parquet filename
    return filename


def flow_convert_sqlite_to_parquet(
    sql_engine: Union[str, Engine],
    flow_executor: Executor,
    flow_storage: Storage,
    sql_tbl_basis: str = "Image",
    sql_join_keys: List[str] = ["TableNumber", "ImageNumber"],
    sql_chunk_size: int = 10,
    pq_filename: str = "combined.parquet",
) -> str:
    """
    Run a Prefect Flow to convert Pycytominer SQLite data
    to single parquet file with same data.

    Parameters
    ----------
    sql_engine: Union[str, Engine]:
        filename of the SQLite database or existing sqlalchemy engine
    flow_executor: Executor:
        Prefect flow executor
    flow_storage: Storage:
        Prefect flow storage
    sql_tbl_basis: str:  (Default value = "Image")
        Database table to use as the basis of building
        join keys and chunks
    sql_join_keys: List[str]:  (Default value = ["TableNumber","ImageNumber"]):
        Database column name keys to be used as for
        chunking and frame concatenation.
    sql_chunk_size: int:  (Default value = 10)
        Chunk size for unique join key datasets.
        Note: adjust to resource capabilities of
        machine and provided dataset. Smaller
        chunksizes may mean greater time duration
        and lower memory consumption
    pq_filename: str:  (Default value = "combined.parquet")
        Target parquet filename to be used for chunks
        and also resulting converted filename.

    Returns
    -------
    str
        Single parquet filename of file which contains
        all SQLite data based on the outcome of a
        Prefect flow.

    Examples
    --------

    .. code-block:: python

        from prefect.executors import LocalExecutor
        from prefect.storage import Local
        from sqlalchemy.engine import create_engine

        from pycytominer.cyto_utils.sqlite.convert import flow_convert_sqlite_to_parquet

        sql_path = "test_SQ00014613.sqlite"
        sql_url = f"sqlite:///{sql_path}"
        sql_engine = create_engine(sql_url)

        # note: encapsulate the following within a __main__ block for
        # dask compatibility if desired and set with executor

        result_file_path = flow_convert_sqlite_to_parquet(
            sql_engine=sql_engine,
            flow_executor=LocalExecutor(),
            flow_storage=Local(),
            sql_tbl_basis="Image",
            sql_join_keys=["TableNumber", "ImageNumber"],
            sql_chunk_size=10,
            pq_filename="test_SQ00014613.parquet",
        )

    """

    logger.info("Setting up Prefect flow for running SQLite to parquet conversion.")

    # cast the provided sql_engine to a string from sqlalchemy url if necessary
    if not isinstance(sql_engine, str):
        sql_engine = str(sql_engine.url)

    # build a prefect flow with explicit storage based on parameter
    with Flow("flow_convert_sqlite_to_parquet", storage=flow_storage) as flow:

        # set flow parameters
        param_sql_engine = Parameter("sql_engine", default="")
        param_sql_tbl_basis = Parameter("sql_tbl_basis", default=sql_tbl_basis)
        param_sql_join_keys = Parameter("sql_join_keys", default=sql_join_keys)
        param_sql_chunk_size = Parameter("sql_chunk_size", default=sql_chunk_size)
        param_pq_filename = Parameter("pq_filename", default=pq_filename)

        # form prefect tasks from sqlite meta utils
        task_collect_columns = task(collect_columns)

        # gather sql column and table data flow operations
        column_data = task_collect_columns(sql_engine=param_sql_engine)

        # chunk the dicts so as to create batches
        chunk_dicts = sql_select_distinct_join_chunks(
            sql_engine=param_sql_engine,
            table_name=param_sql_tbl_basis,
            join_keys=param_sql_join_keys,
            chunk_size=param_sql_chunk_size,
        )

        # map to gather our concatted/merged pd dataframes as a list within this flow
        pq_files = table_concat_to_parquet.map(
            sql_engine=unmapped(param_sql_engine),
            column_data=unmapped(column_data),
            prepend_tablename_to_cols=unmapped(True),
            avoid_prepend_for=unmapped(param_sql_join_keys),
            chunk_list_dicts=chunk_dicts,
            filename=unmapped(param_pq_filename),
        )

        # reduce to single pq "concatted" file from the map result list
        reduced_pq_result = multi_to_single_parquet(
            pq_files=pq_files, filename=param_pq_filename
        )

    # run the flow
    flow_state = flow.run(
        executor=flow_executor,
        parameters=dict(
            sql_engine=sql_engine,
            sql_tbl_basis=sql_tbl_basis,
            sql_join_keys=sql_join_keys,
            sql_chunk_size=sql_chunk_size,
            pq_filename=pq_filename,
        ),
    )

    # return the result of reduced_pq_result, a path to the new file
    return flow_state.result[reduced_pq_result].result
