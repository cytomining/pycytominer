"""
Utility function to compress output data
"""

import pathlib
from typing import Dict, Union, Literal

import pandas as pd
from pydantic import validate_call

from .warnings import alias_param

OutputType = Literal["csv", "parquet"]


@alias_param(param_name="compression", param_alias="compression_options")
@validate_call
def output(
    df: pd.DataFrame,
    output_filename: pathlib.Path,
    output_type: OutputType = "csv",
    **kwargs,
):
    """Given an output file and compression options, write file to disk.

    Note for csv output type the default compression options are {"method": "gzip", "mtime": 1}
    which differs from the default for pandas.DataFrame.to_csv which is None.

    Parameters
    ----------
    df :  pandas.DataFrame
        a pandas dataframe that will be written to file
    output_filename : str
        location of file to write
    output_type : str, default "csv"
        type of output file to create (csv or parquet)
    compression : str or dict, default {"method": "gzip", "mtime": 1}
        Contains compression options as input to
        pd.DataFrame.to_csv(compression=compression).
    kwargs :
        Additional keyword arguments to pass to pandas.DataFrame.to_csv or pandas.DataFrame.to_parquet

    Returns
    -------
    str
        returns output_filename

    Examples
    --------
    import pandas as pd
    from pycytominer.cyto_utils import output

    data_df = pd.concat(
        [
            pd.DataFrame(
                {
                    "Metadata_Plate": "X",
                    "Metadata_Well": "a",
                    "Cells_x": [0.1, 0.3, 0.8],
                    "Nuclei_y": [0.5, 0.3, 0.1],
                }
            ),
            pd.DataFrame(
                {
                    "Metadata_Plate": "X",
                    "Metadata_Well": "b",
                    "Cells_x": [0.4, 0.2, -0.5],
                    "Nuclei_y": [-0.8, 1.2, -0.5],
                }
            ),
        ]
    ).reset_index(drop=True)

    output_file = "test.csv.gz"
    output(
        df=data_df,
        output_filename=output_file,
        sep=",",
        compression={"method": "gzip", "mtime": 1},
        float_format=None,
    )
    """

    if output_type == "csv":
        # Use the compression options from the function signature if not provided
        compression = kwargs.pop("compression", {"method": "gzip", "mtime": 1})
        return _output_csv(
            df=df,
            output_filename=output_filename,
            compression=compression,
            **kwargs,
        )

    elif output_type == "parquet":
        return _output_parquet(
            df=df,
            output_filename=output_filename,
            **kwargs,
        )
    else:
        raise ValueError(f"Output type {output_type} is not supported.")


@validate_call
def _output_csv(
    df: pd.DataFrame,
    output_filename: pathlib.Path,
    compression: Union[str, Dict] = {"method": "gzip", "mtime": 1},
    **kwargs,
):
    """Given an output file and compression options, write a CSV file to disk."""

    df.to_csv(
        path_or_buf=output_filename,
        index=False,
        compression=compression,
        **kwargs,
    )

    return output_filename


@validate_call
def _output_parquet(
    df: pd.DataFrame,
    output_filename: pathlib.Path,
    **kwargs,
):
    """Given an output file and compression options, write a parquet file to disk."""

    df.to_parquet(
        path=output_filename,
        **kwargs,
    )

    return output_filename
