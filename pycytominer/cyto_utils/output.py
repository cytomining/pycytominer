"""
Utility function to compress output data
"""

import os
import warnings
import pandas as pd

compress_options = ["gzip", None]


def output(
    df,
    output_filename,
    sep=",",
    float_format=None,
    compression_options={"method": "gzip", "mtime": 1},
):
    """Given an output file and compression options, write file to disk

    :param df: a pandas dataframe that will be written to file
    :type df: pandas.DataFrame
    :param output_filename: a string or path object that stores location of file
    :type output_filename: str
    :param sep: file delimiter
    :type sep: str
    :param float_format: decimal precision to use in writing output file [default: None]
    :type float_format: str
    :param compression_options: compression arguments as input to pandas.to_csv() [default: check different function call]
    :type compression_options: str, dict

    :Example:

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
        compression_options={"method": "gzip", "mtime": 1},
        float_format=None,
    )
    """
    # Make sure the compression method is supported
    compression_options = set_compression_method(compression=compression_options)

    df.to_csv(
        path_or_buf=output_filename,
        sep=sep,
        index=False,
        float_format=float_format,
        compression=compression_options,
    )


def set_compression_method(compression):
    """Set the compression options

    :param compression: indicating compression options
    :type compression: str, dict
    """

    if compression is None:
        compression = {"method": None}

    if isinstance(compression, str):
        compression = {"method": compression}

    check_compression_method(compression["method"])
    return compression


def check_compression_method(compression):
    """Ensure compression options are set properly

    :param compression: the compression used to output data
    :type compression: str
    """
    assert (
        compression in compress_options
    ), "{} is not supported, select one of {}".format(compression, compress_options)
