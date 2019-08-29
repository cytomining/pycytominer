"""
Utility function to compress output data
"""

import os
import warnings
import pandas as pd

compress_options = {"gzip": ".gz", "bz2": ".bz2", "zip": ".zip", "xz": ".xz", None: ""}


def output(df, output_filename, compression="gzip", float_format=None):
    """
    Given an output file and compression options, write file to disk

    Arguments:
    df - a pandas dataframe that will be written to file
    output_filename - a string or path object that stores location of file
    compression - the mechanism to compress [default: "gzip"]
    float_format - decimal precision to use in writing output file [default: None]
                   For example, use "%.3g" for 3 decimal precision.

    Return:
    Nothing, write df to file
    """

    # Extract suffixes from the provided output file name
    filename, output_file_extension = os.path.splitext(output_filename)
    basefilename, non_compression_suffix = os.path.splitext(filename)

    # if no additional suffix was provided, make it a csv
    if len(non_compression_suffix) == 0 and output_file_extension not in [
        ".csv",
        ".tsv",
    ]:
        output_filename = "{}.csv".format(output_filename)

    # Set the delimiter
    delim = ","
    if non_compression_suffix == ".tsv":
        delim = "\t"

    # Determine the compression suffix
    compression_suffix = infer_compression_suffix(compression=compression)
    if output_file_extension in compress_options.values():
        if output_file_extension != compression_suffix:
            warnings.warn(
                "The output file has a compression file extension ('{}') that is different than what is specified in 'compression' ('{}'). Defaulting to output filename suffix.".format(
                    output_file_extension, compression_suffix
                )
            )
        compression = "infer"
    else:
        output_filename = "{}{}".format(output_filename, compression_suffix)

    df.to_csv(
        path_or_buf=output_filename,
        sep=delim,
        index=False,
        float_format=float_format,
        compression=compression,
    )


def infer_compression_suffix(compression="gzip"):
    """
    Determine the compression suffix

    Arguments:
    compression - the mechanism to compress [default: "gzip"]
    """
    assert (
        compression in compress_options
    ), "{} is not supported, select one of {}".format(
        compression, list(compress_options.keys())
    )

    return compress_options[compression]
