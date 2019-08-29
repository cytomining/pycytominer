"""
Utility function to compress output data
"""

import os
import warnings
import pandas as pd

compress_options = {"gzip": ".gz", "bz2": ".bz2", "zip": ".zip", "xz": ".xz", None: ""}


def compress(df, output_filename, how="gzip", float_format=None):
    """
    Given an input file and compression options, write file to disk

    Arguments:
    df - a pandas dataframe that will be written to file
    output_filename - a string or path object that stores location of file
    how - the mechanism to compress [default: "gzip"]
    float_format - decimal precision to use in writing output file [default: None]
                   For example, use "%.3g" for 3 decimal precision.

    Return:
    Nothing, write df to file
    """

    # Extract suffixes from the provided output file name
    filename, input_file_extension = os.path.splitext(output_filename)
    basefilename, non_compression_suffix = os.path.splitext(filename)

    # if no additional suffix was provided, make it a csv
    if len(non_compression_suffix) == 0:
        output_filename = "{}.csv".format(output_filename)

    # Set the delimiter
    delim = ","
    if non_compression_suffix == ".tsv":
        delim = "\t"

    # Determine the compression suffix
    suffix = infer_compression_suffix(how=how)
    if input_file_extension in compress_options.values():
        if input_file_extension != suffix:
            warnings.warn(
                "The input file has a compression file extension ('{}') that is different than what is specified in 'how' ('{}'). Defaulting to input filename suffix.".format(
                    input_file_extension, suffix
                )
            )
        how = "infer"
    else:
        output_filename = "{}{}".format(output_filename, suffix)

    df.to_csv(
        path_or_buf=output_filename,
        sep=delim,
        index=False,
        float_format=float_format,
        compression=how,
    )


def infer_compression_suffix(how="gzip"):
    """
    Determine the compression suffix

    Arguments:
    how - the mechanism to compress [default: "gzip"]
    """
    assert how in compress_options, "{} is not supported, select one of {}".format(
        how, list(compress_options.keys())
    )

    return compress_options[how]
