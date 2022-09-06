import io
import os
import pathlib
import random
import tempfile
import time

import pandas as pd
import pytest
from pycytominer.cyto_utils.output import (
    check_compression_method,
    output,
    set_compression_method,
)

random.seed(123)

# Get temporary directory
TMPDIR = tempfile.gettempdir()

# Build data to use in tests
DATA_DF = pd.DataFrame(
    {
        "Metadata_plate": ["a", "a", "a", "a", "b", "b", "b", "b"],
        "Metadata_treatment": [
            "drug",
            "drug",
            "control",
            "control",
            "drug",
            "drug",
            "control",
            "control",
        ],
        "x": [1, 2, 8, 2, 5, 5, 5, 1],
        "y": [3, 1, 7, 4, 5, 9, 6, 1],
        "z": [1, 8, 2, 5, 6, 22, 2, 2],
        "zz": [14, 46, 1, 6, 30, 100, 2, 2],
    }
).reset_index(drop=True)

# Set default compression options
TEST_COMPRESSION_OPTIONS = {"method": "gzip"}


def test_output_default():

    output_filename = pathlib.Path(f"{TMPDIR}/test_compress.csv.gz")

    output_result = output(
        df=DATA_DF,
        output_filename=output_filename,
        compression_options=TEST_COMPRESSION_OPTIONS,
        float_format=None,
    )
    result = pd.read_csv(output_result)

    pd.testing.assert_frame_equal(
        result, DATA_DF, check_names=False, check_exact=False, atol=1e-3
    )


def test_output_tsv():
    # Test input filename of writing a tab separated file
    output_filename = pathlib.Path(f"{TMPDIR}/test_compress.tsv.gz")
    output_result = output(
        df=DATA_DF,
        sep="\t",
        output_filename=output_filename,
        compression_options=TEST_COMPRESSION_OPTIONS,
        float_format=None,
    )

    result = pd.read_csv(output_result, sep="\t")
    pd.testing.assert_frame_equal(
        result, DATA_DF, check_names=False, check_exact=False, atol=1e-3
    )


def test_output_parquet():
    """
    Tests using output function with parquet type
    """

    output_filename = pathlib.Path(f"{TMPDIR}/test_output.parquet")

    # test with base output arguments and
    # kwargs output arguments for pd.DataFrame.to_parquet
    output_result = output(
        df=DATA_DF,
        output_filename=output_filename,
        output_type="parquet",
    )
    result = pd.read_parquet(output_result)

    pd.testing.assert_frame_equal(
        result, DATA_DF, check_names=False, check_exact=False, atol=1e-3
    )


def test_output_none():
    output_filename = pathlib.Path(f"{TMPDIR}/test_output_none.csv")
    compression = None
    output(
        df=DATA_DF,
        output_filename=output_filename,
        compression_options=compression,
        float_format=None,
    )

    result = pd.read_csv(output_filename)
    pd.testing.assert_frame_equal(
        result, DATA_DF, check_names=False, check_exact=False, atol=1e-3
    )


def test_output_exception():
    output_filename = pathlib.Path(f"{TMPDIR}/test_compress_warning.csv.zip")
    with pytest.raises(Exception) as e:
        output(
            df=DATA_DF,
            output_filename=output_filename,
            compression_options="not an option",
        )

    assert "not supported" in str(e.value)


def test_check_set_compression():

    check_compression_method(compression="gzip")

    with pytest.raises(AssertionError) as e:
        check_compression_method(compression="THIS WILL NOT WORK")
    assert "not supported" in str(e.value)

    compression = set_compression_method(compression="gzip")
    assert compression == {"method": "gzip"}

    compression = set_compression_method(compression=None)
    assert compression == {"method": None}

    compression = set_compression_method(compression={"method": "gzip", "mtime": 1})
    assert compression == {"method": "gzip", "mtime": 1}

    compression = set_compression_method(compression={"method": None, "mtime": 1})
    assert compression == {"method": None, "mtime": 1}

    with pytest.raises(AssertionError) as e:
        compression = set_compression_method(compression="THIS WILL NOT WORK")
    assert "not supported" in str(e.value)

    with pytest.raises(AssertionError) as e:
        compression = set_compression_method(
            compression={"method": "THIS WILL NOT WORK"}
        )
    assert "not supported" in str(e.value)


def test_output_no_timestamp():
    # The default behavior is to ignore timestamps
    buffer = io.BytesIO()

    output(
        df=DATA_DF,
        output_filename=buffer,
        float_format=None,
    )

    buffer_output = buffer.getvalue()

    # Simulate different timestamp
    time.sleep(2)

    buffer = io.BytesIO()
    output(
        df=DATA_DF,
        output_filename=buffer,
        float_format=None,
    )
    assert buffer_output == buffer.getvalue()

    # Simulate different time stamps
    buffer = io.BytesIO()
    output(
        df=DATA_DF,
        output_filename=buffer,
        float_format=None,
        compression_options=TEST_COMPRESSION_OPTIONS,
    )
    buffer_output = buffer.getvalue()

    time.sleep(2)
    buffer = io.BytesIO()
    output(
        df=DATA_DF,
        output_filename=buffer,
        float_format=None,
        compression_options=TEST_COMPRESSION_OPTIONS,
    )
    assert buffer_output != buffer.getvalue()
