import os
import io
import time
import random
import pytest
import tempfile
import warnings
import pandas as pd
from pycytominer.cyto_utils.output import (
    output,
    check_compression_method,
    set_compression_method,
)

random.seed(123)

# Get temporary directory
tmpdir = tempfile.gettempdir()

# Build data to use in tests
data_df = pd.DataFrame(
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
compression_options = {"method": "gzip"}


def test_compress():

    output_filename = os.path.join(tmpdir, "test_compress.csv.gz")

    output(
        df=data_df,
        output_filename=output_filename,
        compression_options=compression_options,
        float_format=None,
    )
    result = pd.read_csv(output_filename)

    pd.testing.assert_frame_equal(
        result, data_df, check_names=False, check_less_precise=1
    )


def test_compress_tsv():
    # Test input filename of writing a tab separated file
    output_filename = os.path.join(tmpdir, "test_compress.tsv.gz")
    output(
        df=data_df,
        sep="\t",
        output_filename=output_filename,
        compression_options=compression_options,
        float_format=None,
    )

    result = pd.read_csv(output_filename, sep="\t")
    pd.testing.assert_frame_equal(
        result, data_df, check_names=False, check_less_precise=1
    )


def test_output_none():
    output_filename = os.path.join(tmpdir, "test_output_none.csv")
    compression = None
    output(
        df=data_df,
        output_filename=output_filename,
        compression_options=compression,
        float_format=None,
    )

    result = pd.read_csv(output_filename)
    pd.testing.assert_frame_equal(
        result, data_df, check_names=False, check_less_precise=1
    )


def test_compress_exception():
    output_filename = os.path.join(tmpdir, "test_compress_warning.csv.zip")
    with pytest.raises(Exception) as e:
        output(
            df=data_df,
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


def test_compress_no_timestamp():
    # The default behavior is to ignore timestamps
    buffer = io.BytesIO()

    output(
        df=data_df,
        output_filename=buffer,
        float_format=None,
    )

    buffer_output = buffer.getvalue()

    # Simulate different timestamp
    time.sleep(2)

    buffer = io.BytesIO()
    output(
        df=data_df,
        output_filename=buffer,
        float_format=None,
    )
    assert buffer_output == buffer.getvalue()

    # Simulate different time stamps
    buffer = io.BytesIO()
    output(
        df=data_df,
        output_filename=buffer,
        float_format=None,
        compression_options=compression_options,
    )
    buffer_output = buffer.getvalue()

    time.sleep(2)
    buffer = io.BytesIO()
    output(
        df=data_df,
        output_filename=buffer,
        float_format=None,
        compression_options=compression_options,
    )
    assert buffer_output != buffer.getvalue()
