import os
import random
import pytest
import tempfile
import warnings
import pandas as pd
from pycytominer.cyto_utils.output import output, infer_compression_suffix

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


def test_compress():

    output_filename = os.path.join(tmpdir, "test_compress.csv")
    compression = "gzip"

    output(
        df=data_df,
        output_filename=output_filename,
        compression=compression,
        float_format=None,
    )
    result = pd.read_csv("{}.gz".format(output_filename))

    pd.testing.assert_frame_equal(
        result, data_df, check_names=False, check_less_precise=1
    )

    # Test input filename overwriting compression
    output_filename = os.path.join(tmpdir, "test_compress.csv.bz2")
    output(
        df=data_df,
        output_filename=output_filename,
        compression=compression,
        float_format=None,
    )

    result = pd.read_csv(output_filename)
    pd.testing.assert_frame_equal(
        result, data_df, check_names=False, check_less_precise=1
    )


def test_compress_no_csv():
    # Test the ability of a naked string input, appending csv.gz
    output_filename = os.path.join(tmpdir, "test_compress")
    compression = "gzip"

    output(
        df=data_df,
        output_filename=output_filename,
        compression=compression,
        float_format=None,
    )
    result = pd.read_csv("{}.csv.gz".format(output_filename))

    pd.testing.assert_frame_equal(
        result, data_df, check_names=False, check_less_precise=1
    )

    # Test input filename of writing a tab separated file
    output_filename = os.path.join(tmpdir, "test_compress.tsv.bz2")
    output(
        df=data_df,
        output_filename=output_filename,
        compression=compression,
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
        compression=compression,
        float_format=None,
    )

    result = pd.read_csv(output_filename)
    pd.testing.assert_frame_equal(
        result, data_df, check_names=False, check_less_precise=1
    )


def test_compress_warning():
    with pytest.warns(UserWarning) as w:
        warnings.simplefilter("always")

        output_filename = os.path.join(tmpdir, "test_compress_warning.csv.zip")
        compression = "gzip"
        output(
            df=data_df,
            output_filename=output_filename,
            compression=compression,
            float_format=None,
        )

        assert len(w) == 1
        assert issubclass(w[-1].category, UserWarning)

        result = pd.read_csv(output_filename)
        pd.testing.assert_frame_equal(
            result, data_df, check_names=False, check_less_precise=1
        )


def test_compress_exception():
    output_filename = os.path.join(tmpdir, "test_compress_warning.csv.zip")
    with pytest.raises(Exception) as e:
        output(df=data_df, output_filename=output_filename, compression="not an option")

    assert "not supported" in str(e.value)


def test_compress_suffix():

    suffix = infer_compression_suffix(compression="gzip")
    expected_result = ".gz"
    assert suffix == expected_result

    suffix = infer_compression_suffix(compression="bz2")
    expected_result = ".bz2"
    assert suffix == expected_result

    suffix = infer_compression_suffix(compression="zip")
    expected_result = ".zip"
    assert suffix == expected_result

    suffix = infer_compression_suffix(compression="xz")
    expected_result = ".xz"
    assert suffix == expected_result

    suffix = infer_compression_suffix(compression=None)
    assert suffix == ""

    with pytest.raises(AssertionError) as e:
        infer_compression_suffix(compression="THIS WILL NOT WORK")
    assert "not supported" in str(e.value)
