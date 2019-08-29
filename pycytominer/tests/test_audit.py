import os
import random
import pytest
import tempfile
import pandas as pd
from pycytominer.audit import audit

random.seed(123)

# Get temporary directory
tmpdir = tempfile.gettempdir()

# Output file
output_audit_file = os.path.join(tmpdir, "test_audit.csv")

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

data_df.to_csv(output_audit_file, index=False, sep=",")

append_data_df = pd.DataFrame(
    {
        "Metadata_plate": ["c"] * 4,
        "Metadata_treatment": ["drop_this"] * 4,
        "x": [1, 2, 8, 2],
        "y": [3, 1, 7, 4],
        "z": [1, 8, 2, 5],
        "zz": [14, 46, 1, 6],
    }
).reset_index(drop=True)

append_data_df = pd.concat([data_df, append_data_df]).reset_index(drop=True)
append_data_df


def test_audit():
    result = audit(
        profiles=data_df,
        operation="replicate_quality",
        groups=["Metadata_treatment"],
        cor_method="pearson",
        quantile=0.95,
        output_file="none",
        samples="all",
        iterations=500,
    ).round(2)

    from_file_result = audit(
        profiles=output_audit_file,
        operation="replicate_quality",
        groups=["Metadata_treatment"],
        cor_method="pearson",
        quantile=0.95,
        output_file="none",
        samples="all",
        iterations=500,
    ).round(2)

    expected_result = pd.DataFrame(
        {
            "Metadata_treatment": ["control", "drug", "control", "drug"],
            "correlation": [-0.83, 0.99, 0.87, 0.86],
            "replicate_type": [
                "replicate",
                "replicate",
                "non_replicate",
                "non_replicate",
            ],
        }
    ).assign(
        quantile=0.95,
        iterations=500,
        cor_method="pearson",
        samples="all",
        groups=",".join(["Metadata_treatment"]),
    )

    pd.testing.assert_frame_equal(
        result, expected_result, check_names=False, check_less_precise=1
    )

    pd.testing.assert_frame_equal(
        from_file_result, expected_result, check_names=False, check_less_precise=1
    )


def test_audit_assertion_quantile_input():
    with pytest.raises(AssertionError):
        result = audit(
            profiles=data_df,
            operation="replicate_quality",
            groups=["Metadata_treatment"],
            cor_method="pearson",
            quantile=1.1,
            output_file="none",
            samples="all",
            iterations=500,
        )


def test_audit_assertion_groups_input():
    with pytest.raises(AssertionError):
        result = audit(
            profiles=data_df,
            operation="replicate_quality",
            groups=["Metadata_treatment", "Missing_Value"],
            cor_method="pearson",
            output_file="none",
            samples="all",
            iterations=500,
        )


def test_audit_assertion_operations_input():
    with pytest.raises(AssertionError):
        result = audit(
            profiles=data_df,
            operation="something_that_is_not_valid",
            groups=["Metadata_treatment"],
            cor_method="pearson",
            output_file="none",
            samples="all",
            iterations=500,
        )


def test_audit_subset_sample():
    subset_sample_string = "Metadata_treatment != 'drop_this'"
    result = audit(
        profiles=data_df,
        operation="replicate_quality",
        groups=["Metadata_treatment"],
        cor_method="pearson",
        quantile=0.95,
        output_file="none",
        samples=subset_sample_string,
        iterations=500,
    ).round(2)

    expected_result = pd.DataFrame(
        {
            "Metadata_treatment": ["control", "drug", "control", "drug"],
            "correlation": [-0.83, 0.99, 0.87, 0.86],
            "replicate_type": [
                "replicate",
                "replicate",
                "non_replicate",
                "non_replicate",
            ],
        }
    ).assign(
        quantile=0.95,
        iterations=500,
        cor_method="pearson",
        samples=subset_sample_string,
        groups=",".join(["Metadata_treatment"]),
    )


def test_audit_compress():
    compress_file = os.path.join(tmpdir, "test_audit_compress.csv.gz")
    subset_sample_string = "Metadata_treatment != 'drop_this'"
    _ = audit(
        profiles=data_df,
        operation="replicate_quality",
        groups=["Metadata_treatment"],
        cor_method="pearson",
        quantile=0.95,
        output_file=compress_file,
        samples=subset_sample_string,
        iterations=500,
        compression="gzip",
    )

    result = audit(
        profiles=data_df,
        operation="replicate_quality",
        groups=["Metadata_treatment"],
        cor_method="pearson",
        quantile=0.95,
        output_file="none",
        samples=subset_sample_string,
        iterations=500,
    ).round(2)

    expected_result = pd.read_csv(compress_file).round(2)
    pd.testing.assert_frame_equal(
        result, expected_result, check_names=False, check_less_precise=1
    )
