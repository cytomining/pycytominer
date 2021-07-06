import os
import tempfile
import pytest
import numpy as np
import pandas as pd
from pycytominer import aggregate
from pycytominer.cyto_utils import infer_cp_features

# Get temporary directory
tmpdir = tempfile.gettempdir()

# Setup a testing file
test_output_file = os.path.join(tmpdir, "test.csv")

# Build data to use in tests
data_df = pd.concat(
    [
        pd.DataFrame(
            {
                "g": "a",
                "Metadata_ObjectNumber": [1, 2, 3],
                "Cells_x": [1, 3, 8],
                "Nuclei_y": [5, 3, 1],
            }
        ),
        pd.DataFrame(
            {
                "g": "b",
                "Metadata_ObjectNumber": [1, 2, 4],
                "Cells_x": [1, 3, 5],
                "Nuclei_y": [8, 3, 1],
            }
        ),
    ]
).reset_index(drop=True)

data_missing_df = pd.concat(
    [
        pd.DataFrame(
            {"g": "a", "Cells_x": [1, 3, 8, np.nan], "Nuclei_y": [5, np.nan, 3, 1]}
        ),
        pd.DataFrame(
            {"g": "b", "Cells_x": [1, 3, np.nan, 5], "Nuclei_y": [np.nan, 8, 3, 1]}
        ),
    ]
).reset_index(drop=True)

features = infer_cp_features(data_df)
dtype_convert_dict = {x: float for x in features}


def test_aggregate_median_allvar():
    """
    Testing aggregate pycytominer function
    """
    aggregate_result = aggregate(
        population_df=data_df, strata=["g"], features="infer", operation="median"
    )

    expected_result = pd.concat(
        [
            pd.DataFrame({"g": "a", "Cells_x": [3], "Nuclei_y": [3]}),
            pd.DataFrame({"g": "b", "Cells_x": [3], "Nuclei_y": [3]}),
        ]
    ).reset_index(drop=True)
    expected_result = expected_result.astype(dtype_convert_dict)

    assert aggregate_result.equals(expected_result)

    # Test output
    aggregate(
        population_df=data_df,
        strata=["g"],
        features="infer",
        operation="median",
        output_file=test_output_file,
    )

    test_df = pd.read_csv(test_output_file)
    pd.testing.assert_frame_equal(test_df, expected_result)


def test_aggregate_mean_allvar():
    """
    Testing aggregate pycytominer function
    """
    aggregate_result = aggregate(
        population_df=data_df, strata=["g"], features="infer", operation="mean"
    )

    expected_result = pd.concat(
        [
            pd.DataFrame({"g": "a", "Cells_x": [4], "Nuclei_y": [3]}),
            pd.DataFrame({"g": "b", "Cells_x": [3], "Nuclei_y": [4]}),
        ]
    ).reset_index(drop=True)
    expected_result = expected_result.astype(dtype_convert_dict)

    assert aggregate_result.equals(expected_result)


def test_aggregate_median_subsetvar():
    """
    Testing aggregate pycytominer function
    """
    aggregate_result = aggregate(
        population_df=data_df, strata=["g"], features=["Cells_x"], operation="median"
    )

    expected_result = pd.DataFrame({"g": ["a", "b"], "Cells_x": [3, 3]})
    expected_result.Cells_x = expected_result.Cells_x.astype(float)

    assert aggregate_result.equals(expected_result)


def test_aggregate_mean_subsetvar():
    """
    Testing aggregate pycytominer function
    """
    aggregate_result = aggregate(
        population_df=data_df, strata=["g"], features=["Cells_x"], operation="mean"
    )

    expected_result = pd.DataFrame({"g": ["a", "b"], "Cells_x": [4, 3]})
    expected_result.Cells_x = expected_result.Cells_x.astype(float)

    assert aggregate_result.equals(expected_result)


def test_aggregate_median_dtype_confirm():
    """
    Testing aggregate pycytominer function
    """

    # Convert dtype of one variable to object
    data_dtype_df = data_df.copy()
    data_dtype_df.Cells_x = data_dtype_df.Cells_x.astype(str)

    aggregate_result = aggregate(
        population_df=data_dtype_df, strata=["g"], features="infer", operation="median"
    )
    print(aggregate_result)
    expected_result = pd.concat(
        [
            pd.DataFrame({"g": "a", "Cells_x": [3], "Nuclei_y": [3]}),
            pd.DataFrame({"g": "b", "Cells_x": [3], "Nuclei_y": [3]}),
        ]
    ).reset_index(drop=True)
    expected_result = expected_result.astype(dtype_convert_dict)

    assert aggregate_result.equals(expected_result)


def test_aggregate_median_with_missing_values():
    """
    Testing aggregate pycytominer function
    """

    # Convert dtype of one variable to object
    data_dtype_df = data_missing_df.copy()
    data_dtype_df.Cells_x = data_dtype_df.Cells_x.astype(str)

    aggregate_result = aggregate(
        population_df=data_dtype_df, strata=["g"], features="infer", operation="median"
    )
    print(aggregate_result)
    expected_result = pd.concat(
        [
            pd.DataFrame({"g": "a", "Cells_x": [3], "Nuclei_y": [3]}),
            pd.DataFrame({"g": "b", "Cells_x": [3], "Nuclei_y": [3]}),
        ]
    ).reset_index(drop=True)
    expected_result = expected_result.astype(dtype_convert_dict)

    assert aggregate_result.equals(expected_result)


def test_aggregate_compute_object_count():
    """
    Testing aggregate pycytominer function
    """

    aggregate_result = aggregate(
        population_df=data_df,
        strata=["g"],
        features="infer",
        operation="median",
        compute_object_count=True,
    )

    expected_result = pd.concat(
        [
            pd.DataFrame(
                {
                    "g": "a",
                    "Metadata_Object_Count": [3],
                    "Cells_x": [3],
                    "Nuclei_y": [3],
                }
            ),
            pd.DataFrame(
                {
                    "g": "b",
                    "Metadata_Object_Count": [3],
                    "Cells_x": [3],
                    "Nuclei_y": [3],
                }
            ),
        ]
    ).reset_index(drop=True)
    expected_result = expected_result.astype(dtype_convert_dict)

    assert aggregate_result.equals(expected_result)

    # Test output
    aggregate(
        population_df=data_df,
        strata=["g"],
        features="infer",
        operation="median",
        compute_object_count=True,
        output_file=test_output_file,
    )

    test_df = pd.read_csv(test_output_file)
    pd.testing.assert_frame_equal(test_df, expected_result)


def test_aggregate_incorrect_object_feature():
    """
    Testing aggregate pycytominer function
    """

    incorrect_object_feature = "DOES NOT EXIST"

    with pytest.raises(KeyError) as err:
        aggregate_result = aggregate(
            population_df=data_df,
            strata=["g"],
            features="infer",
            operation="median",
            compute_object_count=True,
            object_feature=incorrect_object_feature,
        )

        assert (
            f"The following labels were missing: Index(['{incorrect_object_feature}'], dtype='object')"
            in str(err)
        )

    # Test that aggregate doesn't drop samples if strata is na
    data_missing_group_df = pd.concat(
        [
            data_df,
            pd.DataFrame({"g": np.nan, "Cells_x": [1, 3, 8], "Nuclei_y": [5, 3, 1]}),
        ]
    )

    result = aggregate(
        population_df=data_missing_group_df,
        strata=["g"],
        features="infer",
        operation="median",
    )
    # There should be three total groups
    assert result.shape[0] == 3


def test_custom_objectnumber_feature():
    """
    Testing aggregate pycytominer function
    """

    data_df_copy = data_df.copy().rename(
        columns={"Metadata_ObjectNumber": "Custom_ObjectNumber_Feature"}
    )

    aggregate_result = aggregate(
        population_df=data_df_copy,
        strata=["g"],
        features="infer",
        operation="median",
        compute_object_count=True,
        object_feature="Custom_ObjectNumber_Feature",
    )

    expected_result = pd.concat(
        [
            pd.DataFrame(
                {
                    "g": "a",
                    "Metadata_Object_Count": [3],
                    "Cells_x": [3],
                    "Nuclei_y": [3],
                }
            ),
            pd.DataFrame(
                {
                    "g": "b",
                    "Metadata_Object_Count": [3],
                    "Cells_x": [3],
                    "Nuclei_y": [3],
                }
            ),
        ]
    ).reset_index(drop=True)
    expected_result = expected_result.astype(dtype_convert_dict)

    assert aggregate_result.equals(expected_result)
