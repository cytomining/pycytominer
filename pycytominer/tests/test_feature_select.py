import os
import random
import tempfile
import numpy as np
import pandas as pd
import pytest
from pycytominer.feature_select import feature_select

random.seed(123)

# Get temporary directory
tmpdir = tempfile.gettempdir()

data_df = pd.DataFrame(
    {
        "x": [1, 3, 8, 5, 2, 2],
        "y": [1, 2, 8, 5, 2, 1],
        "z": [9, 3, 8, 9, 2, 9],
        "zz": [0, -3, 8, 9, 6, 9],
    }
).reset_index(drop=True)

data_na_df = pd.DataFrame(
    {
        "x": [np.nan, 3, 8, 5, 2, 2],
        "xx": [np.nan, 3, 8, 5, 2, 2],
        "y": [1, 2, 8, np.nan, 2, np.nan],
        "yy": [1, 2, 8, 10, 2, 100],
        "z": [9, 3, 8, 9, 2, np.nan],
        "zz": [np.nan, np.nan, 8, np.nan, 6, 9],
    }
).reset_index(drop=True)

data_feature_infer_df = pd.DataFrame(
    {
        "Metadata_x": [np.nan, np.nan, 8, np.nan, 2, np.nan],
        "Cytoplasm_xx": [np.nan, 3, 8, 5, 2, 2],
        "Nuclei_y": [1, 2, 8, np.nan, 2, np.nan],
        "Nuclei_yy": [1, 2, 8, 10, 2, 100],
        "Cytoplasm_z": [9, 3, 8, 9, 2, np.nan],
        "Cells_zz": [np.nan, np.nan, 8, np.nan, 6, 9],
    }
).reset_index(drop=True)

a_feature = [1] * 99 + [2]
b_feature = [1, 2] * 50
c_feature = [1, 2] * 25 + random.sample(range(1, 1000), 50)
d_feature = random.sample(range(1, 1000), 100)

data_unique_test_df = pd.DataFrame(
    {"a": a_feature, "b": b_feature, "c": c_feature, "d": d_feature}
).reset_index(drop=True)

data_outlier_df = pd.DataFrame(
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
        "Cells_x": [1, 2, -8, 2, 5, 5, 5, -1],
        "Cytoplasm_y": [3, -1, 7, 4, 5, -9, 6, 1],
        "Nuclei_z": [-1, 8, 2, 5, -6, 20, 2, -2],
        "Cells_zz": [14, -46, 1, 60, -30, -100, 2, 2],
    }
).reset_index(drop=True)


def test_feature_select_noise_removal():
    """
    Testing noise_removal feature selection operation
    """
    # Set perturbation groups for the test dataframes
    data_df_groups = ["a", "a", "a", "b", "b", "b"]

    # Tests on data_df
    result1 = feature_select(
        profiles=data_df,
        features=data_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups=data_df_groups,
        noise_removal_stdev_cutoff=2.5,
    )
    result2 = feature_select(
        profiles=data_df,
        features=data_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups=data_df_groups,
        noise_removal_stdev_cutoff=2,
    )
    result3 = feature_select(
        profiles=data_df,
        features=data_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups=data_df_groups,
        noise_removal_stdev_cutoff=3.5,
    )
    expected_result1 = data_df[["x", "y"]]
    expected_result2 = data_df[[]]
    expected_result3 = data_df[["x", "y", "z", "zz"]]
    pd.testing.assert_frame_equal(result1, expected_result1)
    pd.testing.assert_frame_equal(result2, expected_result2)
    pd.testing.assert_frame_equal(result3, expected_result3)

    # Test on data_unique_test_df, which has 100 rows
    data_unique_test_df_groups = []
    # Create a 100 element list containing 10 replicates of 10 perturbations
    for elem in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]:
        data_unique_test_df_groups.append([elem] * 10)
    # Unstack so it's just a single list
    data_unique_test_df_groups = [
        item for sublist in data_unique_test_df_groups for item in sublist
    ]

    result4 = feature_select(
        profiles=data_unique_test_df,
        features=data_unique_test_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups=data_unique_test_df_groups,
        noise_removal_stdev_cutoff=3.5,
    )
    result5 = feature_select(
        profiles=data_unique_test_df,
        features=data_unique_test_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups=data_unique_test_df_groups,
        noise_removal_stdev_cutoff=500,
    )
    expected_result4 = data_unique_test_df[["a", "b"]]
    expected_result5 = data_unique_test_df[["a", "b", "c", "d"]]
    pd.testing.assert_frame_equal(result4, expected_result4)
    pd.testing.assert_frame_equal(result5, expected_result5)

    # Test the same as above, except that data_unique_test_df_groups is now made into a metadata column
    data_unique_test_df2 = data_unique_test_df.copy()
    data_unique_test_df2["perturb_group"] = data_unique_test_df_groups
    result4b = feature_select(
        profiles=data_unique_test_df2,
        features=data_unique_test_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups="perturb_group",
        noise_removal_stdev_cutoff=3.5,
    )
    result5b = feature_select(
        profiles=data_unique_test_df2,
        features=data_unique_test_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups="perturb_group",
        noise_removal_stdev_cutoff=500,
    )
    expected_result4b = data_unique_test_df2[["a", "b", "perturb_group"]]
    expected_result5b = data_unique_test_df2[["a", "b", "c", "d", "perturb_group"]]
    pd.testing.assert_frame_equal(result4b, expected_result4b)
    pd.testing.assert_frame_equal(result5b, expected_result5b)

    # Test assertion errors for the user inputting the perturbation groupings
    bad_perturb_list = ["a", "a", "b", "b", "a", "a", "b"]
    with pytest.raises(
        AssertionError
    ):  # When the inputted perturb list doesn't match the length of the data
        feature_select(
            data_df,
            features=data_df.columns.tolist(),
            operation="noise_removal",
            noise_removal_perturb_groups=bad_perturb_list,
            noise_removal_stdev_cutoff=3,
        )

    with pytest.raises(
        AssertionError
    ):  # When the perturb list is inputted as string, but there is no such metadata column in the population_df
        feature_select(
            profiles=data_df,
            features=data_df.columns.tolist(),
            operation="noise_removal",
            noise_removal_perturb_groups="bad_string",
            noise_removal_stdev_cutoff=2.5,
        )

    with pytest.raises(
        TypeError
    ):  # When the perturbation groups are not either a list or metadata column string
        feature_select(
            profiles=data_df,
            features=data_df.columns.tolist(),
            operation="noise_removal",
            noise_removal_perturb_groups=12345,
            noise_removal_stdev_cutoff=2.5,
        )


def test_feature_select_get_na_columns():
    """
    Testing feature_select and get_na_columns pycytominer function
    """
    result = feature_select(
        data_na_df, features=data_na_df.columns.tolist(), operation="drop_na_columns"
    )
    expected_result = pd.DataFrame({"yy": [1, 2, 8, 10, 2, 100]})
    pd.testing.assert_frame_equal(result, expected_result)

    result = feature_select(
        data_na_df,
        features=data_na_df.columns.tolist(),
        operation="drop_na_columns",
        na_cutoff=1,
    )
    pd.testing.assert_frame_equal(result, data_na_df)

    result = feature_select(
        data_na_df,
        features=data_na_df.columns.tolist(),
        operation="drop_na_columns",
        na_cutoff=0.3,
    )
    expected_result = pd.DataFrame(
        {
            "x": [np.nan, 3, 8, 5, 2, 2],
            "xx": [np.nan, 3, 8, 5, 2, 2],
            "yy": [1, 2, 8, 10, 2, 100],
            "z": [9, 3, 8, 9, 2, np.nan],
        }
    )
    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_get_na_columns_feature_infer():
    """
    Testing feature_select and get_na_columns pycytominer function
    """
    result = feature_select(
        data_feature_infer_df,
        features="infer",
        operation="drop_na_columns",
        na_cutoff=0.3,
    )
    expected_result = pd.DataFrame(
        {
            "Metadata_x": [np.nan, np.nan, 8, np.nan, 2, np.nan],
            "Cytoplasm_xx": [np.nan, 3, 8, 5, 2, 2],
            "Nuclei_yy": [1, 2, 8, 10, 2, 100],
            "Cytoplasm_z": [9, 3, 8, 9, 2, np.nan],
        }
    )
    pd.testing.assert_frame_equal(result, expected_result)

    result = feature_select(
        data_feature_infer_df,
        features=data_feature_infer_df.columns.tolist(),
        operation="drop_na_columns",
        na_cutoff=0.3,
    )
    expected_result = pd.DataFrame(
        {
            "Cytoplasm_xx": [np.nan, 3, 8, 5, 2, 2],
            "Nuclei_yy": [1, 2, 8, 10, 2, 100],
            "Cytoplasm_z": [9, 3, 8, 9, 2, np.nan],
        }
    )
    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_variance_threshold():
    """
    Testing feature_select and variance_threshold pycytominer function
    """
    result = feature_select(
        data_unique_test_df,
        features=data_unique_test_df.columns.tolist(),
        operation="variance_threshold",
        unique_cut=0.01,
    )
    expected_result = pd.DataFrame(
        {"b": b_feature, "c": c_feature, "d": d_feature}
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, expected_result)

    na_data_unique_test_df = data_unique_test_df.copy()
    na_data_unique_test_df.iloc[[x for x in range(0, 50)], 1] = np.nan
    result = feature_select(
        na_data_unique_test_df,
        features=na_data_unique_test_df.columns.tolist(),
        operation=["drop_na_columns", "variance_threshold"],
    )
    expected_result = pd.DataFrame({"c": c_feature, "d": d_feature}).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(result, expected_result)

    na_data_unique_test_df = data_unique_test_df.copy()
    na_data_unique_test_df.iloc[[x for x in range(0, 50)], 1] = np.nan

    result = feature_select(
        na_data_unique_test_df,
        features=na_data_unique_test_df.columns.tolist(),
        operation=["variance_threshold", "drop_na_columns"],
    )
    expected_result = pd.DataFrame({"c": c_feature, "d": d_feature}).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_correlation_threshold():
    """
    Testing feature_select and correlation_threshold pycytominer function
    """

    result = feature_select(
        data_df, features=data_df.columns.tolist(), operation="correlation_threshold"
    )
    expected_result = data_df.drop(["y"], axis="columns")
    pd.testing.assert_frame_equal(result, expected_result)

    data_cor_thresh_na_df = data_df.copy()
    data_cor_thresh_na_df.iloc[0, 2] = np.nan

    result = feature_select(
        data_cor_thresh_na_df,
        features=data_cor_thresh_na_df.columns.tolist(),
        operation=["drop_na_columns", "correlation_threshold"],
    )
    expected_result = data_df.drop(["z", "x"], axis="columns")
    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_all():
    data_all_test_df = data_unique_test_df.assign(zz=a_feature)
    data_all_test_df.iloc[1, 4] = 2
    data_all_test_df.iloc[[x for x in range(0, 50)], 1] = np.nan

    result = feature_select(
        profiles=data_all_test_df,
        features=data_all_test_df.columns.tolist(),
        operation=["drop_na_columns", "correlation_threshold"],
        corr_threshold=0.7,
    )
    expected_result = pd.DataFrame(
        {"c": c_feature, "d": d_feature, "zz": a_feature}
    ).reset_index(drop=True)
    expected_result.iloc[1, 2] = 2
    pd.testing.assert_frame_equal(result, expected_result)

    # Get temporary directory
    tmpdir = tempfile.gettempdir()

    # Write file to output
    data_file = os.path.join(tmpdir, "test_feature_select.csv")
    data_all_test_df.to_csv(data_file, index=False, sep=",")
    out_file = os.path.join(tmpdir, "test_feature_select_out.csv")
    _ = feature_select(
        profiles=data_file,
        features=data_all_test_df.columns.tolist(),
        operation=["drop_na_columns", "correlation_threshold"],
        corr_threshold=0.7,
        output_file=out_file,
    )
    from_file_result = pd.read_csv(out_file)
    pd.testing.assert_frame_equal(from_file_result, expected_result)

    result = feature_select(
        profiles=data_all_test_df,
        features=data_all_test_df.columns.tolist(),
        operation=["drop_na_columns", "correlation_threshold", "variance_threshold"],
        corr_threshold=0.7,
    )
    expected_result = pd.DataFrame({"c": c_feature, "d": d_feature}).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_compress():
    compress_file = os.path.join(tmpdir, "test_feature_select_compress.csv.gz")
    _ = feature_select(
        data_na_df,
        features=data_na_df.columns.tolist(),
        operation="drop_na_columns",
        output_file=compress_file,
        compression_options={"method": "gzip"},
    )
    expected_result = pd.DataFrame({"yy": [1, 2, 8, 10, 2, 100]})
    result = pd.read_csv(compress_file)

    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_blocklist():
    """
    Testing feature_select and get_na_columns pycytominer function
    """

    data_blocklist_df = pd.DataFrame(
        {
            "Nuclei_Correlation_Manders_AGP_DNA": [1, 3, 8, 5, 2, 2],
            "y": [1, 2, 8, 5, 2, 1],
            "Nuclei_Correlation_RWC_ER_RNA": [9, 3, 8, 9, 2, 9],
            "zz": [0, -3, 8, 9, 6, 9],
        }
    ).reset_index(drop=True)

    result = feature_select(data_blocklist_df, features="infer", operation="blocklist")
    expected_result = pd.DataFrame({"y": [1, 2, 8, 5, 2, 1], "zz": [0, -3, 8, 9, 6, 9]})
    pd.testing.assert_frame_equal(result, expected_result)

    result = feature_select(
        data_blocklist_df,
        features=data_blocklist_df.columns.tolist(),
        operation="blocklist",
    )
    expected_result = pd.DataFrame({"y": [1, 2, 8, 5, 2, 1], "zz": [0, -3, 8, 9, 6, 9]})
    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_drop_outlier():
    """
    Testing feature_select and get_na_columns pycytominer function
    """
    result = feature_select(
        data_outlier_df, features="infer", operation="drop_outliers"
    )
    expected_result = data_outlier_df.drop(["Cells_zz", "Nuclei_z"], axis="columns")
    pd.testing.assert_frame_equal(result, expected_result)

    result = feature_select(
        data_outlier_df, features="infer", operation="drop_outliers", outlier_cutoff=30
    )
    expected_result = data_outlier_df.drop(["Cells_zz"], axis="columns")
    pd.testing.assert_frame_equal(result, expected_result)

    result = feature_select(
        data_outlier_df, features=["Cells_x", "Cytoplasm_y"], operation="drop_outliers"
    )
    pd.testing.assert_frame_equal(result, data_outlier_df)
