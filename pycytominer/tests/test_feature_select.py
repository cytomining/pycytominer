import os
import random
import tempfile
import numpy as np
import pandas as pd
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
        compression="gzip",
    )
    expected_result = pd.DataFrame({"yy": [1, 2, 8, 10, 2, 100]})
    result = pd.read_csv(compress_file)

    pd.testing.assert_frame_equal(result, expected_result)


def test_feature_select_blacklist():
    """
    Testing feature_select and get_na_columns pycytominer function
    """

    data_blacklist_df = pd.DataFrame(
        {
            "Nuclei_Correlation_Manders_AGP_DNA": [1, 3, 8, 5, 2, 2],
            "y": [1, 2, 8, 5, 2, 1],
            "Nuclei_Correlation_RWC_ER_RNA": [9, 3, 8, 9, 2, 9],
            "zz": [0, -3, 8, 9, 6, 9],
        }
    ).reset_index(drop=True)

    result = feature_select(data_blacklist_df, features="infer", operation="blacklist")
    expected_result = pd.DataFrame({"y": [1, 2, 8, 5, 2, 1], "zz": [0, -3, 8, 9, 6, 9]})
    pd.testing.assert_frame_equal(result, expected_result)

    result = feature_select(
        data_blacklist_df,
        features=data_blacklist_df.columns.tolist(),
        operation="blacklist",
    )
    expected_result = pd.DataFrame({"y": [1, 2, 8, 5, 2, 1], "zz": [0, -3, 8, 9, 6, 9]})
    pd.testing.assert_frame_equal(result, expected_result)
