import os
import tempfile
import random
import numpy as np
import pandas as pd
from pycytominer.normalize import normalize

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

data_file = os.path.join(tmpdir, "test_normalize.csv")
data_df.to_csv(data_file, index=False, sep=",")

data_feature_infer_df = pd.DataFrame(
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
        "Cells_x": [1, 2, 8, 2, 5, 5, 5, 1],
        "Cells_y": [3, 1, 7, 4, 5, 9, 6, 1],
        "Cytoplasm_z": [1, 8, 2, 5, 6, 22, 2, 2],
        "Nuclei_zz": [14, 46, 1, 6, 30, 100, 2, 2],
    }
).reset_index(drop=True)

data_feature_infer_file = os.path.join(tmpdir, "test_normalize_infer.csv")
data_feature_infer_df.to_csv(data_feature_infer_file, index=False, sep=",")

a_feature = random.sample(range(1, 100), 10)
b_feature = random.sample(range(1, 100), 10)
c_feature = random.sample(range(1, 100), 10)
d_feature = random.sample(range(1, 100), 10)
id_feature = ["control"] * 5 + ["treatment"] * 5

data_spherize_df = pd.DataFrame(
    {"a": a_feature, "b": b_feature, "c": c_feature, "d": d_feature, "id": id_feature}
).reset_index(drop=True)

data_no_var_df = pd.concat(
    [data_df, pd.DataFrame([1] * data_df.shape[0], columns=["yy"])], axis="columns"
)


def test_normalize_standardize_allsamples():
    """
    Testing normalize pycytominer function
    method = "standardize"
    meta_features = "none"
    samples="all"
    """
    normalize_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="standardize",
    ).round(1)

    expected_result = pd.DataFrame(
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
            "x": [-1.1, -0.7, 1.9, -0.7, 0.6, 0.6, 0.6, -1.1],
            "y": [-0.6, -1.3, 0.9, -0.2, 0.2, 1.7, 0.6, -1.3],
            "z": [-0.8, 0.3, -0.6, -0.2, 0.0, 2.5, -0.6, -0.6],
            "zz": [-0.3, 0.7, -0.8, -0.6, 0.2, 2.3, -0.7, -0.7],
        }
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_standardize_ctrlsamples():
    """
    Testing normalize pycytominer function
    method = "standardize"
    meta_features = "none"
    samples="Metadata_treatment == 'control'"
    """
    normalize_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="Metadata_treatment == 'control'",
        method="standardize",
    ).round(1)

    expected_result = pd.DataFrame(
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
            "x": [-1.1, -0.7, 1.5, -0.7, 0.4, 0.4, 0.4, -1.1],
            "y": [-0.7, -1.5, 1.1, -0.2, 0.2, 2.0, 0.7, -1.5],
            "z": [-1.3, 4.0, -0.6, 1.7, 2.5, 14.8, -0.6, -0.6],
            "zz": [5.9, 22.5, -0.9, 1.7, 14.2, 50.6, -0.4, -0.4],
        }
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_robustize_allsamples():
    """
    Testing normalize pycytominer function
    method = "robustize"
    meta_features = "none"
    samples="all"
    """
    normalize_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="robustize",
    ).round(1)

    expected_result = pd.DataFrame(
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
            "x": [-0.8, -0.5, 1.4, -0.5, 0.5, 0.5, 0.5, -0.8],
            "y": [-0.4, -0.9, 0.7, -0.1, 0.1, 1.2, 0.4, -0.9],
            "z": [-0.6, 1.0, -0.3, 0.3, 0.6, 4.1, -0.3, -0.3],
            "zz": [0.1, 1.1, -0.3, -0.1, 0.6, 2.8, -0.2, -0.2],
        }
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_robustize_ctrlsamples():
    """
    Testing normalize pycytominer function
    method = "robustize"
    meta_features = "none"
    samples="Metadata_treatment == 'control'"
    """
    normalize_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="Metadata_treatment == 'control'",
        method="robustize",
    ).round(1)

    expected_result = pd.DataFrame(
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
            "x": [-0.6, -0.4, 1.1, -0.4, 0.4, 0.4, 0.4, -0.6],
            "y": [-0.7, -1.3, 0.7, -0.3, 0.0, 1.3, 0.3, -1.3],
            "z": [-1.3, 8.0, 0.0, 4.0, 5.3, 26.7, 0.0, 0.0],
            "zz": [9.6, 35.2, -0.8, 3.2, 22.4, 78.4, 0.0, 0.0],
        }
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_robustize_mad_allsamples():
    """
    Testing normalize pycytominer function
    method = "mad_robustize"
    meta_features = "none"
    samples="all"
    """
    normalize_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="mad_robustize",
    ).round(1)

    expected_result = pd.DataFrame(
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
            "x": [-1.1, -0.7, 2, -0.7, 0.7, 0.7, 0.7, -1.1],
            "y": [-0.5, -1.2, 0.8, -0.2, 0.2, 1.5, 0.5, -1.2],
            "z": [-0.8, 1.5, -0.5, 0.5, 0.8, 6.2, -0.5, -0.5],
            "zz": [0.3, 2.9, -0.7, -0.3, 1.6, 7.1, -0.6, -0.6],
        }
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_robustize_mad_allsamples_novar():
    """
    Testing normalize pycytominer function
    method = "mad_robustize"
    meta_features = "none"
    samples="all"
    """
    features = ["x", "y", "z", "zz", "yy"]

    normalize_result = normalize(
        profiles=data_no_var_df.copy(),
        features=features,
        meta_features="infer",
        samples="all",
        method="mad_robustize",
    ).round(1)

    expected_result = pd.DataFrame(
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
            "x": [-1.1, -0.7, 2, -0.7, 0.7, 0.7, 0.7, -1.1],
            "y": [-0.5, -1.2, 0.8, -0.2, 0.2, 1.5, 0.5, -1.2],
            "z": [-0.8, 1.5, -0.5, 0.5, 0.8, 6.2, -0.5, -0.5],
            "zz": [0.3, 2.9, -0.7, -0.3, 1.6, 7.1, -0.6, -0.6],
            "yy": [0.0] * normalize_result.shape[0],
        }
    ).reset_index(drop=True)

    # Check that infinite or nan values are not introduced
    assert np.isfinite(normalize_result.loc[:, features].values).all()
    assert normalize_result.isna().sum().sum() == 0

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_standardize_allsamples_fromfile():
    """
    Testing normalize pycytominer function
    data_file provided
    method = "standardize"
    meta_features = "none"
    samples="all"
    """
    normalize_result = normalize(
        profiles=data_file,
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="standardize",
    ).round(1)

    infer_normalize_result = normalize(
        profiles=data_feature_infer_file,
        features="infer",
        meta_features=["Metadata_plate", "Metadata_treatment"],
        samples="all",
        method="standardize",
    ).round(1)

    expected_result = pd.DataFrame(
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
            "Cells_x": [-1.1, -0.7, 1.9, -0.7, 0.6, 0.6, 0.6, -1.1],
            "Cells_y": [-0.6, -1.3, 0.9, -0.2, 0.2, 1.7, 0.6, -1.3],
            "Cytoplasm_z": [-0.8, 0.3, -0.6, -0.2, 0.0, 2.5, -0.6, -0.6],
            "Nuclei_zz": [-0.3, 0.7, -0.8, -0.6, 0.2, 2.3, -0.7, -0.7],
        }
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(infer_normalize_result, expected_result)

    infer_normalize_result.columns = normalize_result.columns
    pd.testing.assert_frame_equal(normalize_result, infer_normalize_result)


def test_normalize_standardize_allsamples_output():
    """
    Testing normalize pycytominer function
    data_file provided
    method = "standardize"
    meta_features = "none"
    samples="all"
    """
    out_normalize_file = os.path.join(tmpdir, "test_normalize_output.csv")

    _ = normalize(
        profiles=data_file,
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="standardize",
        output_file=out_normalize_file,
    )

    expected_result = pd.DataFrame(
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
            "x": [-1.1, -0.7, 1.9, -0.7, 0.6, 0.6, 0.6, -1.1],
            "y": [-0.6, -1.3, 0.9, -0.2, 0.2, 1.7, 0.6, -1.3],
            "z": [-0.8, 0.3, -0.6, -0.2, 0.0, 2.5, -0.6, -0.6],
            "zz": [-0.3, 0.7, -0.8, -0.6, 0.2, 2.3, -0.7, -0.7],
        }
    ).reset_index(drop=True)

    from_file_result = pd.read_csv(out_normalize_file).round(1)

    pd.testing.assert_frame_equal(from_file_result, expected_result)


def test_normalize_standardize_allsamples_compress():
    compress_file = os.path.join(tmpdir, "test_normalize_compress.csv.gz")

    _ = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="standardize",
        output_file=compress_file,
        compression_options={"method": "gzip"},
    )
    normalize_result = pd.read_csv(compress_file).round(1)

    expected_result = pd.DataFrame(
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
            "x": [-1.1, -0.7, 1.9, -0.7, 0.6, 0.6, 0.6, -1.1],
            "y": [-0.6, -1.3, 0.9, -0.2, 0.2, 1.7, 0.6, -1.3],
            "z": [-0.8, 0.3, -0.6, -0.2, 0.0, 2.5, -0.6, -0.6],
            "zz": [-0.3, 0.7, -0.8, -0.6, 0.2, 2.3, -0.7, -0.7],
        }
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_spherize():
    for spherize_method in ["ZCA", "PCA", "ZCA-cor", "PCA-cor"]:
        for spherize_center in [True, False]:
            result = normalize(
                data_spherize_df,
                features=["a", "b", "c", "d"],
                meta_features=["id"],
                method="spherize",
                spherize_method=spherize_method,
                spherize_center=spherize_center,
            )
            result_cov = (
                pd.DataFrame(np.cov(np.transpose(result.drop("id", axis="columns"))))
                .round()
                .sum()
                .clip(1)
                .sum()
            )
            expected_result = data_spherize_df.shape[1] - 1
            assert result_cov == expected_result

            result = normalize(
                data_spherize_df,
                samples="id == 'control'",
                features=["a", "b", "c", "d"],
                meta_features=["id"],
                method="spherize",
                spherize_method=spherize_method,
                spherize_center=spherize_center,
            )
            result_cov = (
                np.cov(
                    np.transpose(
                        result.query("id == 'control'").drop("id", axis="columns")
                    )
                )
                .round()
                .sum()
                .clip(1)
                .sum()
            )
            # Add some tolerance to result b/c of low sample size
            expected_result = data_spherize_df.shape[1]
            assert result_cov < expected_result

            non_spherize_result_cov = (
                np.cov(
                    np.transpose(
                        result.query("id == 'treatment'").drop("id", axis="columns")
                    )
                )
                .round()
                .sum()
                .sum()
            )
            assert non_spherize_result_cov >= expected_result - 5


def test_spherize_epsilon():
    """
    Test that epsilon is successfully passed to the spherize transform method
    """
    sphere_norm_df = normalize(
        data_spherize_df, features=["a", "b", "c", "d"], meta_features=["id"]
    )

    spherize_method = "ZCA"
    spherize_center = True

    for custom_eps in [1e-6, 1]:
        result = normalize(
            sphere_norm_df,
            features=["a", "b", "c", "d"],
            meta_features=["id"],
            method="spherize",
            spherize_method=spherize_method,
            spherize_center=spherize_center,
            spherize_epsilon=custom_eps,
        )

        cov_mat = pd.DataFrame(np.cov(np.transpose(result.drop("id", axis="columns"))))
        off_diag_sum = np.round(np.sum(cov_mat).sum() - np.trace(cov_mat), 3)

        if custom_eps >= 1:
            assert off_diag_sum > 0
        else:
            assert off_diag_sum == 0
