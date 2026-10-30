import os
import pathlib
import random
import tempfile

import numpy as np
import pandas as pd
import pytest
from sklearn.preprocessing import QuantileTransformer, RobustScaler, StandardScaler

from pycytominer.cyto_utils.normalize_transform_io import load_normalize_transform
from pycytominer.normalize import normalize
from pycytominer.operations.transform import (
    InverseNormalTransform,
    RobustMAD,
    Spherize,
)

random.seed(123)

# Get temporary directory
tmpdir = tempfile.gettempdir()

# Setup testing files
output_test_file_csv = os.path.join(tmpdir, "test.csv")
output_test_file_parquet = os.path.join(tmpdir, "test.parquet")

# Build data to use in tests
data_df = pd.DataFrame({
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
}).reset_index(drop=True)

data_file = os.path.join(tmpdir, "test_normalize.parquet")
data_df.to_parquet(data_file, index=False)

data_feature_infer_df = pd.DataFrame({
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
}).reset_index(drop=True)

data_feature_infer_file = os.path.join(tmpdir, "test_normalize_infer.parquet")
data_feature_infer_df.to_parquet(data_feature_infer_file, index=False)

a_feature = random.sample(range(1, 100), 10)
b_feature = random.sample(range(1, 100), 10)
c_feature = random.sample(range(1, 100), 10)
d_feature = random.sample(range(1, 100), 10)
id_feature = ["control"] * 5 + ["treatment"] * 5

data_spherize_df = pd.DataFrame({
    "a": a_feature,
    "b": b_feature,
    "c": c_feature,
    "d": d_feature,
    "id": id_feature,
}).reset_index(drop=True)

data_no_var_df = pd.concat(
    [data_df, pd.DataFrame([1] * data_df.shape[0], columns=["yy"])], axis="columns"
)

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
EXAMPLE_OME_PARQUET = os.path.join(
    ROOT_DIR, "tests", "test_data", "cytodataframe", "example.ome.parquet"
)
EXAMPLE_ICEBERG_ROOT = os.path.join(
    ROOT_DIR, "tests", "test_data", "cytotable", "examplehuman_iceberg_warehouse"
)


def test_normalize_standardize_allsamples():
    """
    Testing normalize pycytominer function
    method = "standardize"
    meta_features = None
    samples="all"
    """
    normalize_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="standardize",
    ).round(1)

    expected_result = pd.DataFrame({
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
    }).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_preserves_ome_arrow_columns_when_inferred():
    profiles_path = os.path.join(
        ROOT_DIR,
        "tests",
        "test_data",
        "cytotable",
        "examplehuman_iceberg_warehouse",
        "warehouse",
        "profiles",
        "joined_profiles",
        "data",
        "00000-0-fe1e327c-3eb3-4711-833b-73ba36da733c.parquet",
    )
    image_crops_path = os.path.join(
        ROOT_DIR,
        "tests",
        "test_data",
        "cytotable",
        "examplehuman_iceberg_warehouse",
        "warehouse",
        "images",
        "image_crops",
        "data",
        "00000-0-2fa7c8c6-117b-4fab-ba87-c934a56fe88d.parquet",
    )

    profiles = (
        pd.read_parquet(profiles_path, engine="pyarrow").head(8).reset_index(drop=True)
    )
    ome_arrow_columns = pd.read_parquet(image_crops_path, engine="pyarrow")[
        ["ome_arrow_image", "ome_arrow_outline"]
    ].head(8)
    profiles = profiles.assign(
        Metadata_treatment=["control"] * 4 + ["drug"] * 4,
        ome_arrow_image=ome_arrow_columns["ome_arrow_image"].tolist(),
        ome_arrow_outline=ome_arrow_columns["ome_arrow_outline"].tolist(),
    )

    normalize_result = normalize(
        profiles=profiles,
        features="infer",
        meta_features="infer",
        samples="Metadata_treatment == 'control'",
        method="standardize",
    )

    expected_metadata_columns = [
        column for column in profiles.columns if column.startswith("Metadata_")
    ]
    expected_passthrough_image_columns = [
        "Image_FileName_DNA",
        "Image_FileName_OrigOverlay",
        "Image_FileName_PH3",
        "Image_FileName_cellbody",
        "ome_arrow_image",
        "ome_arrow_outline",
    ]

    assert "ome_arrow_image" in normalize_result.columns
    assert "ome_arrow_outline" in normalize_result.columns
    assert "Metadata_treatment" in normalize_result.columns
    assert normalize_result.columns[: len(expected_metadata_columns)].tolist() == (
        expected_metadata_columns
    )
    image_start = len(expected_metadata_columns)
    image_end = image_start + len(expected_passthrough_image_columns)
    assert normalize_result.columns[image_start:image_end].tolist() == (
        expected_passthrough_image_columns
    )
    assert all(
        column.startswith(("Cells_", "Cytoplasm_", "Nuclei_"))
        for column in normalize_result.columns[image_end:]
    )


def test_normalize_example_ome_parquet_with_explicit_feature_columns():
    profiles = pd.read_parquet(EXAMPLE_OME_PARQUET, engine="pyarrow")
    profiles = profiles.assign(
        Metadata_treatment=[
            "control" if i % 2 == 0 else "drug" for i in range(len(profiles))
        ]
    )

    normalize_result = normalize(
        profiles=profiles,
        # This column uses a ``Metadata_`` prefix in the source data but is
        # intentionally normalized here as an explicit feature.
        features=["Metadata_Cells_Number_Object_Number"],
        meta_features=["Metadata_ImageNumber", "Metadata_treatment"],
        samples="Metadata_treatment == 'control'",
        method="standardize",
    )

    assert normalize_result.shape == (3, 15)
    assert normalize_result.columns.tolist() == [
        "Metadata_ImageNumber",
        "Metadata_treatment",
        "Image_FileName_GFP",
        "Image_FileName_RFP",
        "Image_FileName_DAPI",
        "Image_FileName_GFP_OMEArrow_ORIG",
        "Image_FileName_GFP_OMEArrow_LABL",
        "Image_FileName_GFP_OMEArrow_COMP",
        "Image_FileName_RFP_OMEArrow_ORIG",
        "Image_FileName_RFP_OMEArrow_LABL",
        "Image_FileName_RFP_OMEArrow_COMP",
        "Image_FileName_DAPI_OMEArrow_ORIG",
        "Image_FileName_DAPI_OMEArrow_LABL",
        "Image_FileName_DAPI_OMEArrow_COMP",
        "Metadata_Cells_Number_Object_Number",
    ]


def test_normalize_allows_none_missing_values_in_numeric_feature_columns():
    profiles = data_df.copy()
    profiles.loc[0, "x"] = None

    normalize_result = normalize(
        profiles=profiles,
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="standardize",
    )

    assert "x" in normalize_result.columns
    assert pd.isna(normalize_result.loc[0, "x"])


def test_normalize_allows_string_missing_markers_in_feature_columns():
    profiles = data_df.copy()
    profiles["x"] = profiles["x"].astype(object)
    profiles.loc[0, "x"] = "nan"
    profiles.loc[1, "x"] = "None"

    normalize_result = normalize(
        profiles=profiles,
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="standardize",
    )

    assert "x" in normalize_result.columns
    assert pd.isna(normalize_result.loc[0, "x"])
    assert pd.isna(normalize_result.loc[1, "x"])


def test_normalize_rejects_malformed_string_feature_values():
    profiles = data_df.copy()
    profiles["x"] = profiles["x"].astype(object)
    profiles.loc[0, "x"] = "not_a_number"

    with pytest.raises(
        ValueError,
        match="normalize\\(\\) requires numeric feature columns",
    ) as exc_info:
        normalize(
            profiles=profiles,
            features=["x", "y", "z", "zz"],
            meta_features="infer",
            samples="all",
            method="standardize",
        )

    assert "x" in str(exc_info.value)
    assert "feature_select() first" in str(exc_info.value)


def test_normalize_rejects_non_numeric_feature_columns():
    profiles = pd.read_parquet(EXAMPLE_OME_PARQUET, engine="pyarrow")
    profiles = profiles.assign(
        Metadata_treatment=[
            "control" if i % 2 == 0 else "drug" for i in range(len(profiles))
        ]
    )

    with pytest.raises(
        ValueError,
        match="normalize\\(\\) requires numeric feature columns",
    ) as exc_info:
        normalize(
            profiles=profiles,
            features=["Image_FileName_GFP_OMEArrow_ORIG"],
            meta_features=["Metadata_ImageNumber", "Metadata_treatment"],
            samples="Metadata_treatment == 'control'",
            method="standardize",
        )

    assert "Image_FileName_GFP_OMEArrow_ORIG" in str(exc_info.value)
    assert "feature_select() first" in str(exc_info.value)


def test_normalize_rejects_mixed_numeric_and_ome_arrow_feature_columns():
    profiles = pd.read_parquet(EXAMPLE_OME_PARQUET, engine="pyarrow")
    profiles = profiles.assign(
        Metadata_treatment=[
            "control" if i % 2 == 0 else "drug" for i in range(len(profiles))
        ]
    )

    with pytest.raises(
        ValueError,
        match="normalize\\(\\) requires numeric feature columns",
    ) as exc_info:
        normalize(
            profiles=profiles,
            features=[
                "Metadata_Cells_Number_Object_Number",
                "Image_FileName_GFP_OMEArrow_ORIG",
            ],
            meta_features=["Metadata_ImageNumber", "Metadata_treatment"],
            samples="Metadata_treatment == 'control'",
            method="standardize",
        )

    assert "Image_FileName_GFP_OMEArrow_ORIG" in str(exc_info.value)
    assert "feature_select() first" in str(exc_info.value)


def test_normalize_warehouse_root_with_inferred_features():
    profiles = pd.read_parquet(
        os.path.join(
            EXAMPLE_ICEBERG_ROOT,
            "warehouse",
            "profiles",
            "joined_profiles",
            "data",
            "00000-0-fe1e327c-3eb3-4711-833b-73ba36da733c.parquet",
        ),
        engine="pyarrow",
    )
    profiles = profiles.assign(
        Metadata_treatment=[
            "control" if i % 2 == 0 else "drug" for i in range(len(profiles))
        ]
    )

    normalize_result = normalize(
        profiles=profiles,
        features="infer",
        meta_features="infer",
        samples="Metadata_treatment == 'control'",
        method="standardize",
    )

    expected_metadata_columns = [
        column for column in profiles.columns if column.startswith("Metadata_")
    ]
    expected_passthrough_image_columns = [
        "Image_FileName_DNA",
        "Image_FileName_OrigOverlay",
        "Image_FileName_PH3",
        "Image_FileName_cellbody",
    ]

    assert "Metadata_treatment" in normalize_result.columns
    assert normalize_result.columns[: len(expected_metadata_columns)].tolist() == (
        expected_metadata_columns
    )
    image_start = len(expected_metadata_columns)
    image_end = image_start + len(expected_passthrough_image_columns)
    assert normalize_result.columns[image_start:image_end].tolist() == (
        expected_passthrough_image_columns
    )
    assert all(
        column.startswith(("Cells_", "Cytoplasm_", "Nuclei_"))
        for column in normalize_result.columns[image_end:]
    )


def test_normalize_standardize_ctrlsamples():
    """
    Testing normalize pycytominer function
    method = "standardize"
    meta_features = None
    samples="Metadata_treatment == 'control'"
    """
    normalize_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="Metadata_treatment == 'control'",
        method="standardize",
    ).round(1)

    expected_result = pd.DataFrame({
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
    }).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_drop_cosmicqc_rows_before_normalization():
    profiles = pd.DataFrame({
        "Metadata_plate": ["plate_1"] * 4,
        "Metadata_well": ["A01", "A02", "A03", "A04"],
        "Metadata_cqc_clustered_nuclei_is_outlier": [False, True, False, False],
        "Cells_x": [0, 1000, 1, 2],
        "Nuclei_y": [10, 9999, 20, 30],
    })

    normalize_result = normalize(
        profiles=profiles,
        features="infer",
        meta_features="infer",
        samples="all",
        method="standardize",
        drop_cosmicqc_rows=True,
    )

    expected_result = pd.DataFrame(
        {
            "Metadata_plate": ["plate_1"] * 3,
            "Metadata_well": ["A01", "A03", "A04"],
            "Metadata_cqc_clustered_nuclei_is_outlier": [False, False, False],
            "Cells_x": [-1.224744871391589, 0.0, 1.224744871391589],
            "Nuclei_y": [-1.224744871391589, 0.0, 1.224744871391589],
        },
        index=[0, 2, 3],
    )

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_drop_cosmicqc_rows_error_when_qc_columns_missing():
    """
    Testing the normalize pycytominer function
    drop_cosmicqc_rows = True when there are
    no QC columns in the dataframe.
    """
    profiles = pd.DataFrame({
        "Metadata_plate": ["plate_1"] * 4,
        "Metadata_well": ["A01", "A02", "A03", "A04"],
        "Cells_x": [0, 1000, 1, 2],
        "Nuclei_y": [10, 9999, 20, 30],
    })

    with pytest.raises(ValueError, match="QC columns"):
        normalize(
            profiles=profiles,
            features="infer",
            meta_features="infer",
            samples="all",
            method="standardize",
            drop_cosmicqc_rows=True,
        )


def test_normalize_drop_cosmicqc_rows_drops_rows_with_true():
    """
    Testing the normalize pycytominer function
    drop_cosmicqc_rows = True when all rows
    are True and all rows will be dropped.
    """
    profiles = pd.DataFrame({
        "Metadata_plate": ["plate_1"] * 4,
        "Metadata_well": ["A01", "A02", "A03", "A04"],
        "Metadata_cqc_clustered_nuclei_is_outlier": [True] * 4,
        "Cells_x": [0, 1000, 1, 2],
        "Nuclei_y": [10, 9999, 20, 30],
    })

    with pytest.raises(ValueError, match="All rows were dropped"):
        normalize(
            profiles=profiles,
            features="infer",
            meta_features="infer",
            samples="all",
            method="standardize",
            drop_cosmicqc_rows=True,
        )


def test_normalize_drop_cosmicqc_rows_multiple_qc_columns():
    """
    Testing the normalize pycytominer function
    drop_cosmicqc_rows = True when there are multiple
    QC columns and rows with True in any of the QC columns
    will be dropped.
    """
    profiles = pd.DataFrame({
        "Metadata_plate": ["plate_1"] * 4,
        "Metadata_well": ["A01", "A02", "A03", "A04"],
        "Metadata_cqc_clustered_nuclei_is_outlier": [False, True, False, False],
        "Metadata_cqc_abnormal_small_cell_is_outlier": [False, False, True, False],
        "Cells_x": [0, 1000, 1, 2],
        "Nuclei_y": [10, 9999, 20, 30],
    })

    normalize_result = normalize(
        profiles=profiles,
        features="infer",
        meta_features="infer",
        samples="all",
        method="standardize",
        drop_cosmicqc_rows=True,
    )

    expected_result = pd.DataFrame(
        {
            "Metadata_plate": ["plate_1"] * 2,
            "Metadata_well": ["A01", "A04"],
            "Metadata_cqc_clustered_nuclei_is_outlier": [False, False],
            "Metadata_cqc_abnormal_small_cell_is_outlier": [False, False],
            "Cells_x": [-1.0, 1.0],
            "Nuclei_y": [-1.0, 1.0],
        },
        index=[0, 3],
    )

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_robustize_allsamples():
    """
    Testing normalize pycytominer function
    method = "robustize"
    meta_features = None
    samples="all"
    """
    normalize_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="robustize",
    ).round(1)

    expected_result = pd.DataFrame({
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
    }).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_robustize_ctrlsamples():
    """
    Testing normalize pycytominer function
    method = "robustize"
    meta_features = None
    samples="Metadata_treatment == 'control'"
    """
    normalize_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="Metadata_treatment == 'control'",
        method="robustize",
    ).round(1)

    expected_result = pd.DataFrame({
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
    }).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_robustize_mad_allsamples():
    """
    Testing normalize pycytominer function
    method = "mad_robustize"
    meta_features = None
    samples="all"
    """
    normalize_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="all",
        method="mad_robustize",
    ).round(1)

    expected_result = pd.DataFrame({
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
    }).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_robustize_mad_allsamples_novar():
    """
    Testing normalize pycytominer function
    method = "mad_robustize"
    meta_features = None
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

    expected_result = pd.DataFrame({
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
    }).reset_index(drop=True)

    # Check that infinite or nan values are not introduced
    assert np.isfinite(normalize_result.loc[:, features].values).all()
    assert normalize_result.isna().sum().sum() == 0

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_inverse_normal_inferred_image_profile_features():
    """
    Testing normalize pycytominer function
    method = "inverse_normal"
    features = "infer"
    samples = "all"
    """
    # Set features to the inferred image profile features
    features = ["Cells_x", "Cells_y", "Cytoplasm_z", "Nuclei_zz"]

    # Run normalize with the inferred features
    normalize_result = normalize(
        profiles=data_feature_infer_df.copy(),
        features="infer",
        meta_features=["Metadata_plate", "Metadata_treatment"],
        samples="all",
        method="inverse_normal",
        inverse_normal_n_quantiles=5,
    )

    # Compute the expected result using QuantileTransformer with the same parameters
    expected_features = QuantileTransformer(
        n_quantiles=5,
        output_distribution="normal",
    ).fit_transform(data_feature_infer_df.loc[:, features])
    expected_result = pd.concat(
        [
            data_feature_infer_df.loc[:, ["Metadata_plate", "Metadata_treatment"]],
            pd.DataFrame(expected_features, columns=features),
        ],
        axis="columns",
    )

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_inverse_normal_control_samples():
    """
    Testing normalize pycytominer function
    method = "inverse_normal"
    samples = "Metadata_treatment == 'control'"
    """

    # Set features to the inferred image profile features
    # and add a control query to select only control samples
    features = ["Cells_x", "Cells_y", "Cytoplasm_z", "Nuclei_zz"]
    control_query = "Metadata_treatment == 'control'"

    # Run normalize with the inferred features and control samples
    normalize_result = normalize(
        profiles=data_feature_infer_df.copy(),
        features=features,
        meta_features=["Metadata_plate", "Metadata_treatment"],
        samples=control_query,
        method="inverse_normal",
        inverse_normal_n_quantiles=3,
    )

    # Set up the expected result using QuantileTransformer with the same parameters
    expected_scaler = QuantileTransformer(
        n_quantiles=3,
        output_distribution="normal",
    ).fit(data_feature_infer_df.query(control_query).loc[:, features])
    expected_features = expected_scaler.transform(
        data_feature_infer_df.loc[:, features]
    )
    expected_result = pd.concat(
        [
            data_feature_infer_df.loc[:, ["Metadata_plate", "Metadata_treatment"]],
            pd.DataFrame(expected_features, columns=features),
        ],
        axis="columns",
    )

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_standardize_allsamples_fromfile():
    """
    Testing normalize pycytominer function
    data_file provided
    method = "standardize"
    meta_features = None
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

    expected_result = pd.DataFrame({
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
    }).reset_index(drop=True)

    pd.testing.assert_frame_equal(infer_normalize_result, expected_result)

    infer_normalize_result.columns = normalize_result.columns
    pd.testing.assert_frame_equal(normalize_result, infer_normalize_result)


def test_normalize_standardize_allsamples_output():
    """
    Testing normalize pycytominer function
    data_file provided
    method = "standardize"
    meta_features = None
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

    expected_result = pd.DataFrame({
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
    }).reset_index(drop=True)

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

    expected_result = pd.DataFrame({
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
    }).reset_index(drop=True)

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_spherize():
    for spherize_method in ["ZCA", "PCA", "ZCA-cor", "PCA-cor"]:
        for spherize_center in [True, False]:
            if spherize_method in ["PCA-cor", "ZCA-cor"] and not spherize_center:
                # verify that calling normalize will throw a ValueError
                with pytest.raises(ValueError):
                    normalize(
                        data_spherize_df,
                        features=["a", "b", "c", "d"],
                        meta_features=["id"],
                        method="spherize",
                        spherize_method=spherize_method,
                        spherize_center=spherize_center,
                    )

            else:
                result = normalize(
                    data_spherize_df,
                    features=["a", "b", "c", "d"],
                    meta_features=["id"],
                    method="spherize",
                    spherize_method=spherize_method,
                    spherize_center=spherize_center,
                )
                result_cov = (
                    pd
                    .DataFrame(np.cov(np.transpose(result.drop("id", axis="columns"))))
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
                    np
                    .cov(
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
                    np
                    .cov(
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


def test_output_type():
    """
    Testing normalize pycytominer function with output
    """
    # dictionary with the output name associated with the file type
    output_dict = {"csv": output_test_file_csv, "parquet": output_test_file_parquet}

    # test both output types available with output function
    for _type, outname in output_dict.items():
        # Test output
        normalize(
            profiles=data_file,
            features=["x", "y", "z", "zz"],
            meta_features="infer",
            samples="all",
            method="standardize",
            output_file=outname,
            output_type=_type,
        )

    # read files in with pandas
    csv_df = pd.read_csv(output_test_file_csv)
    parquet_df = pd.read_parquet(output_test_file_parquet)

    # check to make sure the files were read in corrrectly as a pd.Dataframe
    assert isinstance(csv_df, pd.DataFrame)
    assert isinstance(parquet_df, pd.DataFrame)

    # check to make sure both dataframes are the same regardless of the output_type
    pd.testing.assert_frame_equal(csv_df, parquet_df)


@pytest.mark.parametrize(
    "method",
    ["standardize", "robustize", "mad_robustize", "spherize", "inverse_normal"],
)
def test_normalize_transform_save_and_apply(method):
    """
    Fitting a transform on the control samples and saving it via
    `transform_output_file` should produce the same result as loading that
    saved transform with `fitted_transform_file` and applying it to a subset
    of the same profiles.
    """
    transform_file = os.path.join(tmpdir, f"test_normalize_{method}_transform.onnx")

    fit_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="Metadata_treatment == 'control'",
        method=method,
        transform_output_file=transform_file,
    )

    assert os.path.exists(transform_file)

    # Apply the saved transform to a subset of the original profiles
    subset_idx = [0, 2, 5]
    subset_df = data_df.iloc[subset_idx].reset_index(drop=True)

    applied_result = normalize(
        profiles=subset_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method=method,
        fitted_transform_file=transform_file,
    )

    expected_subset_result = fit_result.iloc[subset_idx].reset_index(drop=True)

    numeric_cols = [
        col for col in applied_result.columns if not col.startswith("Metadata_")
    ]

    np.testing.assert_allclose(
        applied_result.loc[:, numeric_cols].values,
        expected_subset_result.loc[:, numeric_cols].values,
        rtol=1e-6,
        atol=1e-8,
    )


@pytest.mark.parametrize(
    "method,scaler",
    [
        ("standardize", StandardScaler()),
        ("robustize", RobustScaler()),
    ],
)
def test_normalize_saved_transform_applied_to_different_data(method, scaler):
    """
    A transform fit on one dataset and saved should, when applied to a
    different dataset, use the saved (training) parameters rather than
    re-fitting on the new data.
    """
    features = ["x", "y", "z", "zz"]
    transform_file = os.path.join(
        tmpdir, f"test_normalize_{method}_different_data_transform.onnx"
    )

    normalize(
        profiles=data_df.copy(),
        features=features,
        meta_features="infer",
        samples="Metadata_treatment == 'control'",
        method=method,
        transform_output_file=transform_file,
    )

    # New data with a different distribution than the training data
    new_df = data_df.copy()
    new_df[features] = new_df[features] * 3 + 10
    new_df["Metadata_plate"] = ["c"] * len(new_df)

    applied_result = normalize(
        profiles=new_df.copy(),
        features=features,
        meta_features="infer",
        method=method,
        fitted_transform_file=transform_file,
    )

    # Expected: training-data parameters applied to the new data
    train_controls = data_df.query("Metadata_treatment == 'control'")
    expected = scaler.fit(train_controls[features]).transform(new_df[features])
    np.testing.assert_allclose(
        applied_result[features].values, expected, rtol=1e-6, atol=1e-8
    )

    # Re-fitting on the new data would give a different result
    refit_result = normalize(
        profiles=new_df.copy(),
        features=features,
        meta_features="infer",
        method=method,
    )
    assert not np.allclose(
        applied_result[features].values, refit_result[features].values
    )

    # Metadata is passed through untouched
    pd.testing.assert_series_equal(
        applied_result["Metadata_plate"], new_df["Metadata_plate"]
    )


def test_normalize_fitted_transform_file_with_inferred_features():
    """With CellProfiler-style column names, features="infer" should resolve
    to the features the saved transform was fit on."""
    transform_file = os.path.join(tmpdir, "test_normalize_infer_transform.onnx")
    cp_df = data_df.rename(
        columns={
            "x": "Cells_AreaShape_x",
            "y": "Cells_AreaShape_y",
            "z": "Cells_AreaShape_z",
            "zz": "Cells_AreaShape_zz",
        }
    )

    fit_result = normalize(
        profiles=cp_df.copy(),
        features="infer",
        meta_features="infer",
        method="standardize",
        transform_output_file=transform_file,
    )
    applied_result = normalize(
        profiles=cp_df.copy(),
        features="infer",
        meta_features="infer",
        method="standardize",
        fitted_transform_file=transform_file,
    )

    pd.testing.assert_frame_equal(applied_result, fit_result)


def test_normalize_transform_save_and_apply_explicit_features():
    """
    The save/apply round trip should also work end-to-end when every
    "infer"-defaulted argument (`features`, `meta_features`) is instead
    passed explicitly, both when fitting/saving and when applying the saved
    transform.
    """
    transform_file = os.path.join(
        tmpdir, "test_normalize_explicit_features_transform.onnx"
    )
    explicit_features = ["x", "y", "z", "zz"]
    explicit_meta_features = ["Metadata_plate", "Metadata_treatment"]

    fit_result = normalize(
        profiles=data_df.copy(),
        features=explicit_features,
        meta_features=explicit_meta_features,
        samples="Metadata_treatment == 'control'",
        method="standardize",
        transform_output_file=transform_file,
    )

    assert os.path.exists(transform_file)

    subset_idx = [0, 2, 5]
    subset_df = data_df.iloc[subset_idx].reset_index(drop=True)

    applied_result = normalize(
        profiles=subset_df.copy(),
        features=explicit_features,
        meta_features=explicit_meta_features,
        method="standardize",
        fitted_transform_file=transform_file,
    )

    expected_subset_result = fit_result.iloc[subset_idx].reset_index(drop=True)

    pd.testing.assert_frame_equal(applied_result, expected_subset_result)


def test_normalize_fitted_transform_file_mismatched_features():
    """
    Applying a saved transform with an explicit `features` list that does not
    match the features the transform was fit on should raise a ValueError.
    """
    transform_file = os.path.join(
        tmpdir, "test_normalize_mismatch_features_transform.onnx"
    )

    normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="standardize",
        transform_output_file=transform_file,
    )

    with pytest.raises(ValueError):
        normalize(
            profiles=data_df.copy(),
            features=["x", "y", "z"],
            meta_features="infer",
            method="standardize",
            fitted_transform_file=transform_file,
        )


def test_normalize_fitted_transform_file_reorders_features():
    """
    Applying a saved transform with `features` given in a different order
    than the transform was fit on should reorder to the fit order (not
    raise), and should produce the same result as applying with the saved
    order directly.
    """
    transform_file = os.path.join(
        tmpdir, "test_normalize_reorder_features_transform.onnx"
    )

    normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="standardize",
        transform_output_file=transform_file,
    )

    result_saved_order = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="standardize",
        fitted_transform_file=transform_file,
    )

    # Same set of features, different order than the transform was fit on
    result_reordered = normalize(
        profiles=data_df.copy(),
        features=["zz", "z", "y", "x"],
        meta_features="infer",
        method="standardize",
        fitted_transform_file=transform_file,
    )

    pd.testing.assert_frame_equal(result_saved_order, result_reordered)


def test_normalize_transform_output_file_and_fitted_transform_file_mutually_exclusive():
    """
    Passing both `transform_output_file` and `fitted_transform_file` should
    raise a ValueError, since `fitted_transform_file` skips fitting entirely
    and there is nothing to save.
    """
    transform_file = os.path.join(
        tmpdir, "test_normalize_mutually_exclusive_transform.onnx"
    )

    normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="standardize",
        transform_output_file=transform_file,
    )

    other_transform_file = os.path.join(
        tmpdir, "test_normalize_mutually_exclusive_other_transform.onnx"
    )
    with pytest.raises(ValueError):
        normalize(
            profiles=data_df.copy(),
            features=["x", "y", "z", "zz"],
            meta_features="infer",
            method="standardize",
            fitted_transform_file=transform_file,
            transform_output_file=other_transform_file,
        )


def test_normalize_fitted_transform_file_method_mismatch():
    """
    Passing a `method` that does not match the method used to fit a saved
    transform should raise a ValueError, even when `method` is left at its
    "standardize" default.
    """
    transform_file = os.path.join(
        tmpdir, "test_normalize_method_mismatch_transform.onnx"
    )

    normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        samples="Metadata_treatment == 'control'",
        method="robustize",
        transform_output_file=transform_file,
    )

    # Default method ("standardize") does not match the saved "robustize" transform
    with pytest.raises(ValueError):
        normalize(
            profiles=data_df.copy(),
            features=["x", "y", "z", "zz"],
            meta_features="infer",
            fitted_transform_file=transform_file,
        )

    # Explicit, mismatched method
    with pytest.raises(ValueError):
        normalize(
            profiles=data_df.copy(),
            features=["x", "y", "z", "zz"],
            meta_features="infer",
            method="spherize",
            fitted_transform_file=transform_file,
        )


@pytest.mark.parametrize(
    ("method", "kwargs", "expected_params"),
    [
        (
            "mad_robustize",
            {"mad_robustize_epsilon": 0.5},
            {"mad_robustize_epsilon": 0.5},
        ),
        (
            "spherize",
            {
                "spherize_center": False,
                "spherize_method": "PCA",
                "spherize_epsilon": 0.25,
            },
            {
                "spherize_center": False,
                "spherize_method": "PCA",
                "spherize_epsilon": 0.25,
            },
        ),
        (
            "inverse_normal",
            {"inverse_normal_n_quantiles": 4},
            {"inverse_normal_n_quantiles": 4},
        ),
        ("standardize", {}, {}),
        ("robustize", {}, {}),
    ],
)
def test_normalize_transform_output_file_stores_provenance_params(
    method, kwargs, expected_params
):
    """
    The ONNX file written via `transform_output_file` should record the
    method-specific fitting parameters (for provenance/inspection), not just
    the fitted scaler.
    """
    transform_file = os.path.join(
        tmpdir, f"test_normalize_{method}_provenance_transform.onnx"
    )

    normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method=method,
        transform_output_file=transform_file,
        **kwargs,
    )

    _, loaded_method, loaded_features, loaded_params = load_normalize_transform(
        transform_file
    )

    assert loaded_method == method
    assert loaded_params == expected_params
    assert loaded_features == ["x", "y", "z", "zz"]


def test_normalize_transform_output_file_does_not_create_parent_directory():
    """
    Pycytominer should not be opinionated about creating directories: if the
    parent directory for `transform_output_file` does not exist, `normalize`
    should raise rather than silently creating it, matching the behavior of
    the underlying file-write call.
    """
    missing_parent = os.path.join(tmpdir, "does_not_exist_normalize_dir")
    transform_file = os.path.join(missing_parent, "transform.onnx")

    assert not os.path.exists(missing_parent)

    with pytest.raises(OSError):
        normalize(
            profiles=data_df.copy(),
            features=["x", "y", "z", "zz"],
            meta_features="infer",
            method="standardize",
            transform_output_file=transform_file,
        )

    assert not os.path.exists(missing_parent)


def test_normalize_fitted_transform_file_ignores_fit_only_arguments():
    """
    When `fitted_transform_file` is given, fit-only arguments are ignored, so
    the checks that only make sense when fitting should not fire: a rank-based
    inverse normal with a non-"all" `samples`, and a missing
    `mad_robustize_epsilon`. Fitting a new transform with the same arguments
    should still raise.
    """
    transform_file = os.path.join(tmpdir, "test_fit_only_args_transform.onnx")

    fit_result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="inverse_normal",
        inverse_normal_method="blom",
        transform_output_file=transform_file,
    )

    applied = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="inverse_normal",
        inverse_normal_method="blom",
        samples="Metadata_treatment == 'control'",
        fitted_transform_file=transform_file,
    )
    # `samples` is ignored, so the loaded transform reproduces the fit output
    pd.testing.assert_frame_equal(applied, fit_result)

    with pytest.raises(ValueError):
        normalize(
            profiles=data_df.copy(),
            features=["x", "y", "z", "zz"],
            meta_features="infer",
            method="inverse_normal",
            inverse_normal_method="blom",
            samples="Metadata_treatment == 'control'",
        )

    mad_file = os.path.join(tmpdir, "test_fit_only_args_mad_transform.onnx")

    mad_fit = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="mad_robustize",
        transform_output_file=mad_file,
    )

    mad_applied = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="mad_robustize",
        mad_robustize_epsilon=None,
        fitted_transform_file=mad_file,
    )
    pd.testing.assert_frame_equal(mad_applied, mad_fit)

    with pytest.raises(ValueError):
        normalize(
            profiles=data_df.copy(),
            features=["x", "y", "z", "zz"],
            meta_features="infer",
            method="mad_robustize",
            mad_robustize_epsilon=None,
        )


def test_normalize_does_not_mutate_input_dataframe():
    """
    Missing-value cleanup of feature columns should happen on a copy, so the
    caller's DataFrame keeps its original object columns.
    """
    profiles = pd.DataFrame({
        "Metadata_treatment": ["control", "drug", "control"],
        "x": pd.Series([1.0, "nan", 3.0], dtype=object),
        "y": [1.0, 2.0, 3.0],
    })
    original = profiles.copy()

    normalize(
        profiles=profiles,
        features=["x", "y"],
        meta_features=["Metadata_treatment"],
        method="standardize",
    )

    pd.testing.assert_frame_equal(profiles, original)


def test_normalize_accepts_object_column_of_only_numbers():
    """
    An object-dtype feature column that holds only Python numbers should be
    coerced to numeric instead of rejected.
    """
    profiles = pd.DataFrame({
        "Metadata_treatment": ["control", "drug", "control"],
        "x": pd.Series([1, 2, 3], dtype=object),
        "y": [1.0, 2.0, 3.0],
    })

    result = normalize(
        profiles=profiles,
        features=["x", "y"],
        meta_features=["Metadata_treatment"],
        method="standardize",
    )

    assert set(result.columns) == {"Metadata_treatment", "x", "y"}


def test_normalize_rejects_feature_listed_as_meta_feature():
    """A column cannot be both a feature and a meta feature."""
    with pytest.raises(ValueError, match="both `features` and `meta_features`"):
        normalize(
            profiles=data_df.copy(),
            features=["x", "y", "z", "zz"],
            meta_features=["Metadata_treatment", "x"],
            method="standardize",
        )


def test_normalize_inverse_normal_args_are_case_insensitive_and_validated():
    """
    `inverse_normal_method` should be lowercased like `method`, and an invalid
    `inverse_normal_ties` should be rejected up front.
    """
    result = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="Inverse_Normal",
        inverse_normal_method="Blom",
        inverse_normal_ties="Average",
    )
    assert not result.empty

    with pytest.raises(ValueError, match="inverse_normal_ties"):
        normalize(
            profiles=data_df.copy(),
            features=["x", "y", "z", "zz"],
            meta_features="infer",
            method="inverse_normal",
            inverse_normal_ties="avg",
        )


def test_normalize_fitted_transform_file_infer_uses_saved_features(tmp_path):
    """
    With `features="infer"`, applying a saved transform should use the features
    it was fit on rather than re-inferring them from `profiles`.
    """
    transform_file = os.path.join(str(tmp_path), "infer_saved_features.onnx")

    fitted = normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="standardize",
        transform_output_file=transform_file,
    )

    applied = normalize(
        profiles=data_df.copy(),
        features="infer",
        meta_features="infer",
        method="standardize",
        fitted_transform_file=transform_file,
    )

    pd.testing.assert_frame_equal(applied, fitted)


def test_normalize_does_not_copy_input_without_cleanup(monkeypatch):
    """Clean numeric input must not pay for a DataFrame copy."""
    df = pd.DataFrame({
        "Metadata_a": ["a", "b", "c", "d"],
        "x": [1.0, 2.0, 3.0, 4.0],
        "y": [4.0, 3.0, 1.0, 2.0],
    })

    def fail_copy(self, *args, **kwargs):
        raise AssertionError("DataFrame.copy() should not be called")

    monkeypatch.setattr(pd.DataFrame, "copy", fail_copy)

    normalize(
        profiles=df,
        features=["x", "y"],
        meta_features=["Metadata_a"],
        method="standardize",
    )


def _new_data_frames():
    rng = np.random.default_rng(0)
    train = pd.DataFrame(rng.normal(5, 2, size=(30, 3)), columns=["x", "y", "z"])
    new = pd.DataFrame(rng.normal(-20, 9, size=(8, 3)), columns=["x", "y", "z"])
    train.insert(0, "Metadata_a", [f"t{i}" for i in range(len(train))])
    new.insert(0, "Metadata_a", [f"n{i}" for i in range(len(new))])
    return train, new


@pytest.mark.parametrize(
    "method,kwargs",
    [
        ("standardize", {}),
        ("robustize", {}),
        ("mad_robustize", {"mad_robustize_epsilon": 1e-6}),
        ("spherize", {"spherize_method": "ZCA-cor", "spherize_epsilon": 1e-6}),
        ("inverse_normal", {"inverse_normal_method": "quantile"}),
    ],
)
def test_normalize_fitted_transform_file_applies_to_new_data(tmp_path, method, kwargs):
    """
    A saved transform applied to data it was not trained on must reproduce the
    training-fit transform, not statistics refit on the new data. Expected
    values come from fitting the in-memory scaler on the training data.
    """
    train_df, new_df = _new_data_frames()
    features = ["x", "y", "z"]
    transform_file = str(tmp_path / f"{method}.onnx")
    common = {"features": features, "meta_features": ["Metadata_a"], "method": method}

    normalize(
        profiles=train_df, transform_output_file=transform_file, **common, **kwargs
    )
    applied = normalize(
        profiles=new_df, fitted_transform_file=transform_file, **common, **kwargs
    )

    train_x, new_x = train_df[features], new_df[features]
    if method == "standardize":
        expected = StandardScaler().fit(train_x).transform(new_x)
    elif method == "robustize":
        expected = RobustScaler().fit(train_x).transform(new_x)
    elif method == "mad_robustize":
        expected = RobustMAD(epsilon=1e-6).fit(train_x).transform(new_x).to_numpy()
    elif method == "spherize":
        expected = (
            Spherize(
                center=True,
                method="ZCA-cor",
                epsilon=1e-6,
                return_numpy=True,
            )
            .fit(train_x)
            .transform(new_x)
        )
    else:
        expected = (
            InverseNormalTransform(n_quantiles=1000, method="quantile")
            .fit(train_x)
            .transform(new_x)
        )

    np.testing.assert_allclose(
        applied[features].to_numpy(), expected, rtol=1e-6, atol=1e-8
    )

    refit = normalize(profiles=new_df, **common, **kwargs)
    assert not np.allclose(applied[features].to_numpy(), refit[features].to_numpy())


@pytest.mark.parametrize("rankit", ["blom", "tukey", "van_der_waerden", "hazen"])
def test_normalize_fitted_transform_file_rankit_ranks_new_data(tmp_path, rankit):
    """
    Rank-based inverse normal methods keep no fitted state, so a loaded
    transform ranks the new data itself: it equals a direct normalize() of it.
    """
    train_df, new_df = _new_data_frames()
    transform_file = str(tmp_path / f"{rankit}.onnx")
    common = {
        "features": ["x", "y", "z"],
        "meta_features": ["Metadata_a"],
        "method": "inverse_normal",
        "inverse_normal_method": rankit,
    }

    normalize(profiles=train_df, transform_output_file=transform_file, **common)
    applied = normalize(profiles=new_df, fitted_transform_file=transform_file, **common)
    direct = normalize(profiles=new_df, **common)

    pd.testing.assert_frame_equal(applied, direct)


@pytest.mark.parametrize(
    "method,kwargs",
    [
        ("standardize", {}),
        ("robustize", {}),
        ("mad_robustize", {"mad_robustize_epsilon": 1e-6}),
        ("inverse_normal", {"inverse_normal_method": "quantile"}),
        ("inverse_normal", {"inverse_normal_method": "blom"}),
    ],
)
def test_normalize_nan_is_preserved_and_does_not_leak(tmp_path, method, kwargs):
    """
    A NaN stays NaN in its own cell and does not change any other cell, both
    when fitting and when applying a saved transform.
    """
    train_df, _ = _new_data_frames()
    features = ["x", "y", "z"]
    with_nan = train_df.copy()
    with_nan.loc[2, "x"] = np.nan
    transform_file = str(tmp_path / "nan.onnx")
    common = {"features": features, "meta_features": ["Metadata_a"], "method": method}

    fit = normalize(
        profiles=with_nan, transform_output_file=transform_file, **common, **kwargs
    )
    applied = normalize(
        profiles=with_nan, fitted_transform_file=transform_file, **common, **kwargs
    )

    for result in (fit, applied):
        assert result[features].isna().sum().to_dict() == {"x": 1, "y": 0, "z": 0}
        assert np.isnan(result.loc[2, "x"])
    pd.testing.assert_frame_equal(applied, fit)

    # Columns without NaN are unaffected by the NaN in another column
    clean = normalize(profiles=train_df, **common, **kwargs)
    np.testing.assert_allclose(fit[["y", "z"]], clean[["y", "z"]])


def test_normalize_spherize_rejects_nan():
    """Spherize needs complete rows, so NaN fails fast with a clear error."""
    train_df, _ = _new_data_frames()
    train_df.loc[2, "x"] = np.nan

    with pytest.raises(ValueError, match=r"missing values.*\['x'\]"):
        normalize(
            profiles=train_df,
            features=["x", "y", "z"],
            meta_features=["Metadata_a"],
            method="spherize",
        )


def _write_transform_variant(src, dst, mutate):
    """Copy a saved transform to `dst`, letting `mutate(model)` corrupt it."""
    import onnx

    model = onnx.load(str(src))
    mutate(model)
    onnx.save(model, str(dst))
    return dst


def _set_metadata(key, value):
    def mutate(model):
        for entry in model.metadata_props:
            if entry.key == f"pycytominer_{key}":
                entry.value = value

    return mutate


def _drop_metadata(key):
    def mutate(model):
        kept = [e for e in model.metadata_props if e.key != f"pycytominer_{key}"]
        del model.metadata_props[:]
        model.metadata_props.extend(kept)

    return mutate


@pytest.mark.parametrize(
    "mutate",
    [
        _set_metadata("features", '"x"'),
        _set_metadata("features", "[]"),
        _set_metadata("features", "[1, 2]"),
        _set_metadata("features", '["x", "y"]'),
        _set_metadata("features", "not json"),
        _drop_metadata("columns"),
    ],
    ids=[
        "features-not-list",
        "features-empty",
        "features-not-str",
        "features-wrong-count",
        "metadata-not-json",
        "columns-missing",
    ],
)
def test_load_normalize_transform_rejects_invalid_payload(tmp_path, mutate):
    """A well-formed ONNX file with a bad pycytominer payload raises ValueError."""
    good = str(tmp_path / "good.onnx")
    normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="standardize",
        transform_output_file=good,
    )
    bad = _write_transform_variant(good, tmp_path / "bad.onnx", mutate)

    with pytest.raises(ValueError, match="normalize transform"):
        load_normalize_transform(bad)
    with pytest.raises(ValueError, match="normalize transform"):
        normalize(
            profiles=data_df.copy(),
            features="infer",
            meta_features="infer",
            method="standardize",
            fitted_transform_file=bad,
        )


def test_load_normalize_transform_rejects_inverse_normal_missing_tensors(tmp_path):
    """A quantile inverse-normal transform without its quantile tensors is rejected."""
    good = str(tmp_path / "good.onnx")
    normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="inverse_normal",
        inverse_normal_method="quantile",
        transform_output_file=good,
    )

    def strip_tensors(model):
        del model.graph.initializer[:]

    bad = _write_transform_variant(good, tmp_path / "bad.onnx", strip_tensors)

    with pytest.raises(ValueError, match="normalize transform"):
        load_normalize_transform(bad)


@pytest.mark.parametrize(
    "content", [b"", b"this is not an onnx file", b"\x00\x01\x02\x03" * 16]
)
def test_load_normalize_transform_rejects_corrupt_file(tmp_path, content):
    """Empty, text, or binary-garbage files raise ValueError, not a protobuf error."""
    bad = tmp_path / "corrupt.onnx"
    bad.write_bytes(content)

    with pytest.raises(ValueError, match=r"could not be parsed|metadata is missing"):
        load_normalize_transform(bad)


def test_load_normalize_transform_rejects_truncated_file(tmp_path):
    good = str(tmp_path / "good.onnx")
    normalize(
        profiles=data_df.copy(),
        features=["x", "y", "z", "zz"],
        meta_features="infer",
        method="robustize",
        transform_output_file=good,
    )
    data = pathlib.Path(good).read_bytes()
    bad = tmp_path / "truncated.onnx"
    bad.write_bytes(data[: len(data) // 2])

    with pytest.raises(ValueError, match="normalize transform"):
        load_normalize_transform(bad)
