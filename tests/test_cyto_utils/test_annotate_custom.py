import os
import tempfile
import random
import pytest
import pandas as pd
from pycytominer import annotate
from pycytominer.cyto_utils import cp_clean

random.seed(123)

# Get temporary directory
tmpdir = tempfile.gettempdir()

# Lauch a sqlite connection
output_file = os.path.join(tmpdir, "test_external.csv")

# Build data to use in tests
example_broad_samples = [
    "BRD-K76022557-003-28-9",
    "BRD-K65856711-001-03-6",
    "BRD-K38019854-323-01-4",
    "BRD-K06182768-001-02-3",
    "BRD-K91623615-001-06-8",
    "BRD-K13094524-001-09-1",
]
expected_pert_ids = [
    "BRD-K76022557",
    "BRD-K65856711",
    "BRD-K38019854",
    "BRD-K06182768",
    "BRD-K91623615",
    "BRD-K13094524",
]
example_genetic_perts = ["TP53", "KRAS", "DNMT3", "PTEN", "EMPTY", "EMPTY"]

data_df = pd.concat(
    [
        pd.DataFrame(
            {"Metadata_Well": ["A01", "A02", "A03"], "x": [1, 3, 8], "y": [5, 3, 1]}
        ),
        pd.DataFrame(
            {"Metadata_Well": ["B01", "B02", "B03"], "x": [1, 3, 5], "y": [8, 3, 1]}
        ),
    ]
).reset_index(drop=True)

platemap_df = pd.DataFrame(
    {
        "well_position": ["A01", "A02", "A03", "B01", "B02", "B03"],
        "gene": ["x", "y", "z"] * 2,
    }
).reset_index(drop=True)

broad_platemap_df = platemap_df.assign(Metadata_broad_sample=example_broad_samples)


def test_annotate_cmap_assert():
    with pytest.raises(AssertionError) as nocmap:
        anno_result = annotate(
            profiles=data_df,
            platemap=platemap_df,
            join_on=["Metadata_well_position", "Metadata_Well"],
            format_broad_cmap=True,
            cmap_args={"perturbation_mode": "none"},
        )

        assert "Are you sure this is a CMAP file?" in str(nocmap.value)


def test_annotate_cmap_pertnone():
    anno_result = annotate(
        profiles=data_df,
        platemap=broad_platemap_df,
        join_on=["Metadata_well_position", "Metadata_Well"],
        format_broad_cmap=True,
        cmap_args={"perturbation_mode": "none"},
    )

    added_cols = [
        "Metadata_pert_id",
        "Metadata_pert_mfc_id",
        "Metadata_pert_well",
        "Metadata_pert_id_vendor",
        "Metadata_cell_id",
        "Metadata_pert_type",
        "Metadata_broad_sample_type",
    ]

    assert all(x in anno_result.columns for x in added_cols)
    assert anno_result.Metadata_pert_id.tolist() == expected_pert_ids


def test_annotate_cmap_pertgenetic():
    anno_result = annotate(
        profiles=data_df,
        platemap=broad_platemap_df.assign(Metadata_pert_name=example_genetic_perts),
        join_on=["Metadata_well_position", "Metadata_Well"],
        format_broad_cmap=True,
        cmap_args={"perturbation_mode": "genetic"},
    )

    expected_Metadata_pert_type = ["trt", "trt", "trt", "trt", "control", "control"]
    assert anno_result.Metadata_pert_type.tolist() == expected_Metadata_pert_type
    assert (
        anno_result.Metadata_broad_sample_type.tolist() == expected_Metadata_pert_type
    )
    assert anno_result.Metadata_pert_id.tolist() == expected_pert_ids


def test_annotate_cmap_pertchemical():
    anno_result = annotate(
        profiles=data_df,
        platemap=broad_platemap_df,
        join_on=["Metadata_well_position", "Metadata_Well"],
        format_broad_cmap=True,
        cmap_args={"perturbation_mode": "genetic"},
    )

    added_cols = [
        "Metadata_pert_id",
        "Metadata_pert_mfc_id",
        "Metadata_pert_well",
        "Metadata_pert_id_vendor",
        "Metadata_cell_id",
        "Metadata_pert_type",
        "Metadata_broad_sample_type",
    ]

    assert all(x in anno_result.columns for x in added_cols)

    some_doses = [1000, 2, 1, 1, 1, 1]
    chemical_platemap = broad_platemap_df.copy()
    chemical_platemap.loc[0, "Metadata_broad_sample"] = "DMSO"
    chemical_platemap = chemical_platemap.assign(
        Metadata_mmoles_per_liter=some_doses,
        Metadata_mg_per_ml=some_doses,
        Metadata_solvent="DMSO",
    )

    anno_result = annotate(
        profiles=data_df,
        platemap=chemical_platemap,
        join_on=["Metadata_well_position", "Metadata_Well"],
        format_broad_cmap=True,
        cmap_args={"perturbation_mode": "chemical"},
    )
    expected_Metadata_pert_type = ["control", "trt", "trt", "trt", "trt", "trt"]
    assert anno_result.Metadata_pert_type.tolist() == expected_Metadata_pert_type
    assert (
        anno_result.Metadata_broad_sample_type.tolist() == expected_Metadata_pert_type
    )

    expected_dose = [0, 2, 1, 1, 1, 1]
    assert anno_result.Metadata_mmoles_per_liter.tolist() == expected_dose
    assert anno_result.Metadata_mg_per_ml.tolist() == expected_dose

    added_cols += [
        "Metadata_mmoles_per_liter",
        "Metadata_mg_per_ml",
        "Metadata_solvent",
        "Metadata_pert_vehicle",
    ]
    assert all(x in anno_result.columns for x in added_cols)


def test_annotate_cmap_externalmetadata():
    external_data_example = pd.DataFrame(
        {"test_well_join": ["A01"], "test_info_col": ["DMSO is cool"]}
    ).reset_index(drop=True)

    external_data_example.to_csv(output_file, index=False, sep=",")

    some_doses = [1000, 2, 1, 1, 1, 1]
    chemical_platemap = broad_platemap_df.copy()
    chemical_platemap.loc[0, "Metadata_broad_sample"] = "DMSO"
    chemical_platemap = chemical_platemap.assign(
        Metadata_mmoles_per_liter=some_doses,
        Metadata_mg_per_ml=some_doses,
        Metadata_solvent="DMSO",
        Metadata_cell_id="A549",
    )

    anno_result = annotate(
        profiles=data_df,
        platemap=chemical_platemap,
        join_on=["Metadata_well_position", "Metadata_Well"],
        format_broad_cmap=True,
        cmap_args={"perturbation_mode": "chemical"},
        external_metadata=output_file,
        external_join_left="Metadata_Well",
        external_join_right="Metadata_test_well_join",
    )

    assert anno_result.loc[0, "Metadata_test_info_col"] == "DMSO is cool"
    assert anno_result.Metadata_cell_id.unique()[0] == "A549"


def test_annotate_cp_clean():
    data_rename_df = data_df.rename(
        {"Metadata_Well": "Image_Metadata_Well"}, axis="columns"
    )
    data_rename_df = data_rename_df.assign(Image_Metadata_Plate="test")

    anno_result = annotate(
        profiles=data_rename_df,
        platemap=broad_platemap_df,
        clean_cellprofiler=False,
        join_on=["Metadata_well_position", "Image_Metadata_Well"],
    )

    assert all(
        [
            x in anno_result.columns
            for x in ["Image_Metadata_Well", "Image_Metadata_Plate"]
        ]
    )

    anno_result = annotate(
        profiles=data_rename_df,
        platemap=broad_platemap_df,
        clean_cellprofiler=True,
        join_on=["Metadata_well_position", "Image_Metadata_Well"],
    )

    assert all([x in anno_result.columns for x in ["Metadata_Well", "Metadata_Plate"]])
