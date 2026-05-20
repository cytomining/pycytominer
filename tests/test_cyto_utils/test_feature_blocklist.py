import pathlib

import pandas as pd
import pytest
import yaml

from pycytominer.cyto_utils.features import (
    Blocklist,
    blocklists_file,
    default_blocklist_name,
    get_blocklist_features,
)

packaged_blocklist_name = "default"

with pathlib.Path(blocklists_file).open() as blocklist_stream:
    blocklist = yaml.safe_load(blocklist_stream)[packaged_blocklist_name]

data_blocklist_df = pd.DataFrame({
    "Nuclei_Correlation_Manders_AGP_DNA": [1, 3, 8, 5, 2, 2],
    "Nuclei_Correlation_RWC_ER_RNA": [9, 3, 8, 9, 2, 9],
}).reset_index(drop=True)


@pytest.fixture
def dummy_blocklists_file(tmp_path):
    blocklists_file = tmp_path / "blocklists.yaml"
    blocklists_file.write_text(
        "\n".join([
            "custom:",
            "  - Cells_Custom",
            "  - Cytoplasm_Custom",
            "nuclear_blocklist:",
            "  - Nuclei_Custom",
            "correlation_blocklist:",
            "  - Nuclei_Correlation_Manders_AGP_DNA",
            "  - Nuclei_Correlation_RWC_ER_RNA",
            "",
        ]),
        encoding="utf-8",
    )

    return blocklists_file


def test_blocklist_no_args_uses_default():
    # With no arguments, the packaged default blocklist is used.
    blocklist_from_func = get_blocklist_features()
    assert blocklist_from_func == blocklist


def test_blocklist_none_name_uses_default():
    # Explicitly passing blocklist_name=None also falls back to the default.
    blocklist_from_func = get_blocklist_features(blocklist_name=None)
    assert blocklist_from_func == blocklist


def test_blocklist_explicit_name_not_overridden_by_default():
    # An explicitly provided blocklist_name is used as-is; the default fallback does not apply.
    # An empty list produces no features, distinguishing it from the non-empty default.
    blocklist_from_func = get_blocklist_features(blocklist_name=[])
    assert blocklist_from_func == []


def test_blocklist_df_no_args_filters_default_to_population():
    # With only a population_df, the default blocklist is filtered to matching columns.
    blocklist_from_func = get_blocklist_features(population_df=data_blocklist_df)
    assert blocklist_from_func == data_blocklist_df.columns.tolist()


def test_default_blocklist_df():
    blocklist_from_func = get_blocklist_features(
        blocklist_name=default_blocklist_name,
        population_df=data_blocklist_df,
    )

    assert default_blocklist_name == packaged_blocklist_name
    assert blocklist_from_func == data_blocklist_df.columns.tolist()


def test_named_blocklist_df():
    blocklist_from_func = get_blocklist_features(
        blocklist_name=packaged_blocklist_name,
        population_df=data_blocklist_df,
    )
    assert data_blocklist_df.columns.tolist() == blocklist_from_func


def test_empty_blocklist():
    blocklist_from_object = Blocklist()
    assert blocklist_from_object.to_list() == []


def test_empty_blocklist_does_not_load_blocklists_file(tmp_path):
    blocklist_from_object = Blocklist(blocklists_file=tmp_path / "missing.yaml")
    assert blocklist_from_object.to_list() == []


def test_named_blocklist():
    blocklist_from_object = Blocklist(blocklist_name=packaged_blocklist_name)
    assert blocklist == blocklist_from_object.to_list()


def test_named_blocklist_additional_features():
    blocklist_from_object = Blocklist(
        blocklist_name=packaged_blocklist_name, features_to_block=["Cells_Custom"]
    )
    assert blocklist_from_object.to_list() == [*blocklist, "Cells_Custom"]


def test_named_blocklist_from_dummy_file(dummy_blocklists_file):
    blocklist_from_object = Blocklist(
        blocklist_name="custom",
        blocklists_file=dummy_blocklists_file,
    )

    assert blocklist_from_object.to_list() == ["Cells_Custom", "Cytoplasm_Custom"]


def test_named_blocklists_from_dummy_file(dummy_blocklists_file):
    blocklist_from_object = Blocklist(
        blocklist_name=["custom", "nuclear_blocklist", "correlation_blocklist"],
        blocklists_file=dummy_blocklists_file,
    )

    assert blocklist_from_object.to_list() == [
        "Cells_Custom",
        "Cytoplasm_Custom",
        "Nuclei_Custom",
        "Nuclei_Correlation_Manders_AGP_DNA",
        "Nuclei_Correlation_RWC_ER_RNA",
    ]


def test_named_blocklists_from_dummy_file_filters_to_population_features(
    dummy_blocklists_file,
):
    blocklist_from_object = Blocklist(
        blocklist_name=["custom", "correlation_blocklist"],
        blocklists_file=dummy_blocklists_file,
    )
    blocklist_from_func = get_blocklist_features(
        blocklist=blocklist_from_object,
        population_df=data_blocklist_df,
    )

    assert blocklist_from_func == data_blocklist_df.columns.tolist()


def test_named_blocklist_converts_features_to_strings(tmp_path):
    blocklists_file = tmp_path / "blocklists.yaml"
    blocklists_file.write_text("custom:\n  - 1\n  - Cells_Custom\n", encoding="utf-8")

    blocklist_from_object = Blocklist(
        blocklist_name="custom", blocklists_file=blocklists_file
    )
    assert blocklist_from_object.to_list() == ["1", "Cells_Custom"]


def test_named_blocklist_requires_list_entry(tmp_path):
    blocklists_file = tmp_path / "blocklists.yaml"
    blocklists_file.write_text("custom: Cells_Custom\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a list of feature names"):
        Blocklist(blocklist_name="custom", blocklists_file=blocklists_file)


def test_blocklist_add_features():
    blocklist_from_object = Blocklist(features_to_block=["Cells_Custom"])
    blocklist_from_object.add(["Cells_Custom", "Nuclei_Custom"])
    assert blocklist_from_object.to_list() == [
        "Cells_Custom",
        "Cells_Custom",
        "Nuclei_Custom",
    ]


def test_blocklist_add_features_converts_to_strings():
    blocklist_from_object = Blocklist(features_to_block=[1])
    blocklist_from_object.add([2])
    assert blocklist_from_object.to_list() == ["1", "2"]


def test_blocklist_add_features_requires_list():
    blocklist_from_object = Blocklist()
    with pytest.raises(TypeError, match="requires a list"):
        blocklist_from_object.add("Cells_Custom")


def test_blocklist_object_filters_to_population_features():
    blocklist_from_object = Blocklist(
        features_to_block=["Nuclei_Correlation_Manders_AGP_DNA", "Cells_Custom"]
    )
    blocklist_from_func = get_blocklist_features(
        blocklist=blocklist_from_object,
        population_df=data_blocklist_df,
    )
    assert blocklist_from_func == ["Nuclei_Correlation_Manders_AGP_DNA"]


def test_blocklist_from_list():
    blocklist_from_func = get_blocklist_features(
        blocklist=["Nuclei_Correlation_Manders_AGP_DNA", "Cells_Custom"],
        population_df=data_blocklist_df,
    )
    assert blocklist_from_func == ["Nuclei_Correlation_Manders_AGP_DNA"]


def test_blocklist_features_requires_list_or_blocklist():
    with pytest.raises(TypeError, match="feature-name string, a list"):
        get_blocklist_features(blocklist=1)
