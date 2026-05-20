import pathlib

import pandas as pd
import pytest
import yaml

from pycytominer.cyto_utils.features import (
    Blocklist,
    blocklists_file,
    get_blocklist_features,
)

packaged_blocklist_name = "nuclei_corr_and_granularity"

with pathlib.Path(blocklists_file).open() as blocklist_stream:
    blocklist = yaml.safe_load(blocklist_stream)[packaged_blocklist_name]

data_blocklist_df = pd.DataFrame({
    "Nuclei_Correlation_Manders_AGP_DNA": [1, 3, 8, 5, 2, 2],
    "Nuclei_Correlation_RWC_ER_RNA": [9, 3, 8, 9, 2, 9],
}).reset_index(drop=True)


def test_blocklist():
    blocklist_from_func = get_blocklist_features()
    assert blocklist_from_func == []


def test_blocklist_df():
    blocklist_from_func = get_blocklist_features(population_df=data_blocklist_df)
    assert blocklist_from_func == []


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
    with pytest.raises(TypeError, match="list of feature names or a Blocklist"):
        get_blocklist_features(blocklist="Nuclei_Correlation_Manders_AGP_DNA")
