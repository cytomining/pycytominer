import pathlib

import pandas as pd
import yaml

from pycytominer.cyto_utils.features import (
    Blocklist,
    blocklists_file,
    get_blocklist_features,
)

with pathlib.Path(blocklists_file).open() as blocklist_stream:
    blocklist = yaml.safe_load(blocklist_stream)["default"]

data_blocklist_df = pd.DataFrame({
    "Nuclei_Correlation_Manders_AGP_DNA": [1, 3, 8, 5, 2, 2],
    "Nuclei_Correlation_RWC_ER_RNA": [9, 3, 8, 9, 2, 9],
}).reset_index(drop=True)


def test_blocklist():
    blocklist_from_func = get_blocklist_features()
    assert blocklist == blocklist_from_func


def test_blocklist_df():
    blocklist_from_func = get_blocklist_features(population_df=data_blocklist_df)
    assert data_blocklist_df.columns.tolist() == blocklist_from_func


def test_empty_blocklist():
    blocklist_from_object = Blocklist()
    assert blocklist_from_object.to_list() == []


def test_named_blocklist():
    blocklist_from_object = Blocklist(blocklist_name="default")
    assert blocklist == blocklist_from_object.to_list()


def test_named_blocklist_additional_features():
    blocklist_from_object = Blocklist(
        blocklist_name="default", extra_features=["Cells_Custom"]
    )
    assert blocklist_from_object.to_list() == [*blocklist, "Cells_Custom"]


def test_blocklist_add_features():
    blocklist_from_object = Blocklist(extra_features=["Cells_Custom"])
    blocklist_from_object.add(["Cells_Custom", "Nuclei_Custom"])
    assert blocklist_from_object.to_list() == ["Cells_Custom", "Nuclei_Custom"]


def test_blocklist_object_filters_to_population_features():
    blocklist_from_object = Blocklist(
        extra_features=["Nuclei_Correlation_Manders_AGP_DNA", "Cells_Custom"]
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
