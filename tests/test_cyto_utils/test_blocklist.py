import pathlib
import warnings

import pandas as pd
import pytest
import yaml

from pycytominer.cyto_utils.blocklist import (
    DEFAULT_BLOCKLIST_NAME,
    Blocklist,
    blocklists_file,
    get_blocklist_features,
)

# Path to the old-style CSV blocklist file kept for backward compatibility.
_legacy_blocklist_file = (
    pathlib.Path(blocklists_file).parent.parent / "data" / "blocklist_features.txt"
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
        blocklist_name=DEFAULT_BLOCKLIST_NAME,
        population_df=data_blocklist_df,
    )

    assert packaged_blocklist_name == DEFAULT_BLOCKLIST_NAME
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


def test_normalize_blocklist_names_none():
    assert Blocklist._normalize_blocklist_names(None) == []


def test_normalize_blocklist_names_string():
    assert Blocklist._normalize_blocklist_names("default") == ["default"]


def test_normalize_blocklist_names_list():
    assert Blocklist._normalize_blocklist_names(["a", "b"]) == ["a", "b"]


def test_normalize_blocklist_names_list_converts_to_strings():
    assert Blocklist._normalize_blocklist_names([1, 2]) == ["1", "2"]


def test_normalize_blocklist_names_invalid_type():
    with pytest.raises(TypeError, match="string, a list of strings, or None"):
        Blocklist._normalize_blocklist_names(42)


def test_load_named_blocklist_returns_features(dummy_blocklists_file):
    result = Blocklist._load_named_blocklist("custom", dummy_blocklists_file)
    assert result == ["Cells_Custom", "Cytoplasm_Custom"]


def test_load_named_blocklist_converts_to_strings(tmp_path):
    f = tmp_path / "blocklists.yaml"
    f.write_text("custom:\n  - 1\n  - Cells_Custom\n", encoding="utf-8")
    assert Blocklist._load_named_blocklist("custom", f) == ["1", "Cells_Custom"]


def test_load_named_blocklist_unknown_name_raises(dummy_blocklists_file):
    with pytest.raises(ValueError, match="Unknown blocklist name"):
        Blocklist._load_named_blocklist("nonexistent", dummy_blocklists_file)


def test_load_named_blocklist_non_list_entry_raises(tmp_path):
    f = tmp_path / "blocklists.yaml"
    f.write_text("custom: Cells_Custom\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must be a list of feature names"):
        Blocklist._load_named_blocklist("custom", f)


def test_load_named_blocklist_non_mapping_registry_raises(tmp_path):
    f = tmp_path / "blocklists.yaml"
    f.write_text("- Cells_Custom\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must be a mapping"):
        Blocklist._load_named_blocklist("custom", f)


# ---------------------------------------------------------------------------
# Backward-compatibility: deprecated blocklist_file parameter
# ---------------------------------------------------------------------------


def test_blocklist_file_deprecation_warning():
    """Passing blocklist_file emits a DeprecationWarning."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        get_blocklist_features(blocklist_file=_legacy_blocklist_file)

    deprecation_warnings = [
        w for w in caught if issubclass(w.category, DeprecationWarning)
    ]
    assert len(deprecation_warnings) == 1
    assert "blocklist_file" in str(deprecation_warnings[0].message)


def test_blocklist_file_returns_same_features_as_default():
    """Legacy blocklist_file produces the same feature list as the packaged default."""
    legacy = get_blocklist_features(blocklist_file=_legacy_blocklist_file)
    default = get_blocklist_features()
    assert legacy == default


def test_blocklist_file_custom_csv(tmp_path):
    """A user-supplied CSV with a 'blocklist' column is read correctly."""
    csv_file = tmp_path / "my_blocklist.txt"
    csv_file.write_text("blocklist\nCells_Custom\nNuclei_Custom\n", encoding="utf-8")

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        result = get_blocklist_features(blocklist_file=csv_file)

    assert result == ["Cells_Custom", "Nuclei_Custom"]


def test_blocklist_file_missing_column_raises(tmp_path):
    """A CSV without a 'blocklist' column raises a clear ValueError."""
    csv_file = tmp_path / "bad.txt"
    csv_file.write_text("features\nCells_Custom\n", encoding="utf-8")

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        with pytest.raises(ValueError, match="column named 'blocklist'"):
            get_blocklist_features(blocklist_file=csv_file)


def test_blocklist_file_filters_to_population(tmp_path):
    """blocklist_file respects population_df filtering."""
    csv_file = tmp_path / "my_blocklist.txt"
    csv_file.write_text(
        "blocklist\nNuclei_Correlation_Manders_AGP_DNA\nCells_Custom\n",
        encoding="utf-8",
    )

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        result = get_blocklist_features(
            blocklist_file=csv_file,
            population_df=data_blocklist_df,
        )

    assert result == ["Nuclei_Correlation_Manders_AGP_DNA"]
