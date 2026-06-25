"""Tests for the Pycytominer CLI."""

from __future__ import annotations

import pathlib

import fire
import numpy as np
import pandas as pd
import pytest

from pycytominer import cli as pycytominer_cli
from pycytominer.cli import PycytominerCLI, PycytominerCLIError


def _write_profiles(tmp_path: pathlib.Path) -> tuple[pd.DataFrame, pathlib.Path]:
    """Write a small profiles Parquet file for CLI tests.

    Parquet is used to cover the preferred profile storage format.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        The dataframe and the path to the saved Parquet file.
    """
    df = pd.DataFrame({
        "Metadata_Plate": ["P1", "P1", "P1", "P1"],
        "Metadata_Well": ["A01", "A01", "A02", "A02"],
        "Feature_1": [1.0, 2.0, 3.0, 4.0],
        "Feature_2": [5.0, 6.0, 7.0, 8.0],
        "Feature_3": [1.0, 1.0, 1.0, 1.0],
    })
    path = tmp_path / "profiles.parquet"
    df.to_parquet(path, index=False)
    return df, path


def test_cli_aggregate(tmp_path: pathlib.Path) -> None:
    """Ensure CLI aggregate reads a file and writes aggregated output."""
    _, profiles_path = _write_profiles(tmp_path)
    output_path = tmp_path / "aggregated.csv"

    cli = PycytominerCLI()
    cli.aggregate(
        profiles=str(profiles_path),
        output_file=str(output_path),
        strata="Metadata_Plate,Metadata_Well",
        features="Feature_1,Feature_2",
        operation="median",
    )

    result = pd.read_csv(output_path)
    assert result.shape[0] == 2
    assert np.isclose(
        result.loc[result["Metadata_Well"] == "A01", "Feature_1"].item(), 1.5
    )
    assert np.isclose(
        result.loc[result["Metadata_Well"] == "A02", "Feature_2"].item(), 7.5
    )


def test_cli_annotate(tmp_path: pathlib.Path) -> None:
    """Ensure CLI annotate merges platemap metadata and writes output."""
    _, profiles_path = _write_profiles(tmp_path)
    platemap = pd.DataFrame({
        "well_position": ["A01", "A02"],
        "Treatment": ["control", "drug"],
    })
    platemap_path = tmp_path / "platemap.csv"
    platemap.to_csv(platemap_path, index=False)

    output_path = tmp_path / "annotated.csv"
    cli = PycytominerCLI()
    cli.annotate(
        profiles=str(profiles_path),
        platemap=str(platemap_path),
        output_file=str(output_path),
        join_on="Metadata_well_position,Metadata_Well",
    )

    result = pd.read_csv(output_path)
    assert "Metadata_Treatment" in result.columns
    assert set(result["Metadata_Treatment"].unique()) == {"control", "drug"}


def test_cli_normalize(tmp_path: pathlib.Path) -> None:
    """Ensure CLI normalize writes standardized output."""
    _, profiles_path = _write_profiles(tmp_path)
    output_path = tmp_path / "normalized.csv"

    cli = PycytominerCLI()
    cli.normalize(
        profiles=str(profiles_path),
        output_file=str(output_path),
        features="Feature_1,Feature_2",
        meta_features="Metadata_Plate,Metadata_Well",
        method="standardize",
    )

    result = pd.read_csv(output_path)
    assert np.isclose(result["Feature_1"].mean(), 0.0, atol=1e-7)
    assert np.isclose(result["Feature_2"].mean(), 0.0, atol=1e-7)


def test_cli_normalize_drop_cosmicqc_rows(tmp_path: pathlib.Path) -> None:
    """Ensure CLI normalize forwards drop_cosmicqc_rows."""
    profiles = pd.DataFrame({
        "Metadata_Plate": ["P1"] * 4,
        "Metadata_Well": ["A01", "A02", "A03", "A04"],
        "Metadata_cqc_clustered_nuclei_is_outlier": [False, True, False, False],
        "Feature_1": [0.0, 1000.0, 1.0, 2.0],
        "Feature_2": [10.0, 9999.0, 20.0, 30.0],
    })
    profiles_path = tmp_path / "profiles_qc.parquet"
    profiles.to_parquet(profiles_path, index=False)
    output_path = tmp_path / "normalized_qc.csv"

    cli = PycytominerCLI()
    cli.normalize(
        profiles=str(profiles_path),
        output_file=str(output_path),
        features="Feature_1,Feature_2",
        meta_features="Metadata_Plate,Metadata_Well,Metadata_cqc_clustered_nuclei_is_outlier",
        method="standardize",
        drop_cosmicqc_rows=True,
    )

    result = pd.read_csv(output_path)
    assert result["Metadata_Well"].tolist() == ["A01", "A03", "A04"]
    assert result["Metadata_cqc_clustered_nuclei_is_outlier"].tolist() == [
        False,
        False,
        False,
    ]
    assert np.allclose(
        result["Feature_1"],
        [-1.224744871391589, 0.0, 1.224744871391589],
    )
    assert np.allclose(
        result["Feature_2"],
        [-1.224744871391589, 0.0, 1.224744871391589],
    )


def test_cli_feature_select(tmp_path: pathlib.Path) -> None:
    """Ensure CLI feature_select drops low-variance features."""
    _, profiles_path = _write_profiles(tmp_path)
    output_path = tmp_path / "feature_selected.csv"

    cli = PycytominerCLI()
    cli.feature_select(
        profiles=str(profiles_path),
        output_file=str(output_path),
        features="Feature_1,Feature_2,Feature_3",
        operation="variance_threshold",
    )

    result = pd.read_csv(output_path)
    assert "Feature_3" not in result.columns


def test_cli_consensus(tmp_path: pathlib.Path) -> None:
    """Ensure CLI consensus writes median consensus profiles."""
    _, profiles_path = _write_profiles(tmp_path)
    output_path = tmp_path / "consensus.csv"

    cli = PycytominerCLI()
    cli.consensus(
        profiles=str(profiles_path),
        output_file=str(output_path),
        replicate_columns="Metadata_Plate,Metadata_Well",
        features="Feature_1,Feature_2",
        operation="median",
    )

    result = pd.read_csv(output_path)
    assert result.shape[0] == 2
    assert np.isclose(
        result.loc[result["Metadata_Well"] == "A01", "Feature_2"].item(), 5.5
    )


def test_cli_accepts_sequence_inputs(tmp_path: pathlib.Path) -> None:
    """Ensure list-like inputs are accepted for CLI sequence arguments."""
    _, profiles_path = _write_profiles(tmp_path)
    output_path = tmp_path / "aggregate_sequence.csv"

    cli = PycytominerCLI()
    cli.aggregate(
        profiles=str(profiles_path),
        output_file=str(output_path),
        strata=["Metadata_Plate", "Metadata_Well"],
        features=["Feature_1", "Feature_2"],
    )

    result = pd.read_csv(output_path)
    assert result.shape[0] == 2
    assert set(result["Metadata_Well"]) == {"A01", "A02"}


def test_cli_annotate_join_on_sequence(tmp_path: pathlib.Path) -> None:
    """Ensure annotate accepts a sequence for join keys."""
    _, profiles_path = _write_profiles(tmp_path)
    platemap = pd.DataFrame({
        "well_position": ["A01", "A02"],
        "Treatment": ["control", "drug"],
    })
    platemap_path = tmp_path / "platemap.csv"
    platemap.to_csv(platemap_path, index=False)
    output_path = tmp_path / "annotated_sequence.csv"

    cli = PycytominerCLI()
    cli.annotate(
        profiles=str(profiles_path),
        platemap=str(platemap_path),
        output_file=str(output_path),
        join_on=["Metadata_well_position", "Metadata_Well"],
    )

    result = pd.read_csv(output_path)
    assert "Metadata_Treatment" in result.columns


def test_cli_raises_if_core_returns_dataframe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure defensive type checking errors when core returns a dataframe."""

    def _mock_aggregate(**_: object) -> pd.DataFrame:
        return pd.DataFrame({"x": [1.0]})

    def _mock_load_profiles(_: object) -> pd.DataFrame:
        return pd.DataFrame({"Metadata_Plate": ["P1"], "Feature_1": [1.0]})

    monkeypatch.setattr(pycytominer_cli, "aggregate", _mock_aggregate)
    monkeypatch.setattr(pycytominer_cli, "load_profiles", _mock_load_profiles)
    cli = PycytominerCLI()

    with pytest.raises(
        PycytominerCLIError,
        match=r"aggregate\(\) returned a DataFrame when a file path was expected.",
    ):
        cli.aggregate(
            profiles="fake.csv",
            output_file="out.csv",
            strata="Metadata_Plate",
            features=["Feature_1"],
        )


def test_cli_aggregate_passes_image_features(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, object] = {}

    def _mock_aggregate(**kwargs: object) -> str:
        captured_kwargs.update(kwargs)
        return "out.csv"

    def _mock_load_profiles(_: object) -> pd.DataFrame:
        return pd.DataFrame({"Metadata_Plate": ["P1"], "Feature_1": [1.0]})

    monkeypatch.setattr(pycytominer_cli, "aggregate", _mock_aggregate)
    monkeypatch.setattr(pycytominer_cli, "load_profiles", _mock_load_profiles)
    cli = PycytominerCLI()

    result = cli.aggregate(
        profiles="fake.csv",
        output_file="out.csv",
        strata="Metadata_Plate",
        features="infer",
        image_features=True,
    )

    assert result == "out.csv"
    assert captured_kwargs["image_features"] is True


def test_cli_unknown_argument_errors(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Ensure unknown Fire arguments fail with a non-zero exit status."""
    _, profiles_path = _write_profiles(tmp_path)
    output_path = tmp_path / "unknown_arg.csv"

    # set commands
    command = [
        "aggregate",
        f"--profiles={profiles_path}",
        f"--output_file={output_path}",
        "--features=Feature_1",
        "--not_a_real_argument=1",
    ]

    # use fire.Fire() to execute the command
    # useful for testing in-process execution in testing suites
    with pytest.raises(fire.core.FireExit) as exc_info:
        fire.Fire(PycytominerCLI, command=command)

    # check that the command failed with a non-zero exit status
    assert exc_info.value.code != 0
    captured = capsys.readouterr()
    error_text = f"{captured.out}\n{captured.err}"
    assert "not_a_real_argument" in error_text


def test_cli_propagates_file_not_found_error() -> None:
    """Ensure core function file loading errors propagate through CLI."""
    cli = PycytominerCLI()

    with pytest.raises(
        FileNotFoundError,
        match=r"load_profiles\(\) didn't find the path: .*missing_file\.csv\.",
    ):
        cli.normalize(
            profiles="missing_file.csv",
            output_file="unused.csv",
            features=["Feature_1"],
            meta_features=["Metadata_Plate"],
        )


def test_cli_csv_profiles(tmp_path: pathlib.Path) -> None:
    """CSV profiles load normally through the CLI on every platform."""
    profiles_path = tmp_path / "profiles.csv"
    df = pd.DataFrame({
        "Metadata_Plate": ["P1", "P1"],
        "Metadata_Well": ["A01", "A02"],
        "Feature_1": [1.0, 3.0],
        "Feature_2": [5.0, 7.0],
    })
    df.to_csv(profiles_path, index=False)
    output_path = tmp_path / "normalized.csv"

    cli = PycytominerCLI()
    cli.normalize(
        profiles=str(profiles_path),
        output_file=str(output_path),
        features="Feature_1,Feature_2",
        meta_features="Metadata_Plate,Metadata_Well",
        method="standardize",
    )

    result = pd.read_csv(output_path)
    assert np.isclose(result["Feature_1"].mean(), 0.0, atol=1e-7)
    assert np.isclose(result["Feature_2"].mean(), 0.0, atol=1e-7)


def _write_profiles_with_blocklist_features(tmp_path: pathlib.Path) -> pathlib.Path:
    """Write profiles containing one default-blocklisted and one non-blocklisted feature."""
    df = pd.DataFrame({
        "Metadata_Plate": ["P1", "P1"],
        # In the packaged default blocklist:
        "Nuclei_Correlation_Manders_AGP_DNA": [0.1, 0.2],
        # Not in the blocklist — generic feature:
        "Cells_AreaShape_Area": [100.0, 200.0],
    })
    path = tmp_path / "profiles_blocklist.parquet"
    df.to_parquet(path, index=False)
    return path


def test_cli_feature_select_blocklist_default(tmp_path: pathlib.Path) -> None:
    """CLI feature_select with operation=blocklist uses the packaged default when no blocklist args are given."""
    profiles_path = _write_profiles_with_blocklist_features(tmp_path)
    output_path = tmp_path / "out_default.csv"

    cli = PycytominerCLI()
    cli.feature_select(
        profiles=str(profiles_path),
        output_file=str(output_path),
        features="Nuclei_Correlation_Manders_AGP_DNA,Cells_AreaShape_Area",
        operation="blocklist",
    )

    result = pd.read_csv(output_path)
    assert "Nuclei_Correlation_Manders_AGP_DNA" not in result.columns
    assert "Cells_AreaShape_Area" in result.columns


def test_cli_feature_select_blocklist_name_default(tmp_path: pathlib.Path) -> None:
    """CLI feature_select with blocklist_name='default' is identical to the implicit default."""
    profiles_path = _write_profiles_with_blocklist_features(tmp_path)
    output_path = tmp_path / "out_named.csv"

    cli = PycytominerCLI()
    cli.feature_select(
        profiles=str(profiles_path),
        output_file=str(output_path),
        features="Nuclei_Correlation_Manders_AGP_DNA,Cells_AreaShape_Area",
        operation="blocklist",
        blocklist_name="default",
    )

    result = pd.read_csv(output_path)
    assert "Nuclei_Correlation_Manders_AGP_DNA" not in result.columns
    assert "Cells_AreaShape_Area" in result.columns


def test_cli_feature_select_blocklist_explicit(tmp_path: pathlib.Path) -> None:
    """CLI feature_select drops features listed explicitly via blocklist (comma-delimited string)."""
    _, profiles_path = _write_profiles(tmp_path)
    output_path = tmp_path / "out_explicit.csv"

    cli = PycytominerCLI()
    cli.feature_select(
        profiles=str(profiles_path),
        output_file=str(output_path),
        features="Feature_1,Feature_2,Feature_3",
        operation="blocklist",
        blocklist="Feature_1,Feature_3",
    )

    result = pd.read_csv(output_path)
    assert "Feature_1" not in result.columns
    assert "Feature_3" not in result.columns
    assert "Feature_2" in result.columns
