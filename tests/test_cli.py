"""Tests for the Pycytominer CLI."""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd

from pycytominer.cli import PycytominerCLI


def _write_profiles(tmp_path: pathlib.Path) -> tuple[pd.DataFrame, pathlib.Path]:
    """Write a small profiles CSV for CLI tests.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        The dataframe and the path to the saved CSV.
    """
    df = pd.DataFrame({
        "Metadata_Plate": ["P1", "P1", "P1", "P1"],
        "Metadata_Well": ["A01", "A01", "A02", "A02"],
        "Feature_1": [1.0, 2.0, 3.0, 4.0],
        "Feature_2": [5.0, 6.0, 7.0, 8.0],
        "Feature_3": [1.0, 1.0, 1.0, 1.0],
    })
    path = tmp_path / "profiles.csv"
    df.to_csv(path, index=False)
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
