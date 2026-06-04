"""Minimal installed-package pytest checks for Pycytominer."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def test_minimal_install_imports() -> None:
    """Ensure core Pycytominer modules import in a minimal environment."""
    import pycytominer
    import pycytominer.cyto_utils

    assert pycytominer is not None
    assert pycytominer.cyto_utils is not None


def test_minimal_install_cli_aggregate(
    minimal_install_profiles_file: Path,
    tmp_path: Path,
) -> None:
    """Ensure the installed CLI can aggregate a tiny checked-in profiles file."""
    output_path = tmp_path / "aggregated.csv"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pycytominer.cli",
            "aggregate",
            f"--profiles={minimal_install_profiles_file}",
            f"--output_file={output_path}",
            "--strata=Metadata_Plate,Metadata_Well",
            "--features=Feature_1,Feature_2",
            "--operation=median",
        ],
        check=True,
    )

    result = pd.read_csv(output_path)
    assert result.shape[0] == 2
    assert np.isclose(
        result.loc[result["Metadata_Well"] == "A01", "Feature_1"].item(), 1.5
    )
    assert np.isclose(
        result.loc[result["Metadata_Well"] == "A02", "Feature_2"].item(), 7.5
    )


def test_minimal_install_feature_select_uses_packaged_blocklist(
    minimal_install_blocklist_profiles_file: Path,
    tmp_path: Path,
) -> None:
    """Ensure packaged blocklist data is available in the installed artifact."""
    from pycytominer import feature_select

    output_path = tmp_path / "feature_selected.csv"

    feature_select(
        profiles=str(minimal_install_blocklist_profiles_file),
        output_file=str(output_path),
        features=[
            "Nuclei_Correlation_Manders_AGP_DNA",
            "Cells_AreaShape_Area",
        ],
        operation="blocklist",
    )

    result = pd.read_csv(output_path)
    assert "Nuclei_Correlation_Manders_AGP_DNA" not in result.columns
    assert "Cells_AreaShape_Area" in result.columns
