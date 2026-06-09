"""Shared pytest fixtures for minimal wheel build tests."""

from pathlib import Path

import pytest


@pytest.fixture(name="minimal_wheel_build_test_data_dir")
def fixture_minimal_wheel_build_test_data_dir() -> Path:
    """
    Provide a data directory for minimal wheel build test data
    """

    return Path(__file__).resolve().parent.parent / "test_data" / "minimal_install"


@pytest.fixture(name="minimal_install_profiles_file")
def fixture_minimal_install_profiles_file(
    minimal_wheel_build_test_data_dir: Path,
) -> Path:
    """
    Provide a profiles input file for minimal wheel build tests
    """

    return minimal_wheel_build_test_data_dir / "profiles.parquet"


@pytest.fixture(name="minimal_install_blocklist_profiles_file")
def fixture_minimal_install_blocklist_profiles_file(
    minimal_wheel_build_test_data_dir: Path,
) -> Path:
    """
    Provide a blocklist-focused profiles input file for minimal wheel build tests
    """

    return minimal_wheel_build_test_data_dir / "profiles_blocklist.parquet"
