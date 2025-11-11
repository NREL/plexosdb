from __future__ import annotations

from pathlib import Path

import pytest


def pytest_generate_tests(metafunc):
    """
    Parametrize _master_xml_param fixture with XML versions.
    The v9.2R6 version is marked with 'fast' for quick testing.
    Run with -m fast to only test the v9.2R6 version.
    Run without -m fast to test all three XML versions.
    """
    if "_master_xml_param" in metafunc.fixturenames:
        params = [
            pytest.param("v9.2R6", marks=pytest.mark.fast),
            "v10.0R2",
            "v11.0R4",
        ]

        metafunc.parametrize("_master_xml_param", params, indirect=True)


@pytest.fixture
def create_profile_csv() -> callable:
    """Fixture that provides a function to create CSV files with component and hourly profile values."""

    def _create_profile_csv(data_file: Path, component_name: str, profile: list[float]) -> None:
        """Create a CSV file with component and hourly profile values.

        Args:
            data_file: Path where the CSV file will be created
            component_name: Name of the component (e.g., 'thermal-01', 'solar-01')
            profile: List of float values representing hourly data
        """
        lines: list[str] = ["Component,Value"]
        for value in profile:
            lines.append(f"{component_name},{value}")
        data_file.write_text("\n".join(lines))

    return _create_profile_csv
