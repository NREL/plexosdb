from __future__ import annotations

from pathlib import Path

import pytest


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
