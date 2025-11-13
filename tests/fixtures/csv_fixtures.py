from __future__ import annotations

from datetime import datetime, timedelta
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


@pytest.fixture
def datetime_single_component_data(tmp_path: Path) -> callable:
    """Fixture that provides a function to create datetime-indexed CSV files with single component profiles.

    Creates CSV files with hourly datetime index and a single component's profile values.
    """

    def _create_datetime_csv(
        component_name: str,
        start_date: datetime,
        days: int,
        profile: list[float],
    ) -> Path:
        """Create a CSV file with datetime index and component profile values.

        Args:
            component_name: Name of the component (e.g., 'solar-01', 'wind-01')
            start_date: Starting date for the time series
            days: Number of days to generate data for
            profile: List of hourly profile values (will be repeated for each day)

        Returns
        -------
            Path to the created CSV file
        """
        from datetime import timedelta

        data_file = tmp_path / f"{component_name}_profile.csv"

        lines: list[str] = ["DateTime,Component,Value"]
        current_date = start_date
        profile_index = 0
        hourly_count = 0

        for _ in range(days):
            for hour in range(24):
                datetime_str = (current_date + timedelta(hours=hour)).isoformat()
                value = profile[profile_index % len(profile)]
                lines.append(f"{datetime_str},{component_name},{value}")
                profile_index += 1
                hourly_count += 1
            current_date += timedelta(days=1)

        data_file.write_text("\n".join(lines))
        return data_file

    return _create_datetime_csv


@pytest.fixture
def monthly_component_data(tmp_path: Path) -> callable:
    """Fixture that provides a function to create CSV files with monthly component data."""

    def _create_monthly_csv(monthly_data: dict[str, dict[str, float]]) -> Path:
        """Create a CSV file with monthly aggregated data for multiple components.

        Args:
            monthly_data: Dictionary mapping component names to dictionaries of monthly values
                         e.g., {"TestGen": {"M01": 0.5, "M02": 0.6, ...}}

        Returns
        -------
            Path to the created CSV file
        """
        data_file = tmp_path / "monthly_data.csv"

        # Get all unique months from the data
        all_months = set()
        for component_dict in monthly_data.values():
            all_months.update(component_dict.keys())
        months = sorted(all_months)

        lines: list[str] = ["Month,Component,Value"]
        for component_name, component_dict in monthly_data.items():
            for month in months:
                if month in component_dict:
                    value = component_dict[month]
                    lines.append(f"{month},{component_name},{value}")

        data_file.write_text("\n".join(lines))
        return data_file

    return _create_monthly_csv


@pytest.fixture
def multi_year_data_file(tmp_path: Path) -> list[Path]:
    """Fixture that provides multiple years of CSV data files."""
    data_files = []

    for year in range(2022, 2025):
        from datetime import datetime

        data_file = tmp_path / f"data_{year}.csv"

        lines: list[str] = ["Date,Value"]
        start_date = datetime(year, 1, 1)

        # Create 365 days of data for each year
        for day_offset in range(365):
            current_date = start_date + timedelta(days=day_offset)
            # Simple value based on day of year
            value = 50.0 + (day_offset % 100) * 0.5
            lines.append(f"{current_date.isoformat()},{value}")

        data_file.write_text("\n".join(lines))
        data_files.append(data_file)

    return data_files
