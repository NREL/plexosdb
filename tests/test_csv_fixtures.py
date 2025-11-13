"""Tests for CSV fixture functions that create test data files."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    pass


def test_datetime_single_component_data_creates_file(
    tmp_path: Path, datetime_single_component_data: callable
) -> None:
    """Test that datetime_single_component_data creates a valid CSV file."""
    result: Path = datetime_single_component_data("solar-01", datetime(2024, 1, 1), 1, [0.5, 0.6])
    assert result.exists()
    assert result.is_file()


def test_datetime_single_component_data_header_format(
    tmp_path: Path, datetime_single_component_data: callable
) -> None:
    """Test datetime_single_component_data creates correct CSV header."""
    result: Path = datetime_single_component_data("test-gen", datetime(2024, 1, 1), 1, [0.5] * 24)
    content = result.read_text()
    lines = content.strip().split("\n")
    assert lines[0] == "DateTime,Component,Value"


def test_datetime_single_component_data_hourly_data(
    tmp_path: Path, datetime_single_component_data: callable
) -> None:
    """Test datetime_single_component_data generates 24 hourly records per day."""
    result: Path = datetime_single_component_data("wind-01", datetime(2024, 1, 1), 2, [0.5] * 24)
    content = result.read_text()
    lines = content.strip().split("\n")
    # Header + 48 hours (2 days * 24 hours)
    assert len(lines) == 49


def test_datetime_single_component_data_datetime_format(
    tmp_path: Path, datetime_single_component_data: callable
) -> None:
    """Test datetime_single_component_data uses ISO format for datetimes."""
    result: Path = datetime_single_component_data("thermal-01", datetime(2024, 1, 1), 1, [0.5] * 24)
    content = result.read_text()
    lines = content.strip().split("\n")
    first_row = lines[1].split(",")

    assert "2024-01-01" in first_row[0]
    assert "T" in first_row[0]


def test_datetime_single_component_data_component_name_included(
    tmp_path: Path, datetime_single_component_data: callable
) -> None:
    """Test datetime_single_component_data includes component name in all rows."""
    component = "test-component"
    result: Path = datetime_single_component_data(component, datetime(2024, 1, 1), 1, [0.5] * 24)
    content = result.read_text()
    lines = content.strip().split("\n")

    for line in lines[1:]:
        assert component in line


def test_datetime_single_component_data_profile_values(
    tmp_path: Path, datetime_single_component_data: callable
) -> None:
    """Test datetime_single_component_data uses provided profile values."""
    profile = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
    result: Path = datetime_single_component_data("solar-01", datetime(2024, 1, 1), 1, profile)
    content = result.read_text()
    lines = content.strip().split("\n")

    # Extract values from first 6 rows
    values = [float(line.split(",")[2]) for line in lines[1:7]]
    assert values == profile


def test_datetime_single_component_data_date_progression(
    tmp_path: Path, datetime_single_component_data: callable
) -> None:
    """Test datetime_single_component_data correctly spans multiple days."""
    result: Path = datetime_single_component_data("wind-01", datetime(2024, 1, 1), 3, [0.5] * 24)
    content = result.read_text()
    lines = content.strip().split("\n")

    first_datetime = lines[1].split(",")[0]
    last_datetime = lines[-1].split(",")[0]

    assert "2024-01-01" in first_datetime
    assert "2024-01-03" in last_datetime


def test_datetime_single_component_data_returns_path(
    tmp_path: Path, datetime_single_component_data: callable
) -> None:
    """Test datetime_single_component_data returns Path object."""
    result = datetime_single_component_data("test", datetime(2024, 1, 1), 1, [0.5])
    assert isinstance(result, Path)


def test_monthly_component_data_creates_file(tmp_path: Path, monthly_component_data: callable) -> None:
    """Test that monthly_component_data creates a valid CSV file."""
    data = {"TestGen": {"M01": 0.5, "M02": 0.6}}
    result: Path = monthly_component_data(data)
    assert result.exists()
    assert result.is_file()


def test_monthly_component_data_header_format(tmp_path: Path, monthly_component_data: callable) -> None:
    """Test monthly_component_data creates correct CSV header."""
    data = {"TestGen": {"M01": 0.5}}
    result: Path = monthly_component_data(data)
    content = result.read_text()
    lines = content.strip().split("\n")
    assert lines[0] == "Month,Component,Value"


def test_monthly_component_data_multiple_components(tmp_path: Path, monthly_component_data: callable) -> None:
    """Test monthly_component_data handles multiple components."""
    data = {
        "Gen1": {"M01": 0.5, "M02": 0.6},
        "Gen2": {"M01": 0.4, "M02": 0.5},
    }
    result: Path = monthly_component_data(data)
    content = result.read_text()

    assert "Gen1" in content
    assert "Gen2" in content
    assert "M01" in content
    assert "M02" in content


def test_monthly_component_data_all_months_included(tmp_path: Path, monthly_component_data: callable) -> None:
    """Test monthly_component_data includes all months across all components."""
    data = {
        "Gen1": {"M01": 0.5, "M02": 0.6},
        "Gen2": {"M03": 0.7},
    }
    result: Path = monthly_component_data(data)
    content = result.read_text()

    assert "M01" in content
    assert "M02" in content
    assert "M03" in content


def test_monthly_component_data_contains_values(tmp_path: Path, monthly_component_data: callable) -> None:
    """Test monthly_component_data includes correct numeric values."""
    data = {"TestGen": {"M01": 25.5, "M02": 35.75}}
    result: Path = monthly_component_data(data)
    content = result.read_text()

    assert "25.5" in content
    assert "35.75" in content


def test_monthly_component_data_returns_path(tmp_path: Path, monthly_component_data: callable) -> None:
    """Test monthly_component_data returns Path object."""
    result = monthly_component_data({"Gen": {"M01": 0.5}})
    assert isinstance(result, Path)


def test_monthly_component_data_empty_input(tmp_path: Path, monthly_component_data: callable) -> None:
    """Test monthly_component_data handles empty input gracefully."""
    result: Path = monthly_component_data({})
    content = result.read_text()
    lines = content.strip().split("\n")

    # Should only have header
    assert len(lines) == 1
    assert lines[0] == "Month,Component,Value"


def test_multi_year_data_file_returns_list(tmp_path: Path, multi_year_data_file: list[Path]) -> None:
    """Test that multi_year_data_file returns a list of paths."""
    assert isinstance(multi_year_data_file, list)
    assert len(multi_year_data_file) > 0


def test_multi_year_data_file_creates_three_files(tmp_path: Path, multi_year_data_file: list[Path]) -> None:
    """Test multi_year_data_file creates 3 data files for 3 years."""
    assert len(multi_year_data_file) == 3


def test_multi_year_data_file_all_exist(tmp_path: Path, multi_year_data_file: list[Path]) -> None:
    """Test all created files exist."""
    for data_file in multi_year_data_file:
        assert data_file.exists()
        assert data_file.is_file()


def test_multi_year_data_file_header_format(tmp_path: Path, multi_year_data_file: list[Path]) -> None:
    """Test multi_year_data_file creates correct CSV header."""
    for data_file in multi_year_data_file:
        content = data_file.read_text()
        lines = content.strip().split("\n")
        assert lines[0] == "Date,Value"


def test_multi_year_data_file_row_count(tmp_path: Path, multi_year_data_file: list[Path]) -> None:
    """Test multi_year_data_file creates 365 data rows per file."""
    for data_file in multi_year_data_file:
        content = data_file.read_text()
        lines = content.strip().split("\n")
        # Header + 365 days
        assert len(lines) == 366


def test_multi_year_data_file_contains_dates(tmp_path: Path, multi_year_data_file: list[Path]) -> None:
    """Test multi_year_data_file rows contain ISO format dates."""
    for data_file in multi_year_data_file:
        content = data_file.read_text()
        lines = content.strip().split("\n")

        # Check first data row
        first_row = lines[1].split(",")
        assert "T00:00:00" in first_row[0]


def test_multi_year_data_file_contains_numeric_values(
    tmp_path: Path, multi_year_data_file: list[Path]
) -> None:
    """Test multi_year_data_file contains numeric values."""
    for data_file in multi_year_data_file:
        content = data_file.read_text()
        lines = content.strip().split("\n")

        # Check first 5 data rows contain numeric values
        for line in lines[1:6]:
            parts = line.split(",")
            value = float(parts[1])
            assert isinstance(value, float)


def test_multi_year_data_file_returns_paths(tmp_path: Path, multi_year_data_file: list[Path]) -> None:
    """Test multi_year_data_file returns list of Path objects."""
    for data_file in multi_year_data_file:
        assert isinstance(data_file, Path)


def test_multi_year_data_file_different_files(tmp_path: Path, multi_year_data_file: list[Path]) -> None:
    """Test multi_year_data_file creates distinct files for different years."""
    file_names = [f.name for f in multi_year_data_file]

    assert "data_2022.csv" in file_names
    assert "data_2023.csv" in file_names
    assert "data_2024.csv" in file_names


def test_create_profile_csv_creates_file(tmp_path: Path, create_profile_csv: callable) -> None:
    """Test that create_profile_csv creates a valid CSV file."""
    data_file: Path = tmp_path / "profile.csv"
    create_profile_csv(data_file, "test-gen", [0.5, 0.6])

    assert data_file.exists()
    assert data_file.is_file()


def test_create_profile_csv_header(tmp_path: Path, create_profile_csv: callable) -> None:
    """Test create_profile_csv creates correct header."""
    data_file: Path = tmp_path / "profile.csv"
    create_profile_csv(data_file, "test-gen", [0.5, 0.6])

    content = data_file.read_text()
    lines = content.strip().split("\n")
    assert lines[0] == "Component,Value"


def test_create_profile_csv_data_format(tmp_path: Path, create_profile_csv: callable) -> None:
    """Test create_profile_csv creates correct data format."""
    data_file: Path = tmp_path / "profile.csv"
    profile = [0.5, 0.6, 0.7]
    create_profile_csv(data_file, "thermal-01", profile)

    content = data_file.read_text()
    lines = content.strip().split("\n")

    assert lines[1] == "thermal-01,0.5"
    assert lines[2] == "thermal-01,0.6"
    assert lines[3] == "thermal-01,0.7"
