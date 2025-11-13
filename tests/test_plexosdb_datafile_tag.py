"""Tests for adding DataFile references through tags."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from plexosdb import PlexosDB


def test_add_datafile_tag_to_property_succeeds(
    db_with_topology: PlexosDB, tmp_path: Path, create_profile_csv: callable
) -> None:
    """Test that a DataFile tag can be added to a property."""
    from plexosdb import ClassEnum

    data_file: Path = tmp_path / "thermal_rating.csv"
    create_profile_csv(data_file, "thermal-01", [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.0, 0.9, 0.8, 0.7, 0.6, 0.5])

    datafile_name: str = "ThermalRatingProfile"
    datafile_id: int = db_with_topology.add_object(ClassEnum.DataFile, datafile_name)
    db_with_topology.add_property(
        ClassEnum.DataFile, datafile_name, "Filename", 0.0, datafile_text=str(data_file), band=1
    )

    property_data_id: int = db_with_topology.add_property(
        ClassEnum.Generator, "thermal-01", "Max Capacity", 100.0, band=1
    )

    result: int = db_with_topology.add_datafile_tag(property_data_id, str(data_file))

    assert result is not None
    assert db_with_topology.check_tag_exists(property_data_id, datafile_id)


def test_add_datafile_tag_with_description_succeeds(
    db_with_topology: PlexosDB, tmp_path: Path, create_profile_csv: callable
) -> None:
    """Test that a DataFile tag with description can be added to a property."""
    from plexosdb import ClassEnum

    data_file: Path = tmp_path / "solar_rating.csv"
    create_profile_csv(data_file, "solar-01", [0.2, 0.3, 0.4, 0.5, 0.6, 0.8, 0.9, 0.8, 0.7, 0.5, 0.3, 0.1])

    datafile_name: str = "SolarRatingProfile"
    datafile_id: int = db_with_topology.add_object(ClassEnum.DataFile, datafile_name)
    db_with_topology.add_property(
        ClassEnum.DataFile, datafile_name, "Filename", 0.0, datafile_text=str(data_file), band=1
    )

    property_data_id: int = db_with_topology.add_property(
        ClassEnum.Generator, "solar-01", "Rating", 0.0, band=1
    )

    result: int = db_with_topology.add_datafile_tag(
        property_data_id, str(data_file), description="Solar generation capacity factors"
    )

    assert result is not None
    assert db_with_topology.check_tag_exists(property_data_id, datafile_id)


def test_add_datafile_tag_creates_link_between_property_and_datafile(
    db_with_topology: PlexosDB, tmp_path: Path, create_profile_csv: callable
) -> None:
    """Test that DataFile tag properly links property to DataFile object."""
    from plexosdb import ClassEnum

    data_file: Path = tmp_path / "wind_rating.csv"
    create_profile_csv(data_file, "wind-01", [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.8, 0.6, 0.4])

    datafile_name: str = "WindRatingProfile"
    datafile_id: int = db_with_topology.add_object(ClassEnum.DataFile, datafile_name)
    db_with_topology.add_property(
        ClassEnum.DataFile, datafile_name, "Filename", 0.0, datafile_text=str(data_file), band=1
    )

    property_data_id: int = db_with_topology.add_property(
        ClassEnum.Generator, "wind-01", "Rating", 0.0, band=1
    )

    db_with_topology.add_datafile_tag(property_data_id, str(data_file))

    assert db_with_topology.check_tag_exists(property_data_id, datafile_id)


def test_add_datafile_tag_to_multiple_properties_succeeds(
    db_with_topology: PlexosDB, tmp_path: Path, create_profile_csv: callable
) -> None:
    """Test that the same DataFile can be tagged to multiple properties."""
    from plexosdb import ClassEnum

    data_file: Path = tmp_path / "shared_profile.csv"
    create_profile_csv(data_file, "thermal-01", [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.0, 0.9, 0.8, 0.7, 0.6, 0.5])

    datafile_name: str = "SharedRatingData"
    datafile_id: int = db_with_topology.add_object(ClassEnum.DataFile, datafile_name)
    db_with_topology.add_property(
        ClassEnum.DataFile, datafile_name, "Filename", 0.0, datafile_text=str(data_file), band=1
    )

    thermal_prop_id: int = db_with_topology.add_property(
        ClassEnum.Generator, "thermal-01", "Rating", 0.0, band=1
    )
    solar_prop_id: int = db_with_topology.add_property(ClassEnum.Generator, "solar-01", "Rating", 0.0, band=1)

    db_with_topology.add_datafile_tag(thermal_prop_id, str(data_file))
    db_with_topology.add_datafile_tag(solar_prop_id, str(data_file))

    assert db_with_topology.check_tag_exists(thermal_prop_id, datafile_id)
    assert db_with_topology.check_tag_exists(solar_prop_id, datafile_id)


def test_add_datafile_tag_to_property_with_different_object_types(
    db_with_topology: PlexosDB, tmp_path: Path, create_profile_csv: callable
) -> None:
    """Test that DataFile tags can be added to properties of different object types."""
    from plexosdb import ClassEnum

    data_file: Path = tmp_path / "node_load.csv"
    create_profile_csv(
        data_file,
        "node-01",
        [100.0, 120.0, 140.0, 160.0, 180.0, 200.0, 200.0, 180.0, 160.0, 140.0, 120.0, 100.0],
    )

    datafile_name: str = "NodeLoadProfile"
    datafile_id: int = db_with_topology.add_object(ClassEnum.DataFile, datafile_name)
    db_with_topology.add_property(
        ClassEnum.DataFile, datafile_name, "Filename", 0.0, datafile_text=str(data_file), band=1
    )

    node_prop_id: int = db_with_topology.add_property(ClassEnum.Node, "node-01", "Load", 100.0, band=1)

    result: int = db_with_topology.add_datafile_tag(node_prop_id, str(data_file))

    assert result is not None
    assert db_with_topology.check_tag_exists(node_prop_id, datafile_id)


def test_add_datafile_tag_with_nonexistent_property_data_id_fails(
    db_with_topology: PlexosDB, tmp_path: Path, create_profile_csv: callable
) -> None:
    """Test that adding a tag with invalid property data_id fails appropriately."""
    from plexosdb import ClassEnum

    data_file: Path = tmp_path / "test_profile.csv"
    create_profile_csv(data_file, "thermal-01", [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.0, 0.9, 0.8, 0.7, 0.6, 0.5])

    db_with_topology.add_object(ClassEnum.DataFile, "InvalidTestDataFile")

    with pytest.raises(Exception):
        db_with_topology.add_datafile_tag(9999, str(data_file))


def test_add_datafile_tag_returns_int_result(
    db_with_topology: PlexosDB, tmp_path: Path, create_profile_csv: callable
) -> None:
    """Test that add_datafile_tag returns an integer result."""
    from plexosdb import ClassEnum

    data_file: Path = tmp_path / "capacity_profile.csv"
    create_profile_csv(data_file, "wind-01", [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.8, 0.6, 0.4])

    datafile_name: str = "CapacityProfile"
    _: int = db_with_topology.add_object(ClassEnum.DataFile, datafile_name)
    db_with_topology.add_property(
        ClassEnum.DataFile, datafile_name, "Filename", 0.0, datafile_text=str(data_file), band=1
    )

    property_data_id: int = db_with_topology.add_property(
        ClassEnum.Generator, "thermal-01", "Max Capacity", 100.0, band=1
    )

    result: int = db_with_topology.add_datafile_tag(property_data_id, str(data_file))

    assert result is not None
    assert isinstance(result, int)
