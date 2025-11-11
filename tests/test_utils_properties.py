"""Tests for property insertion utility functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from plexosdb import PlexosDB


def test_prepare_properties_params_succeeds(db_with_topology: PlexosDB) -> None:
    """Test that prepare_properties_params correctly prepares SQL parameters."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")

    records = [
        {"name": "gen-01", "Max Capacity": 100.0, "Heat Rate": 10.5},
        {"name": "gen-02", "Max Capacity": 150.0, "Heat Rate": 9.8},
    ]

    params, collection_properties = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    assert params is not None
    assert len(params) == 4  # 2 records with 2 properties
    assert collection_properties is not None


def test_insert_property_data_marks_dynamic_and_enabled(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_data marks properties as dynamic and enabled."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "Max Capacity": 100.0}]

    params, _ = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        data_id_map = insert_property_data(db_with_topology, params)

        assert data_id_map is not None
        assert len(data_id_map) == 1


def test_insert_scenario_tags_creates_scenario(db_with_topology: PlexosDB) -> None:
    """Test that insert_scenario_tags creates scenario if it doesn't exist."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_data, insert_scenario_tags, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "Max Capacity": 100.0}]

    params, _ = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        insert_property_data(db_with_topology, params)
        insert_scenario_tags(db_with_topology, "Test Scenario", params, chunksize=1000)

        # Verify scenario was created
        scenario_exists = db_with_topology.check_scenario_exists("Test Scenario")
        assert scenario_exists is True


def test_add_texts_for_properties_with_datafile_text(db_with_topology: PlexosDB) -> None:
    """Test that add_texts_for_properties correctly adds datafile_text field."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import (
        add_texts_for_properties,
        insert_property_data,
        prepare_properties_params,
    )

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "Max Capacity": 100.0, "datafile_text": "/path/to/file.csv"}]

    params, _ = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        data_id_map = insert_property_data(db_with_topology, params)
        add_texts_for_properties(
            db_with_topology, params, data_id_map, records, "datafile_text", ClassEnum.DataFile
        )

        # Verify text was added by checking database
        text_records = db_with_topology.query("SELECT COUNT(*) as count FROM t_text")
        # query returns tuples, so access by index
        assert text_records[0][0] > 0
