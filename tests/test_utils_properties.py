"""Tests for property insertion utility functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

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


def test_prepare_properties_params_raises_error_when_no_memberships(db_with_topology: PlexosDB) -> None:
    """Test that prepare_properties_params raises error when objects don't exist."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.exceptions import NotFoundError
    from plexosdb.utils import prepare_properties_params

    # Don't add objects to database - they should not exist
    records = [
        {"name": "nonexistent-gen-01", "Max Capacity": 100.0},
        {"name": "nonexistent-gen-02", "Max Capacity": 150.0},
    ]

    # Raises NotFoundError when objects don't exist in database
    with pytest.raises(NotFoundError):
        prepare_properties_params(
            db_with_topology,
            records,
            ClassEnum.Generator,
            CollectionEnum.Generators,
            ClassEnum.System,
        )


def test_prepare_properties_params_empty_collection_properties(db_with_topology: PlexosDB) -> None:
    """Test prepare_properties_params with valid objects but no properties in collection."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    # Records with properties that don't exist in the collection
    records = [
        {"name": "gen-01", "NonexistentProperty": 100.0},
    ]

    params, collection_properties = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    # Should return empty params since property doesn't exist in collection
    assert params == []
    assert collection_properties is not None


def test_prepare_properties_params_multiple_records_single_valid(db_with_topology: PlexosDB) -> None:
    """Test prepare_properties_params with multiple records but only some have valid properties."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")

    records = [
        {"name": "gen-01", "Max Capacity": 100.0},
        {"name": "gen-02", "NonexistentProperty": 150.0},
    ]

    params, _ = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    # Should only have params for gen-01 with Max Capacity
    assert len(params) >= 1
    assert all(param[2] is not None for param in params)


def test_prepare_properties_params_return_structure(db_with_topology: PlexosDB) -> None:
    """Test that prepare_properties_params returns correct tuple structure."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "Max Capacity": 100.0}]

    result = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    # Check that result is a tuple with 2 elements
    assert isinstance(result, tuple)
    assert len(result) == 2

    params, collection_properties = result

    # Check params structure
    assert isinstance(params, list)
    if len(params) > 0:
        assert isinstance(params[0], tuple)
        assert len(params[0]) == 3  # (membership_id, property_id, value)

    # Check collection_properties structure
    assert isinstance(collection_properties, list)


def test_insert_property_data_updates_multiple_properties(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_data marks multiple properties as dynamic and enabled."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")

    records = [
        {"name": "gen-01", "Max Capacity": 100.0, "Heat Rate": 10.5},
        {"name": "gen-02", "Max Capacity": 150.0, "Heat Rate": 9.8},
    ]

    params, _ = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        insert_property_data(db_with_topology, params)

        # Verify properties are marked as dynamic and enabled
        properties = db_with_topology.query(
            "SELECT property_id, is_dynamic, is_enabled FROM t_property WHERE is_dynamic=1 AND is_enabled=1"
        )
        assert len(properties) >= 2


def test_insert_property_data_inserts_data_correctly(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_data inserts data rows into t_data table."""
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
        insert_property_data(db_with_topology, params)

        # Verify data was inserted
        data_count = db_with_topology.query("SELECT COUNT(*) FROM t_data")
        assert data_count[0][0] > 0


def test_insert_property_data_builds_data_id_map(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_data builds correct data_id_map."""
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

        # Verify data_id_map structure
        assert isinstance(data_id_map, dict)
        for key, value in data_id_map.items():
            assert len(key) == 3  # (membership_id, property_id, value)
            assert len(value) == 2  # (data_id, obj_name)


def test_insert_property_data_handles_null_values(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_data handles None/NULL values correctly."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "Max Capacity": None}]

    params, _ = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        data_id_map = insert_property_data(db_with_topology, params)

        # Should handle NULL values without error
        assert isinstance(data_id_map, dict)


def test_insert_property_data_empty_params_returns_empty_map(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_data returns empty map when params is empty."""
    from plexosdb.utils import insert_property_data

    with db_with_topology._db.transaction():
        data_id_map = insert_property_data(db_with_topology, [])

        assert data_id_map == {}


def test_insert_scenario_tags_early_return_when_scenario_none(db_with_topology: PlexosDB) -> None:
    """Test that insert_scenario_tags early returns when scenario is None."""
    from plexosdb.utils import insert_scenario_tags

    # Should not raise error and should return early when scenario is None
    # Using type: ignore because we're testing the None case
    insert_scenario_tags(db_with_topology, None, [], chunksize=1000)  # type: ignore[arg-type]


def test_insert_scenario_tags_creates_new_scenario(db_with_topology: PlexosDB) -> None:
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
        insert_scenario_tags(db_with_topology, "NewScenario", params, chunksize=1000)

        # Verify scenario was created
        scenario_exists = db_with_topology.check_scenario_exists("NewScenario")
        assert scenario_exists is True


def test_insert_scenario_tags_uses_existing_scenario(db_with_topology: PlexosDB) -> None:
    """Test that insert_scenario_tags uses existing scenario instead of creating new one."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_data, insert_scenario_tags, prepare_properties_params

    # Pre-create scenario
    scenario_id_before = db_with_topology.add_scenario("ExistingScenario")

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
        insert_scenario_tags(db_with_topology, "ExistingScenario", params, chunksize=1000)

        # Verify scenario still exists and wasn't duplicated
        scenario_id_after = db_with_topology.get_scenario_id("ExistingScenario")
        assert scenario_id_after == scenario_id_before


def test_insert_scenario_tags_batching_single_batch(db_with_topology: PlexosDB) -> None:
    """Test insert_scenario_tags with params less than chunksize."""
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
        # Large chunksize - should process all in one batch
        insert_scenario_tags(db_with_topology, "BatchTest", params, chunksize=1000)

        # Verify tags were inserted
        tag_count = db_with_topology.query("SELECT COUNT(*) FROM t_tag")
        assert tag_count[0][0] > 0


def test_insert_scenario_tags_batching_multiple_batches(db_with_topology: PlexosDB) -> None:
    """Test insert_scenario_tags with params split into multiple batches."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_data, insert_scenario_tags, prepare_properties_params

    for i in range(3):
        db_with_topology.add_object(ClassEnum.Generator, f"gen-{i:02d}")

    records = [
        {"name": f"gen-{i:02d}", "Max Capacity": 100.0 + i * 10.0, "Heat Rate": 10.0 + i} for i in range(3)
    ]

    params, _ = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        insert_property_data(db_with_topology, params)
        # Small chunksize to force multiple batches
        insert_scenario_tags(db_with_topology, "BatchTest", params, chunksize=2)

        # Verify tags were inserted
        tag_count = db_with_topology.query("SELECT COUNT(*) FROM t_tag")
        assert tag_count[0][0] > 0


def test_insert_scenario_tags_empty_params(db_with_topology: PlexosDB) -> None:
    """Test insert_scenario_tags with empty params list."""
    from plexosdb.utils import insert_scenario_tags

    with db_with_topology._db.transaction():
        # Should not raise error with empty params
        insert_scenario_tags(db_with_topology, "EmptyTest", [], chunksize=1000)

        # Verify scenario was still created
        scenario_exists = db_with_topology.check_scenario_exists("EmptyTest")
        assert scenario_exists is True


def test_add_texts_for_properties_skips_records_without_field(db_with_topology: PlexosDB) -> None:
    """Test that add_texts_for_properties skips records without specified field."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import add_texts_for_properties, insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [
        {"name": "gen-01", "Max Capacity": 100.0},
        # No datafile_text field in second record
    ]

    params, _ = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        data_id_map = insert_property_data(db_with_topology, params)

        # Call with field that doesn't exist in all records
        add_texts_for_properties(
            db_with_topology, params, data_id_map, records, "datafile_text", ClassEnum.DataFile
        )

        # Should not raise error


def test_add_texts_for_properties_handles_data_id_none(db_with_topology: PlexosDB) -> None:
    """Test that add_texts_for_properties handles missing data_id in map."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import add_texts_for_properties, insert_property_data, prepare_properties_params

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
        insert_property_data(db_with_topology, params)

        # Create empty map - simulating missing data_ids
        empty_data_id_map: dict[tuple[int, int, int], tuple[int, str]] = {}

        # Should not raise error when data_id is missing
        add_texts_for_properties(
            db_with_topology, params, empty_data_id_map, records, "datafile_text", ClassEnum.DataFile
        )


def test_add_texts_for_properties_multiple_texts(db_with_topology: PlexosDB) -> None:
    """Test that add_texts_for_properties handles multiple text records."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import add_texts_for_properties, insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")

    records = [
        {"name": "gen-01", "Max Capacity": 100.0, "datafile_text": "/path/file1.csv"},
        {"name": "gen-02", "Max Capacity": 150.0, "datafile_text": "/path/file2.csv"},
    ]

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

        text_count = db_with_topology.query("SELECT COUNT(*) FROM t_text")
        assert text_count[0][0] >= 2


def test_add_texts_for_properties_empty_params(db_with_topology: PlexosDB) -> None:
    """Test add_texts_for_properties with empty inputs."""
    from plexosdb import ClassEnum
    from plexosdb.utils import add_texts_for_properties

    add_texts_for_properties(db_with_topology, [], {}, [], "datafile_text", ClassEnum.DataFile)


def test_prepare_properties_params_raises_on_no_memberships(db_with_topology: PlexosDB) -> None:
    """Test prepare_properties_params raises NotFoundError when no memberships exist."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.exceptions import NotFoundError
    from plexosdb.utils import prepare_properties_params

    # Try to prepare params for object that doesn't exist
    records = [{"name": "NonExistentObject", "property": 100}]

    with pytest.raises(NotFoundError, match="Object = NonExistentObject not found"):
        prepare_properties_params(
            db_with_topology,
            records,
            ClassEnum.Generator,
            CollectionEnum.Generators,
            ClassEnum.System,
        )
