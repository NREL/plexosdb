"""Tests for property insertion utility functions."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from plexosdb import PlexosDB


def test_plan_property_inserts_succeeds(db_with_topology: PlexosDB) -> None:
    """Test that plan_property_inserts correctly prepares SQL parameters."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")

    records = [
        {
            "name": "gen-01",
            "properties": {"Max Capacity": {"value": 100.0}, "Heat Rate": {"value": 10.5}},
        },
        {
            "name": "gen-02",
            "properties": {"Max Capacity": {"value": 150.0}, "Heat Rate": {"value": 9.8}},
        },
    ]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )

    assert prepared.params is not None
    assert len(prepared.params) == 4  # 2 records with 2 properties
    assert prepared.collection_properties is not None
    assert isinstance(prepared.metadata_map, dict)


def test_insert_property_values_marks_dynamic_and_enabled(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_values marks properties as dynamic and enabled."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}}]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        data_id_map = insert_property_values(db_with_topology, params, metadata_map=metadata_map)

        assert data_id_map is not None
        assert len(data_id_map) == 1


def test_insert_property_values_raises_for_invalid_date_type(db_instance_with_schema: PlexosDB) -> None:
    """Invalid date types should trigger TypeError."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, plan_property_inserts

    db = db_instance_with_schema
    db.add_object(ClassEnum.Generator, "DateGen")

    records = [
        {
            "name": "DateGen",
            "properties": {"Max Capacity": {"value": 10.0, "date_from": "2025-01-01"}},
        }
    ]

    prepared = plan_property_inserts(
        db,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db._db.transaction(), pytest.raises(TypeError):
        insert_property_values(db, params, metadata_map=metadata_map)


def test_apply_scenario_tags_creates_scenario(db_with_topology: PlexosDB) -> None:
    """Test that apply_scenario_tags creates scenario if it doesn't exist."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, apply_scenario_tags, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}}]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        insert_property_values(db_with_topology, params, metadata_map=metadata_map)
        apply_scenario_tags(db_with_topology, params, scenario="Test Scenario", chunksize=1000)

        # Verify scenario was created
        scenario_exists = db_with_topology.check_scenario_exists("Test Scenario")
        assert scenario_exists is True


def test_insert_property_texts_with_datafile_text(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_texts correctly adds datafile_text field."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import (
        insert_property_texts,
        insert_property_values,
        plan_property_inserts,
    )

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [
        {
            "name": "gen-01",
            "properties": {"Max Capacity": {"value": 100.0}},
            "datafile_text": "/path/to/file.csv",
        }
    ]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        data_id_map = insert_property_values(db_with_topology, params, metadata_map=metadata_map)
        insert_property_texts(
            db_with_topology,
            params,
            data_id_map=data_id_map,
            records=records,
            field_name="datafile_text",
            text_class=ClassEnum.DataFile,
        )

        # Verify text was added by checking database
        text_records = db_with_topology.query("SELECT COUNT(*) as count FROM t_text")
        assert text_records[0][0] > 0


def test_insert_property_texts_handles_missing_names_and_prop_level(
    db_instance_with_schema: PlexosDB,
) -> None:
    """Text mapping should ignore nameless records and accept property-level text."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import (
        insert_property_texts,
        insert_property_values,
        plan_property_inserts,
    )

    db = db_instance_with_schema
    db.add_object(ClassEnum.Generator, "TextGen")

    records = [
        {
            "name": "TextGen",
            "properties": {"Max Capacity": {"value": 5.0, "datafile_text": "prop-level.txt"}},
        }
    ]

    prepared = plan_property_inserts(
        db,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map
    records_with_extra = [*records, {"name": None, "datafile_text": "skip-me"}]

    with db._db.transaction():
        data_id_map = insert_property_values(db, params, metadata_map=metadata_map)
        insert_property_texts(
            db,
            params,
            data_id_map=data_id_map,
            records=records_with_extra,
            field_name="datafile_text",
            text_class=ClassEnum.DataFile,
            metadata_map=metadata_map,
        )

    text_rows = db._db.fetchall("SELECT value FROM t_text")
    assert text_rows == [("prop-level.txt",)]


def test_plan_property_inserts_raises_error_when_no_memberships(db_with_topology: PlexosDB) -> None:
    """Test that plan_property_inserts raises error when objects don't exist."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.exceptions import NotFoundError
    from plexosdb.utils import plan_property_inserts

    # Don't add objects to database - they should not exist
    records = [
        {"name": "nonexistent-gen-01", "properties": {"Max Capacity": {"value": 100.0}}},
        {"name": "nonexistent-gen-02", "properties": {"Max Capacity": {"value": 150.0}}},
    ]

    # Raises NotFoundError when objects don't exist in database
    with pytest.raises(NotFoundError):
        plan_property_inserts(
            db_with_topology,
            records,
            object_class=ClassEnum.Generator,
            collection=CollectionEnum.Generators,
            parent_class=ClassEnum.System,
        )


def test_plan_property_inserts_empty_collection_properties(db_with_topology: PlexosDB) -> None:
    """Test plan_property_inserts with valid objects but no properties in collection."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    # Records with properties that don't exist in the collection
    records = [
        {"name": "gen-01", "properties": {"NonexistentProperty": {"value": 100.0}}},
    ]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    collection_properties = prepared.collection_properties
    metadata_map = prepared.metadata_map

    # Should return empty params since property doesn't exist in collection
    assert params == []
    assert collection_properties is not None
    assert isinstance(metadata_map, dict)


def test_plan_property_inserts_multiple_records_single_valid(db_with_topology: PlexosDB) -> None:
    """Test plan_property_inserts with multiple records but only some have valid properties."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")

    records = [
        {"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}},
        {"name": "gen-02", "properties": {"NonexistentProperty": {"value": 150.0}}},
    ]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    # Should only have params for gen-01 with Max Capacity
    assert len(params) >= 1
    assert all(param[2] is not None for param in params)
    assert isinstance(metadata_map, dict)


def test_plan_property_inserts_return_structure(db_with_topology: PlexosDB) -> None:
    """Test that plan_property_inserts returns structured result."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}}]

    result = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )

    assert hasattr(result, "params")
    assert hasattr(result, "collection_properties")
    assert hasattr(result, "metadata_map")
    assert hasattr(result, "normalized_records")

    # Check params structure
    assert isinstance(result.params, list)
    if len(result.params) > 0:
        assert isinstance(result.params[0], tuple)
        assert len(result.params[0]) == 3  # (membership_id, property_id, value)

    # Check collection_properties structure
    assert isinstance(result.collection_properties, list)

    # Check metadata_map structure
    assert isinstance(result.metadata_map, dict)


def test_plan_property_inserts_handles_simple_values(db_instance_with_schema: PlexosDB) -> None:
    """Ensure simple (non-dict) property values are normalized."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import plan_property_inserts

    db = db_instance_with_schema
    db.add_object(ClassEnum.Generator, "SimpleGen")

    records = [{"name": "SimpleGen", "properties": {"Max Capacity": 99.0}}]

    prepared = plan_property_inserts(
        db,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )

    assert len(prepared.params) == 1
    meta = next(iter(prepared.metadata_map.values()))
    assert meta["band"] is None and meta["date_from"] is None and meta["date_to"] is None


def test_plan_property_inserts_invalid_record_raises(db_instance_with_schema: PlexosDB) -> None:
    """Invalid payload lacking property info should raise ValueError."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import plan_property_inserts

    db = db_instance_with_schema
    with pytest.raises(ValueError):
        plan_property_inserts(
            db,
            [{"name": "NoProperty"}],
            object_class=ClassEnum.Generator,
            collection=CollectionEnum.Generators,
            parent_class=ClassEnum.System,
        )


def test_plan_property_inserts_empty_records(db_instance_with_schema: PlexosDB) -> None:
    """Empty input returns empty parameter collections."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import plan_property_inserts

    db = db_instance_with_schema
    prepared = plan_property_inserts(
        db,
        [],
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    assert prepared.params == [] and prepared.collection_properties == [] and prepared.metadata_map == {}


def test_plan_property_inserts_membership_missing(monkeypatch, db_instance_with_schema: PlexosDB) -> None:
    """Raises NotFoundError when memberships lookup returns empty."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.exceptions import NotFoundError
    from plexosdb.utils import plan_property_inserts

    db = db_instance_with_schema
    db.add_object(ClassEnum.Generator, "GhostGen")
    monkeypatch.setattr(db, "get_memberships_system", lambda *args, **kwargs: [])

    with pytest.raises(NotFoundError, match="Objects not found: GhostGen"):
        plan_property_inserts(
            db,
            [{"name": "GhostGen", "properties": {"Max Capacity": {"value": 10}}}],
            object_class=ClassEnum.Generator,
            collection=CollectionEnum.Generators,
            parent_class=ClassEnum.System,
        )


def test_plan_property_inserts_skips_missing_membership(
    monkeypatch, db_instance_with_schema: PlexosDB
) -> None:
    """Records without matching membership are skipped gracefully."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import plan_property_inserts

    db = db_instance_with_schema
    db.add_object(ClassEnum.Generator, "HasMembership")

    fake_memberships = [{"name": "OtherObject", "membership_id": 999}]
    monkeypatch.setattr(db, "get_memberships_system", lambda *args, **kwargs: fake_memberships)

    prepared = plan_property_inserts(
        db,
        [{"name": "HasMembership", "properties": {"Max Capacity": {"value": 42}}}],
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )

    assert prepared.params == []
    assert prepared.metadata_map == {}


def test_insert_property_values_updates_multiple_properties(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_values marks multiple properties as dynamic and enabled."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")

    records = [
        {
            "name": "gen-01",
            "properties": {"Max Capacity": {"value": 100.0}, "Heat Rate": {"value": 10.5}},
        },
        {
            "name": "gen-02",
            "properties": {"Max Capacity": {"value": 150.0}, "Heat Rate": {"value": 9.8}},
        },
    ]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        insert_property_values(db_with_topology, params, metadata_map=metadata_map)

        # Verify properties are marked as dynamic and enabled
        properties = db_with_topology.query(
            "SELECT property_id, is_dynamic, is_enabled FROM t_property WHERE is_dynamic=1 AND is_enabled=1"
        )
        assert len(properties) >= 2


def test_insert_property_values_inserts_data_correctly(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_values inserts data rows into t_data table."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}}]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        insert_property_values(db_with_topology, params, metadata_map=metadata_map)

        # Verify data was inserted
        data_count = db_with_topology.query("SELECT COUNT(*) FROM t_data")
        assert data_count[0][0] > 0


def test_insert_property_values_builds_data_id_map(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_values builds correct data_id_map."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}}]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        data_id_map = insert_property_values(db_with_topology, params, metadata_map=metadata_map)

        # Verify data_id_map structure
        assert isinstance(data_id_map, dict)
        for key, value in data_id_map.items():
            assert len(key) == 3  # (membership_id, property_id, value)
            assert len(value) == 2  # (data_id, obj_name)


def test_insert_property_values_handles_null_values(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_values handles None/NULL values correctly."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "properties": {"Max Capacity": {"value": None}}}]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        data_id_map = insert_property_values(db_with_topology, params, metadata_map=metadata_map)

        # Should handle NULL values without error
        assert isinstance(data_id_map, dict)


def test_insert_property_values_empty_params_returns_empty_map(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_values returns empty map when params is empty."""
    from plexosdb.utils import insert_property_values

    with db_with_topology._db.transaction():
        data_id_map = insert_property_values(db_with_topology, [], metadata_map=None)

        assert data_id_map == {}


def test_apply_scenario_tags_early_return_when_scenario_none(db_with_topology: PlexosDB) -> None:
    """Test that apply_scenario_tags early returns when scenario is None."""
    from plexosdb.utils import apply_scenario_tags

    # Should not raise error and should return early when scenario is None
    apply_scenario_tags(db_with_topology, [], scenario=None, chunksize=1000)  # type: ignore[arg-type]


def test_apply_scenario_tags_creates_new_scenario(db_with_topology: PlexosDB) -> None:
    """Test that apply_scenario_tags creates scenario if it doesn't exist."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, apply_scenario_tags, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}}]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        insert_property_values(db_with_topology, params, metadata_map=metadata_map)
        apply_scenario_tags(db_with_topology, params, scenario="NewScenario", chunksize=1000)

        # Verify scenario was created
        scenario_exists = db_with_topology.check_scenario_exists("NewScenario")
        assert scenario_exists is True


def test_apply_scenario_tags_uses_existing_scenario(db_with_topology: PlexosDB) -> None:
    """Test that apply_scenario_tags uses existing scenario instead of creating new one."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, apply_scenario_tags, plan_property_inserts

    # Pre-create scenario
    scenario_id_before = db_with_topology.add_scenario("ExistingScenario")

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}}]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        insert_property_values(db_with_topology, params, metadata_map=metadata_map)
        apply_scenario_tags(db_with_topology, params, scenario="ExistingScenario", chunksize=1000)

        # Verify scenario still exists and wasn't duplicated
        scenario_id_after = db_with_topology.get_scenario_id("ExistingScenario")
        assert scenario_id_after == scenario_id_before


def test_apply_scenario_tags_batching_single_batch(db_with_topology: PlexosDB) -> None:
    """Test apply_scenario_tags with params less than chunksize."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, apply_scenario_tags, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [
        {
            "name": "gen-01",
            "properties": {
                "Max Capacity": {
                    "value": 100.0,
                    "band": 1,
                    "date_from": datetime(2025, 1, 1),
                    "date_to": datetime(2025, 12, 31),
                }
            },
        }
    ]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        insert_property_values(db_with_topology, params, metadata_map=metadata_map)
        # Large chunksize - should process all in one batch
        apply_scenario_tags(db_with_topology, params, scenario="BatchTest", chunksize=1000)

        # Verify tags were inserted
        tag_count = db_with_topology.query("SELECT COUNT(*) FROM t_tag")
        assert tag_count[0][0] > 0


def test_apply_scenario_tags_batching_multiple_batches(db_with_topology: PlexosDB) -> None:
    """Test apply_scenario_tags with params split into multiple batches."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_values, apply_scenario_tags, plan_property_inserts

    for i in range(3):
        db_with_topology.add_object(ClassEnum.Generator, f"gen-{i:02d}")

    records = [
        {
            "name": f"gen-{i:02d}",
            "properties": {
                "Max Capacity": {"value": 100.0 + i * 10.0},
                "Heat Rate": {"value": 10.0 + i},
            },
        }
        for i in range(3)
    ]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        insert_property_values(db_with_topology, params, metadata_map=metadata_map)
        # Small chunksize to force multiple batches
        apply_scenario_tags(db_with_topology, params, scenario="BatchTest", chunksize=2)

        # Verify tags were inserted
        tag_count = db_with_topology.query("SELECT COUNT(*) FROM t_tag")
        assert tag_count[0][0] > 0


def test_apply_scenario_tags_empty_params(db_with_topology: PlexosDB) -> None:
    """Test apply_scenario_tags with empty params list."""
    from plexosdb.utils import apply_scenario_tags

    with db_with_topology._db.transaction():
        # Should not raise error with empty params
        apply_scenario_tags(db_with_topology, [], scenario="EmptyTest", chunksize=1000)

        # Verify scenario was still created
        scenario_exists = db_with_topology.check_scenario_exists("EmptyTest")
        assert scenario_exists is True


def test_insert_property_texts_skips_records_without_field(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_texts skips records without specified field."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_texts, insert_property_values, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [
        {"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}},
        # No datafile_text field in record
    ]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        data_id_map = insert_property_values(db_with_topology, params, metadata_map=metadata_map)

        # Call with field that doesn't exist in all records
        insert_property_texts(
            db_with_topology,
            params,
            data_id_map=data_id_map,
            records=records,
            field_name="datafile_text",
            text_class=ClassEnum.DataFile,
        )

        # Should not raise error


def test_insert_property_texts_handles_data_id_none(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_texts handles missing data_id in map."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_texts, insert_property_values, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [
        {
            "name": "gen-01",
            "properties": {"Max Capacity": {"value": 100.0}},
            "datafile_text": "/path/to/file.csv",
        }
    ]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        insert_property_values(db_with_topology, params, metadata_map=metadata_map)

        # Create empty map - simulating missing data_ids
        empty_data_id_map: dict[tuple[int, int, int], tuple[int, str]] = {}

        # Should not raise error when data_id is missing
        insert_property_texts(
            db_with_topology,
            params,
            data_id_map=empty_data_id_map,
            records=records,
            field_name="datafile_text",
            text_class=ClassEnum.DataFile,
        )


def test_insert_property_texts_multiple_texts(db_with_topology: PlexosDB) -> None:
    """Test that insert_property_texts handles multiple text records."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import insert_property_texts, insert_property_values, plan_property_inserts

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")

    records = [
        {
            "name": "gen-01",
            "properties": {"Max Capacity": {"value": 100.0}},
            "datafile_text": "/path/file1.csv",
        },
        {
            "name": "gen-02",
            "properties": {"Max Capacity": {"value": 150.0}},
            "datafile_text": "/path/file2.csv",
        },
    ]

    prepared = plan_property_inserts(
        db_with_topology,
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
    )
    params = prepared.params
    metadata_map = prepared.metadata_map

    with db_with_topology._db.transaction():
        data_id_map = insert_property_values(db_with_topology, params, metadata_map=metadata_map)

        insert_property_texts(
            db_with_topology,
            params,
            data_id_map=data_id_map,
            records=records,
            field_name="datafile_text",
            text_class=ClassEnum.DataFile,
        )

        text_count = db_with_topology.query("SELECT COUNT(*) FROM t_text")
        assert text_count[0][0] >= 2


def test_insert_property_texts_empty_params(db_with_topology: PlexosDB) -> None:
    """Test insert_property_texts with empty inputs."""
    from plexosdb import ClassEnum
    from plexosdb.utils import insert_property_texts

    insert_property_texts(
        db_with_topology,
        [],
        data_id_map={},
        records=[],
        field_name="datafile_text",
        text_class=ClassEnum.DataFile,
    )


def test_plan_property_inserts_raises_on_no_memberships(db_with_topology: PlexosDB) -> None:
    """Test plan_property_inserts raises NotFoundError when no memberships exist."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.exceptions import NotFoundError
    from plexosdb.utils import plan_property_inserts

    # Try to prepare params for object that doesn't exist
    records = [{"name": "NonExistentObject", "properties": {"property": {"value": 100}}}]

    with pytest.raises(NotFoundError, match="Objects not found: NonExistentObject"):
        plan_property_inserts(
            db_with_topology,
            records,
            object_class=ClassEnum.Generator,
            collection=CollectionEnum.Generators,
            parent_class=ClassEnum.System,
        )
