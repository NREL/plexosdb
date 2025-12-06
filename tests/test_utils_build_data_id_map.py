"""Tests for build_data_id_map utility function."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from plexosdb import PlexosDB


def test_build_data_id_map_single_record(db_with_topology: PlexosDB) -> None:
    """Test build_data_id_map with single record returns correct mapping."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import build_data_id_map, insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}}]

    params, _, metadata_map = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        insert_property_data(db_with_topology, params, metadata_map)
        data_id_map = build_data_id_map(db_with_topology._db, params)

        assert len(data_id_map) == 1
        assert isinstance(data_id_map, dict)


def test_build_data_id_map_returns_correct_structure(db_with_topology: PlexosDB) -> None:
    """Test build_data_id_map returns (data_id, obj_name) tuples with correct key format."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import build_data_id_map, insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [{"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}}]

    params, _, metadata_map = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        insert_property_data(db_with_topology, params, metadata_map)
        data_id_map = build_data_id_map(db_with_topology._db, params)

        for key, value in data_id_map.items():
            assert len(key) == 3
            assert isinstance(key[0], int)  # membership_id
            assert isinstance(key[1], int)  # property_id
            assert isinstance(key[2], int | float)  # value
            assert len(value) == 2
            assert isinstance(value[0], int)  # data_id
            assert isinstance(value[1], str)  # obj_name
            assert value[0] > 0
            assert value[1] == "gen-01"


def test_build_data_id_map_multiple_records(db_with_topology: PlexosDB) -> None:
    """Test build_data_id_map with multiple records."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import build_data_id_map, insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")

    records = [
        {"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}},
        {"name": "gen-02", "properties": {"Max Capacity": {"value": 200.0}}},
    ]

    params, _, metadata_map = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        insert_property_data(db_with_topology, params, metadata_map)
        data_id_map = build_data_id_map(db_with_topology._db, params)

        assert len(data_id_map) == 2


def test_build_data_id_map_multiple_properties(db_with_topology: PlexosDB) -> None:
    """Test build_data_id_map with multiple properties per record."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import build_data_id_map, insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")

    records = [
        {
            "name": "gen-01",
            "properties": {"Max Capacity": {"value": 100.0}, "Fuel Price": {"value": 5.0}},
        }
    ]

    params, _, metadata_map = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        insert_property_data(db_with_topology, params, metadata_map)
        data_id_map = build_data_id_map(db_with_topology._db, params)

        assert len(data_id_map) == 2


def test_build_data_id_map_empty_params(db_with_topology: PlexosDB) -> None:
    """Test build_data_id_map with empty params returns empty dict."""
    from plexosdb.utils import build_data_id_map

    data_id_map = build_data_id_map(db_with_topology._db, [])

    assert data_id_map == {}
    assert isinstance(data_id_map, dict)


def test_build_data_id_map_preserves_mapping_accuracy(db_with_topology: PlexosDB) -> None:
    """Test build_data_id_map maintains accurate mapping between params and results."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import build_data_id_map, insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")

    records = [
        {"name": "gen-01", "properties": {"Max Capacity": {"value": 100.0}}},
        {"name": "gen-02", "properties": {"Max Capacity": {"value": 200.0}}},
    ]

    params, _, metadata_map = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        insert_property_data(db_with_topology, params, metadata_map)
        data_id_map = build_data_id_map(db_with_topology._db, params)

        # Verify all params are in the mapping
        for param in params:
            membership_id, property_id, value = param
            key = (membership_id, property_id, value)
            assert key in data_id_map


def test_build_data_id_map_edge_case_values(db_with_topology: PlexosDB) -> None:
    """Test build_data_id_map handles zero, negative, and large values."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import build_data_id_map, insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "gen-01")
    db_with_topology.add_object(ClassEnum.Generator, "gen-02")
    db_with_topology.add_object(ClassEnum.Generator, "gen-03")

    records = [
        {"name": "gen-01", "properties": {"Max Capacity": {"value": 0.0}}},
        {"name": "gen-02", "properties": {"Max Capacity": {"value": -100.0}}},
        {"name": "gen-03", "properties": {"Max Capacity": {"value": 1e15}}},
    ]

    params, _, metadata_map = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        insert_property_data(db_with_topology, params, metadata_map)
        data_id_map = build_data_id_map(db_with_topology._db, params)

        assert len(data_id_map) == 3
        # Verify edge case values are preserved in keys
        keys = list(data_id_map.keys())
        values_in_keys = [key[2] for key in keys]
        assert 0.0 in values_in_keys
        assert -100.0 in values_in_keys
        assert 1e15 in values_in_keys


def test_build_data_id_map_data_ids_and_names_valid(db_with_topology: PlexosDB) -> None:
    """Test build_data_id_map returns valid data_ids and non-empty object names."""
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.utils import build_data_id_map, insert_property_data, prepare_properties_params

    db_with_topology.add_object(ClassEnum.Generator, "test-generator")

    records = [{"name": "test-generator", "properties": {"Max Capacity": {"value": 100.0}}}]

    params, _, metadata_map = prepare_properties_params(
        db_with_topology,
        records,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        ClassEnum.System,
    )

    with db_with_topology._db.transaction():
        insert_property_data(db_with_topology, params, metadata_map)
        data_id_map = build_data_id_map(db_with_topology._db, params)

        for data_id, obj_name in data_id_map.values():
            assert data_id > 0
            assert len(obj_name) > 0
            assert obj_name == "test-generator"
