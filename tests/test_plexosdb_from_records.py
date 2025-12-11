from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from plexosdb import PlexosDB


def test_bulk_insert_properties_from_records(db_base: PlexosDB):
    from plexosdb import ClassEnum, CollectionEnum

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Generator, "Generator1")
    db.add_object(ClassEnum.Generator, "Generator2")
    db.add_object(ClassEnum.Generator, "Generator3")

    records = [
        {
            "name": "Generator1",
            "properties": {
                "Max Capacity": {"value": 100.0},
                "Min Stable Level": {"value": 20.0},
                "Heat Rate": {"value": 10.5},
            },
        },
        {
            "name": "Generator2",
            "properties": {
                "Max Capacity": {"value": 150.0},
                "Min Stable Level": {"value": 30.0},
                "Heat Rate": {"value": 9.8},
            },
        },
        {
            "name": "Generator3",
            "properties": {
                "Max Capacity": {"value": 200.0},
                "Min Stable Level": {"value": 40.0},
                "Heat Rate": {"value": 8.7},
            },
        },
    ]

    db.add_properties_from_records(
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        scenario="Base Case",
    )

    properties = db.get_object_properties(ClassEnum.Generator, name="Generator1")
    assert properties
    assert properties[0]["property"] == "Max Capacity"
    assert properties[0]["scenario_name"] == "Base Case"


def test_add_properties_supports_flat_records_with_metadata(db_instance_with_schema: PlexosDB):
    from datetime import datetime
    from plexosdb import ClassEnum, CollectionEnum

    db = db_instance_with_schema
    db.add_object(ClassEnum.Generator, "FlatGen")

    records = [
        {
            "name": "FlatGen",
            "property": "Max Capacity",
            "value": 120.5,
            "band": 1,
            "date_from": datetime(2025, 1, 1),
            "date_to": datetime(2025, 2, 1),
        },
        {
            "name": "FlatGen",
            "property": "Max Energy",
            "value": 350.0,
            "datafile_text": "profile.csv",
        },
    ]

    db.add_properties_from_records(
        records,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
        scenario="Planning",
    )

    data_rows = db._db.fetchall("SELECT membership_id, property_id, value FROM t_data")
    assert len(data_rows) == 2

    band_rows = db._db.fetchall("SELECT data_id, band_id FROM t_band")
    assert len(band_rows) == 1
    assert band_rows[0][1] == 1

    date_from_rows = db._db.fetchall("SELECT date FROM t_date_from")
    date_to_rows = db._db.fetchall("SELECT date FROM t_date_to")
    assert date_from_rows[0][0].startswith("2025-01-01")
    assert date_to_rows[0][0].startswith("2025-02-01")

    text_rows = db._db.fetchall("SELECT class_id, value FROM t_text")
    assert len(text_rows) == 1
    assert text_rows[0][1] == "profile.csv"


def test_add_properties_nested_records_emit_deprecation(db_instance_with_schema: PlexosDB):
    from plexosdb import ClassEnum, CollectionEnum

    db = db_instance_with_schema
    db.add_object(ClassEnum.Generator, "LegacyGen")

    records = [
        {
            "name": "LegacyGen",
            "properties": {
                "Max Capacity": {"value": 75.0},
            },
        }
    ]

    with pytest.warns(DeprecationWarning):
        db.add_properties_from_records(
            records,
            object_class=ClassEnum.Generator,
            collection=CollectionEnum.Generators,
            parent_class=ClassEnum.System,
            scenario="Legacy",
        )

    values = db._db.fetchall("SELECT value FROM t_data")
    assert values == [(75.0,)]


def test_bulk_insert_memberships_from_records(db_base: PlexosDB):
    from plexosdb import ClassEnum, CollectionEnum

    db = db_base

    objects = list(f"Generator_{i}" for i in range(5))
    db.add_objects(ClassEnum.Generator, objects)
    parent_object_ids = db.get_objects_id(objects, class_enum=ClassEnum.Generator)
    assert parent_object_ids
    assert db.get_memberships_system(objects, object_class=ClassEnum.Generator)

    objects = list(f"Nodes_{i}" for i in range(5))
    db.add_objects(ClassEnum.Node, objects)
    child_object_ids = db.get_objects_id(objects, class_enum=ClassEnum.Node)
    assert child_object_ids
    assert len(child_object_ids) == 5
    assert db.get_memberships_system(objects, object_class=ClassEnum.Node)

    collection_id = db.get_collection_id(
        CollectionEnum.Nodes, parent_class_enum=ClassEnum.Generator, child_class_enum=ClassEnum.Node
    )
    parent_class_id = db.get_class_id(ClassEnum.Generator)
    child_class_id = db.get_class_id(ClassEnum.Node)
    memberships = [
        {
            "collection_id": collection_id,
            "parent_class_id": parent_class_id,
            "child_class_id": child_class_id,
            "child_object_id": child_id,
            "parent_object_id": parent_id,
        }
        for parent_id, child_id in zip(parent_object_ids, child_object_ids)
    ]
    db.add_memberships_from_records(memberships)

    db_memberships = db.list_object_memberships(
        ClassEnum.Node,
        objects[0],
        collection=CollectionEnum.Nodes,
    )
    assert len(db_memberships) == 2  # 1 + system membership

    memberships = [
        {
            "parent_class_id": parent_class_id,
            "child_class_id": child_class_id,
            "child_object_id": child_id,
            "parent_object_id": parent_id,
        }
        for parent_id, child_id in zip(parent_object_ids, child_object_ids)
    ]
    with pytest.raises(KeyError):
        _ = db.add_memberships_from_records(memberships)


def test_add_properties_from_records_no_records(db_instance_with_schema: PlexosDB, caplog):
    """Gracefully handle empty payload."""
    from plexosdb import ClassEnum, CollectionEnum

    db = db_instance_with_schema
    db.add_object(ClassEnum.Generator, "EmptyGen")

    db.add_properties_from_records(
        [],
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
        scenario="None",
    )

    assert "No records provided" in caplog.text
    assert db._db.fetchone("SELECT COUNT(*) FROM t_data")[0] == 0


def test_add_properties_from_records_unknown_property(db_instance_with_schema: PlexosDB):
    """Return early when properties are not recognized for the collection."""
    from plexosdb import ClassEnum, CollectionEnum

    db = db_instance_with_schema
    db.add_object(ClassEnum.Generator, "BadPropGen")

    db.add_properties_from_records(
        [{"name": "BadPropGen", "property": "Unknown", "value": 1}],
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
        scenario="None",
    )

    assert db._db.fetchone("SELECT COUNT(*) FROM t_data")[0] == 0
