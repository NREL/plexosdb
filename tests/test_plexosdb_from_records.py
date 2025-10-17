import pytest

from plexosdb import ClassEnum, CollectionEnum, PlexosDB


def test_bulk_insert_properties_from_records(db_instance_with_schema):
    db: PlexosDB() = db_instance_with_schema

    # Create the objects first
    db.add_object(ClassEnum.Generator, "Generator1")
    db.add_object(ClassEnum.Generator, "Generator2")
    db.add_object(ClassEnum.Generator, "Generator3")

    # Prepare the property records
    records = [
        {"name": "Generator1", "Max Capacity": 100.0, "Min Stable Level": 20.0, "Heat Rate": 10.5},
        {"name": "Generator2", "Max Capacity": 150.0, "Min Stable Level": 30.0, "Heat Rate": 9.8},
        {"name": "Generator3", "Max Capacity": 200.0, "Min Stable Level": 40.0, "Heat Rate": 8.7},
    ]

    # Add all properties in bulk
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


def test_bulk_insert_memberships_from_records(db_instance_with_schema: PlexosDB):
    db = db_instance_with_schema

    # Create the objects first
    objects = list(f"Generator_{i}" for i in range(5))
    db.add_objects(objects, class_enum=ClassEnum.Generator)
    parent_object_ids = db.get_objects_id(objects, class_enum=ClassEnum.Generator)
    assert parent_object_ids
    assert db.get_memberships_system(objects, object_class=ClassEnum.Generator)

    objects = list(f"Nodes_{i}" for i in range(5))
    db.add_objects(objects, class_enum=ClassEnum.Node)
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
