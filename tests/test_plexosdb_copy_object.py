import pytest

from plexosdb import ClassEnum
from plexosdb.db import PlexosDB
from plexosdb.enums import CollectionEnum


def test_copy_object(db_instance_with_schema, caplog):
    db = db_instance_with_schema
    original_object_name = "TestGen"
    object_class = ClassEnum.Generator
    original_object_id = db.add_object(object_class, original_object_name)
    test_property_name = "Max Capacity"
    test_property_value = 100.0
    _ = db.add_property(
        ClassEnum.Generator,
        original_object_name,
        test_property_name,
        test_property_value,
    )

    # Test default behaviour of copying properties
    new_object_name = "TestGenCopy"
    new_object_id = db.copy_object(object_class, original_object_name, new_object_name)
    assert new_object_id
    assert new_object_id != original_object_id
    assert "do not have any memberships" in caplog.text

    new_properties = db.get_object_properties(object_class, new_object_name)[0]
    old_properties = db.get_object_properties(object_class, original_object_name)[0]
    assert all(property_name in new_properties for property_name in old_properties.keys())
    for old_property in old_properties:
        if old_property == "name":
            assert new_properties[old_property] == new_object_name
        elif old_property == "object_id":
            assert new_properties[old_property] != original_object_id
        else:
            assert old_properties[old_property] == new_properties[old_property]


def test_copy_object_with_memberships(db_instance_with_schema):
    db: PlexosDB = db_instance_with_schema
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    _ = db.add_object(object_class, object_name)
    child_object_name = "TestNode"
    child_class = ClassEnum.Node
    _ = db.add_object(child_class, child_object_name)
    collection = CollectionEnum.Nodes

    membership_id_child = db.add_membership(
        object_class, child_class, object_name, child_object_name, collection
    )
    assert membership_id_child == db.get_membership_id(object_name, child_object_name, collection)

    new_object_name = "TestGen2"
    object_id = db.get_object_id(object_class, name=object_name)
    category_id = db.query("SELECT category_id from t_object WHERE object_id = ?", (object_id,))
    category = db.query("SELECT name from t_category WHERE category_id = ?", (category_id[0][0],))
    _ = db.add_object(object_class, new_object_name, category=category[0][0])

    membership_mapping = db.copy_object_memberships(object_class, object_name, new_object_name)
    new_child_membership = db.get_membership_id(new_object_name, child_object_name, collection)
    assert membership_id_child in membership_mapping
    assert membership_mapping[membership_id_child] == new_child_membership


@pytest.mark.xfail
def test_copy_object_with_text_data(db_instance_with_schema):
    pass
