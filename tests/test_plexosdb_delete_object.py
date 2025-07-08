import pytest

from plexosdb.db import PlexosDB
from plexosdb.enums import ClassEnum
from plexosdb.exceptions import NoPropertiesError, NotFoundError


def test_delete_object_with_no_properties(db_instance_with_schema: PlexosDB, caplog):
    db = db_instance_with_schema
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    _ = db.add_object(object_class, object_name)

    another_object = "TestGen2"
    _ = db.add_object(object_class, another_object)

    assert len(db.list_objects_by_class(object_class)) == 2

    assert len(db.list_object_memberships(object_class, object_name)) == 1

    db.delete_object(object_class, name=object_name)

    # Assert object does not appear on the list of names
    assert not db.check_object_exists(object_class, name=object_name)
    assert object_name not in db.list_objects_by_class(object_class)
    assert len(db.list_objects_by_class(object_class)) == 1

    # Check that we removed all the memberships
    with pytest.raises(NotFoundError):
        db.list_object_memberships(object_class, object_name)

    # Checck there is no object_id
    with pytest.raises(NotFoundError):
        _ = db.get_object_id(object_class, object_name)


def test_delete_object_with_properties(db_instance_with_schema, caplog):
    db = db_instance_with_schema
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    test_property_name = "Max Capacity"
    test_property_value = 100.0
    _ = db.add_object(object_class, object_name)
    _ = db.add_property(
        object_class,
        object_name,
        test_property_name,
        test_property_value,
    )

    # Test default behaviour of copying properties
    db.delete_object(object_class, name=object_name)
    with pytest.raises(NoPropertiesError):
        assert not db.get_object_properties(object_class, object_name)
