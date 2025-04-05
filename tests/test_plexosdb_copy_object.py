import pytest

from plexosdb import ClassEnum


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
        else:
            assert old_properties[old_property] == new_properties[old_property]


@pytest.mark.xfail
def test_copy_object_with_text_data(db_instance_with_schema):
    pass
