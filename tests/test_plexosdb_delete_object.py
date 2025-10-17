import pytest

from plexosdb.db import PlexosDB
from plexosdb.enums import ClassEnum
from plexosdb.exceptions import NameError, NoPropertiesError, NotFoundError


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

    # Check there is no object_id
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


def test_delete_property_basic(db_instance_with_schema: PlexosDB):
    """Test basic property deletion functionality."""
    db = db_instance_with_schema
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    property_name = "Max Capacity"
    property_value = 100.0

    # Setup: Add object and property
    db.add_object(object_class, object_name)
    db.add_property(object_class, object_name, property_name, property_value)

    # Verify property exists
    properties = db.get_object_properties(object_class, object_name)
    assert len(properties) == 1
    assert properties[0]["property"] == property_name
    assert properties[0]["value"] == property_value

    # Delete the property
    db.delete_property(object_class, object_name, property_name=property_name)

    # Verify property is deleted
    with pytest.raises(NoPropertiesError):
        db.get_object_properties(object_class, object_name)


def test_delete_property_with_scenario(db_instance_with_schema: PlexosDB):
    """Test deleting a property associated with a specific scenario."""
    db = db_instance_with_schema
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    property_name = "Max Capacity"
    scenario_name = "High Demand"

    # Setup: Add object and properties with and without scenario
    db.add_object(object_class, object_name)
    db.add_property(object_class, object_name, property_name, 100.0)  # No scenario
    db.add_property(object_class, object_name, property_name, 200.0, scenario=scenario_name)  # With scenario

    # Verify both properties exist
    properties = db.get_object_properties(object_class, object_name)
    assert len(properties) == 2

    # Delete only the property with the scenario
    db.delete_property(object_class, object_name, property_name=property_name, scenario=scenario_name)

    # Verify only the property without scenario remains
    properties = db.get_object_properties(object_class, object_name)
    assert len(properties) == 1
    assert properties[0]["value"] == 100.0
    assert properties[0]["scenario_name"] is None


def test_delete_property_nonexistent_object(db_instance_with_schema: PlexosDB):
    """Test that deleting a property from a nonexistent object raises NotFoundError."""
    db = db_instance_with_schema

    with pytest.raises(NotFoundError, match="Object = `NonexistentGen` does not exist"):
        db.delete_property(ClassEnum.Generator, "NonexistentGen", property_name="Max Capacity")


def test_delete_property_invalid_property_name(db_instance_with_schema: PlexosDB):
    """Test that deleting an invalid property name raises NameError."""
    db = db_instance_with_schema
    object_name = "TestGen"
    object_class = ClassEnum.Generator

    # Setup: Add object
    db.add_object(object_class, object_name)

    with pytest.raises(NameError, match="Property 'Invalid Property' does not exist for collection"):
        db.delete_property(object_class, object_name, property_name="Invalid Property")


def test_delete_property_nonexistent_property(db_instance_with_schema: PlexosDB):
    """Test that deleting a property that doesn't exist for the object raises NotFoundError."""
    db = db_instance_with_schema
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    property_name = "Max Capacity"

    # Setup: Add object but no properties
    db.add_object(object_class, object_name)

    with pytest.raises(
        NotFoundError, match=f"Property '{property_name}' not found for object '{object_name}'"
    ):
        db.delete_property(object_class, object_name, property_name=property_name)


def test_delete_property_nonexistent_scenario(db_instance_with_schema: PlexosDB):
    """Test that deleting a property with nonexistent scenario raises NotFoundError."""
    db = db_instance_with_schema
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    property_name = "Max Capacity"

    # Setup: Add object and property
    db.add_object(object_class, object_name)
    db.add_property(object_class, object_name, property_name, 100.0)

    with pytest.raises(NotFoundError, match="Scenario 'Nonexistent Scenario' does not exist"):
        db.delete_property(
            object_class, object_name, property_name=property_name, scenario="Nonexistent Scenario"
        )


def test_delete_property_scenario_not_associated(db_instance_with_schema: PlexosDB):
    """Test that deleting a property with a scenario that's not associated raises NotFoundError."""
    db = db_instance_with_schema
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    property_name = "Max Capacity"
    scenario_name = "High Demand"
    other_scenario = "Low Demand"

    # Setup: Add object, property, and scenarios
    db.add_object(object_class, object_name)
    db.add_property(object_class, object_name, property_name, 100.0, scenario=scenario_name)
    db.add_scenario(other_scenario)  # Add scenario but don't associate with property

    with pytest.raises(
        NotFoundError, match=f"Property '{property_name}' with scenario '{other_scenario}' not found"
    ):
        db.delete_property(object_class, object_name, property_name=property_name, scenario=other_scenario)


def test_delete_property_with_related_data(db_instance_with_schema: PlexosDB):
    """Test that deleting a property also removes related data (text, tags, dates) due to cascade."""
    db = db_instance_with_schema
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    property_name = "Max Capacity"
    scenario_name = "High Demand"

    # Setup: Add object and property with text and scenario
    db.add_object(object_class, object_name)
    text_data = {ClassEnum.Generator: "Some text description"}
    db.add_property(object_class, object_name, property_name, 100.0, scenario=scenario_name, text=text_data)

    # Verify property exists with related data
    properties = db.get_object_properties(object_class, object_name)
    assert len(properties) == 1
    assert properties[0]["scenario_name"] == scenario_name
    assert len(properties[0]["text"]) > 0

    # Delete the property
    db.delete_property(object_class, object_name, property_name=property_name)

    # Verify property and related data are deleted
    with pytest.raises(NoPropertiesError):
        db.get_object_properties(object_class, object_name)

    # The scenario object should still exist (it's not deleted when property is deleted)
    assert db.check_scenario_exists(scenario_name)
