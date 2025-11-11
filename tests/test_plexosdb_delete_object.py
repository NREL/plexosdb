from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from plexosdb.db import PlexosDB


def test_delete_object_with_no_properties(db_base: PlexosDB, caplog):
    from plexosdb.enums import ClassEnum
    from plexosdb.exceptions import NotFoundError

    db = db_base
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    _ = db.add_object(object_class, object_name)

    another_object = "TestGen2"
    _ = db.add_object(object_class, another_object)

    assert len(db.list_objects_by_class(object_class)) == 2

    assert len(db.list_object_memberships(object_class, object_name)) == 1

    db.delete_object(object_class, name=object_name)

    assert not db.check_object_exists(object_class, name=object_name)
    assert object_name not in db.list_objects_by_class(object_class)
    assert len(db.list_objects_by_class(object_class)) == 1

    with pytest.raises(NotFoundError):
        db.list_object_memberships(object_class, object_name)

    with pytest.raises(NotFoundError):
        _ = db.get_object_id(object_class, object_name)


def test_delete_object_with_properties(db_base: PlexosDB, caplog):
    from plexosdb.enums import ClassEnum
    from plexosdb.exceptions import NotFoundError

    db = db_base
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

    db.delete_object(object_class, name=object_name)
    with pytest.raises(NotFoundError):
        _ = db.get_object_properties(object_class, object_name)


def test_delete_property_with_scenario(db_base: PlexosDB):
    from plexosdb.enums import ClassEnum

    db = db_base
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    property_name = "Max Capacity"
    scenario_name = "High Demand"

    db.add_object(object_class, object_name)
    db.add_property(object_class, object_name, property_name, 100.0)  # No scenario
    db.add_property(object_class, object_name, property_name, 200.0, scenario=scenario_name)  # With scenario

    properties = db.get_object_properties(object_class, object_name)
    assert len(properties) == 2

    db.delete_property(object_class, object_name, property_name=property_name, scenario=scenario_name)

    properties = db.get_object_properties(object_class, object_name)
    assert len(properties) == 1
    assert properties[0]["value"] == 100.0
    assert properties[0]["scenario_name"] is None


def test_delete_property_fails_with_nonexistent_object(db_base: PlexosDB):
    from plexosdb.enums import ClassEnum
    from plexosdb.exceptions import NotFoundError

    db = db_base

    with pytest.raises(NotFoundError, match="Object = `NonexistentGen` does not exist"):
        db.delete_property(ClassEnum.Generator, "NonexistentGen", property_name="Max Capacity")


def test_delete_property_fails_with_invalid_property_name(db_base: PlexosDB):
    from plexosdb.enums import ClassEnum
    from plexosdb.exceptions import NameError

    db = db_base
    object_name = "TestGen"
    object_class = ClassEnum.Generator

    db.add_object(object_class, object_name)

    with pytest.raises(NameError, match="Property 'Invalid Property' does not exist for collection"):
        db.delete_property(object_class, object_name, property_name="Invalid Property")


def test_delete_property_fails_with_nonexistent_property(db_base: PlexosDB):
    """Test that deleting a property that doesn't exist for the object raises NotFoundError."""
    from plexosdb import ClassEnum
    from plexosdb.exceptions import NotFoundError

    db = db_base
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    property_name = "Max Capacity"

    db.add_object(object_class, object_name)

    with pytest.raises(
        NotFoundError, match=f"Property '{property_name}' not found for object '{object_name}'"
    ):
        db.delete_property(object_class, object_name, property_name=property_name)


def test_delete_property_fails_with_nonexistent_scenario(db_base: PlexosDB):
    """Test that deleting a property with nonexistent scenario raises NotFoundError."""
    from plexosdb import ClassEnum
    from plexosdb.exceptions import NotFoundError

    db = db_base
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    property_name = "Max Capacity"

    db.add_object(object_class, object_name)
    db.add_property(object_class, object_name, property_name, 100.0)

    with pytest.raises(NotFoundError, match="Scenario 'Nonexistent Scenario' does not exist"):
        db.delete_property(
            object_class, object_name, property_name=property_name, scenario="Nonexistent Scenario"
        )


def test_delete_property_fails_with_scenario_not_associated(db_base: PlexosDB):
    from plexosdb import ClassEnum
    from plexosdb.exceptions import NotFoundError

    db = db_base
    object_name = "TestGen"
    object_class = ClassEnum.Generator
    property_name = "Max Capacity"
    scenario_name = "High Demand"
    other_scenario = "Low Demand"

    db.add_object(object_class, object_name)
    db.add_property(object_class, object_name, property_name, 100.0, scenario=scenario_name)
    db.add_scenario(other_scenario)  # Add scenario but don't associate with property

    with pytest.raises(
        NotFoundError, match=f"Property '{property_name}' with scenario '{other_scenario}' not found"
    ):
        db.delete_property(object_class, object_name, property_name=property_name, scenario=other_scenario)
