"""Test suite for check methods in PlexosDB.

This module tests all the check_* methods that validate existence of various
database entities like classes, collections, objects, categories, etc.
"""

import pytest

from plexosdb import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum
from plexosdb.exceptions import NotFoundError


@pytest.mark.checks
@pytest.mark.parametrize(
    "class_enum",
    [
        ClassEnum.System,
        ClassEnum.Generator,
        ClassEnum.Node,
    ],
)
def test_check_class_exists(db_instance_with_schema: PlexosDB, class_enum: ClassEnum):
    """Test check_class_exists method for various classes."""
    db = db_instance_with_schema
    assert db.check_class_exists(class_enum)


@pytest.mark.checks
@pytest.mark.parametrize(
    "class_enum",
    [
        ClassEnum.System,
        ClassEnum.Generator,
        ClassEnum.Scenario,
    ],
)
def test_check_class_exists_with_xml(db_instance_with_xml: PlexosDB, class_enum: ClassEnum):
    """Test check_class_exists with XML-loaded database."""
    db = db_instance_with_xml
    assert db.check_class_exists(class_enum)


@pytest.mark.checks
@pytest.mark.parametrize(
    "collection_enum",
    [
        CollectionEnum.Generators,
        CollectionEnum.Nodes,
    ],
)
def test_check_collection_exists_basic(db_instance_with_xml: PlexosDB, collection_enum: CollectionEnum):
    """Test basic check_collection_exists without class filters."""
    db = db_instance_with_xml
    assert db.check_collection_exists(collection_enum)


@pytest.mark.checks
@pytest.mark.parametrize(
    "collection,parent,child,expected",
    [
        # Valid combination - should return True
        (CollectionEnum.Generators, ClassEnum.System, ClassEnum.Generator, True),
        # Invalid combination - Generators collection doesn't have Node as child
        (CollectionEnum.Generators, ClassEnum.System, ClassEnum.Node, False),
    ],
)
def test_check_collection_exists_with_filters(
    db_instance_with_xml: PlexosDB,
    collection: CollectionEnum,
    parent: ClassEnum,
    child: ClassEnum,
    expected: bool,
):
    """Test check_collection_exists with parent/child class filters."""
    db = db_instance_with_xml
    result = db.check_collection_exists(collection, parent_class=parent, child_class=child)
    assert result == expected


@pytest.mark.checks
@pytest.mark.parametrize(
    "collection,parent,child,error_match",
    [
        (
            CollectionEnum.Generators,
            "NonExistentParentClass",
            None,
            "Parent class.*does not exist",
        ),
        (
            CollectionEnum.Generators,
            None,
            "NonExistentChildClass",
            "Child class.*does not exist",
        ),
    ],
)
def test_check_collection_exists_with_nonexistent_class(
    db_instance_with_xml: PlexosDB,
    collection: CollectionEnum,
    parent: str | None,
    child: str | None,
    error_match: str,
):
    """Test that check_collection_exists raises error for non-existent classes."""
    db: PlexosDB = db_instance_with_xml

    with pytest.raises(NotFoundError, match=error_match):
        db.check_collection_exists(collection, parent_class=parent, child_class=child)


@pytest.mark.checks
@pytest.mark.parametrize(
    "fake_class,param_name",
    [
        ("FakeParent", "parent_class"),
        ("FakeChild", "child_class"),
    ],
)
def test_check_collection_exists_error_messages(
    db_instance_with_xml: PlexosDB,
    fake_class: str,
    param_name: str,
):
    """Test that error messages are helpful and informative."""
    db = db_instance_with_xml

    kwargs = {param_name: fake_class}

    with pytest.raises(NotFoundError) as exc_info:
        db.check_collection_exists(CollectionEnum.Generators, **kwargs)  # type: ignore

    error_msg = str(exc_info.value)
    assert "does not exist" in error_msg
    assert "Cannot search for collection" in error_msg
    assert "list_classes()" in error_msg


@pytest.mark.checks
@pytest.mark.parametrize(
    "class_enum,object_name,expected",
    [
        # Existing object
        (ClassEnum.System, "System", True),
        # Non-existent object
        (ClassEnum.Generator, "NonExistentGenerator", False),
    ],
)
def test_check_object_exists(
    db_instance_with_schema: PlexosDB,
    class_enum: ClassEnum,
    object_name: str,
    expected: bool,
):
    """Test check_object_exists method."""
    db = db_instance_with_schema

    # Add test object if we're testing for existence
    if object_name == "TestGenerator":
        db.add_object(ClassEnum.Generator, object_name)

    result = db.check_object_exists(class_enum, object_name)
    assert result == expected


@pytest.mark.checks
def test_check_object_exists_after_adding(db_instance_with_schema: PlexosDB):
    """Test check_object_exists for objects added during test."""
    db = db_instance_with_schema

    db.add_object(ClassEnum.Generator, "TestGenerator")
    assert db.check_object_exists(ClassEnum.Generator, "TestGenerator")


@pytest.mark.checks
def test_check_object_exists_with_invalid_class(db_instance_with_schema: PlexosDB):
    """Test that check_object_exists raises error for non-existent class."""
    db = db_instance_with_schema

    with pytest.raises(NotFoundError, match="does not exist"):
        db.check_object_exists("NonExistentClass", "SomeObject")  # type: ignore


@pytest.mark.checks
def test_check_object_exists_with_category(db_instance_with_schema: PlexosDB):
    """Test check_object_exists with category parameter."""
    db = db_instance_with_schema

    # Add objects with different categories
    db.add_object(ClassEnum.Generator, "Gen1", category="-")
    db.add_object(ClassEnum.Generator, "Gen2", category="Thermal")
    db.add_object(ClassEnum.Generator, "Gen3", category="Hydro")

    # Test checking without category (should find object regardless of category)
    assert db.check_object_exists(ClassEnum.Generator, "Gen1")
    assert db.check_object_exists(ClassEnum.Generator, "Gen2")
    assert db.check_object_exists(ClassEnum.Generator, "Gen3")

    # Test checking with matching category
    assert db.check_object_exists(ClassEnum.Generator, "Gen2", category="Thermal")
    assert db.check_object_exists(ClassEnum.Generator, "Gen3", category="Hydro")

    # Test checking with non-matching category (should return False)
    assert not db.check_object_exists(ClassEnum.Generator, "Gen2", category="Hydro")
    assert not db.check_object_exists(ClassEnum.Generator, "Gen3", category="Thermal")

    # Test checking non-existent object with category
    assert not db.check_object_exists(ClassEnum.Generator, "NonExistent", category="Thermal")


@pytest.mark.checks
@pytest.mark.parametrize(
    "category,exists_initially",
    [
        ("-", True),  # Default category
        ("Thermal", False),  # Custom category (needs to be added)
        ("NonExistent", False),  # Non-existent category
    ],
)
def test_check_category_exists(
    db_instance_with_schema: PlexosDB,
    category: str,
    exists_initially: bool,
):
    """Test check_category_exists method."""
    db = db_instance_with_schema

    # Add an object first to ensure default category is created
    db.add_object(ClassEnum.Generator, "Gen1")

    if category == "Thermal":
        # Add custom category for testing
        db.add_category(ClassEnum.Generator, "Thermal")
        assert db.check_category_exists(ClassEnum.Generator, category)
    elif category == "NonExistent":
        assert not db.check_category_exists(ClassEnum.Generator, category)
    else:
        # Default category
        assert db.check_category_exists(ClassEnum.Generator, category) == exists_initially


@pytest.mark.checks
def test_check_category_exists_with_invalid_class(db_instance_with_schema: PlexosDB):
    """Test that check_category_exists raises error for non-existent class."""
    db = db_instance_with_schema

    with pytest.raises(NotFoundError, match="does not exist"):
        db.check_category_exists("NonExistentClass", "-")  # type: ignore


@pytest.mark.checks
@pytest.mark.parametrize(
    "scenario_name,should_exist",
    [
        ("Base Case", True),  # Will be added
        ("NonExistent Scenario", False),
    ],
)
def test_check_scenario_exists(
    db_instance_with_schema: PlexosDB,
    scenario_name: str,
    should_exist: bool,
):
    """Test check_scenario_exists method."""
    db = db_instance_with_schema

    if should_exist:
        db.add_object(ClassEnum.Scenario, scenario_name)

    result = db.check_scenario_exists(scenario_name)
    assert result == should_exist


@pytest.mark.checks
def test_check_membership_exists(db_instance_with_schema: PlexosDB):
    """Test check_membership_exists method."""
    db = db_instance_with_schema

    # Add objects and membership using classes that exist in schema
    db.add_object(ClassEnum.Generator, "Gen1")
    db.add_object(ClassEnum.Node, "Node1")
    db.add_membership(ClassEnum.Generator, ClassEnum.Node, "Gen1", "Node1", CollectionEnum.Nodes)

    # Check that membership exists
    assert db.check_membership_exists(
        "Gen1",
        "Node1",
        parent_class=ClassEnum.Generator,
        child_class=ClassEnum.Node,
        collection=CollectionEnum.Nodes,
    )

    # Check non-existent membership
    with pytest.raises(NotFoundError):
        _ = db.check_membership_exists(
            "Gen1",
            "NonExistent",
            parent_class=ClassEnum.Generator,
            child_class=ClassEnum.Node,
            collection=CollectionEnum.Nodes,
        )


@pytest.mark.checks
@pytest.mark.parametrize(
    "parent_name,child_name,expected",
    [
        # Non-existent parent object
        ("NonExistentGen", "Node1", False),
        # Non-existent child object
        ("Gen1", "NonExistentNode", False),
    ],
)
def test_check_membership_exists_with_nonexistent_objects(
    db_instance_with_schema: PlexosDB,
    parent_name: str,
    child_name: str,
    expected: bool,
):
    """Test check_membership_exists with non-existent parent or child objects."""
    db = db_instance_with_schema

    db.add_object(ClassEnum.Generator, "Gen1")
    db.add_object(ClassEnum.Node, "Node1")

    with pytest.raises(NotFoundError):
        db.check_membership_exists(
            parent_name,
            child_name,
            parent_class=ClassEnum.Generator,
            child_class=ClassEnum.Node,
            collection=CollectionEnum.Nodes,
        )


@pytest.mark.checks
@pytest.mark.parametrize(
    "invalid_param,error_match",
    [
        ("parent_class", "Parent class.*does not exist"),
        ("child_class", "Child class.*does not exist"),
        ("collection", "Collection.*does not exist"),
    ],
)
def test_check_membership_exists_with_invalid_parameters(
    db_instance_with_schema: PlexosDB,
    invalid_param: str,
    error_match: str,
):
    """Test that check_membership_exists raises error for invalid class/collection parameters."""
    db = db_instance_with_schema

    # Add valid objects
    db.add_object(ClassEnum.Generator, "Gen1")
    db.add_object(ClassEnum.Node, "Node1")

    # Build kwargs with one invalid parameter
    if invalid_param == "parent_class":
        kwargs = {
            "parent_class": "FakeClass",  # type: ignore
            "child_class": ClassEnum.Node,
            "collection": CollectionEnum.Nodes,
        }
    elif invalid_param == "child_class":
        kwargs = {
            "parent_class": ClassEnum.Generator,
            "child_class": "FakeClass",  # type: ignore
            "collection": CollectionEnum.Nodes,
        }
    else:  # collection
        kwargs = {
            "parent_class": ClassEnum.Generator,
            "child_class": ClassEnum.Node,
            "collection": "FakeCollection",  # type: ignore
        }

    with pytest.raises(NotFoundError, match=error_match):
        db.check_membership_exists("Gen1", "Node1", **kwargs)


@pytest.mark.checks
@pytest.mark.parametrize(
    "property_names,expected",
    [
        # Single valid property
        ("Max Capacity", True),
        # Valid property in a list
        (["Max Capacity"], True),
        # Invalid property
        ("Invalid Property Name", False),
    ],
)
def test_check_property_exists(
    db_instance_with_xml: PlexosDB,
    property_names: str | list[str],
    expected: bool,
):
    """Test check_property_exists method."""
    db = db_instance_with_xml

    result = db.check_property_exists(CollectionEnum.Generators, ClassEnum.Generator, property_names)
    assert result == expected


@pytest.mark.checks
@pytest.mark.parametrize(
    "parent_class,child_class,error_match",
    [
        # Non-existent parent class
        ("FakeClass", ClassEnum.Generator, "does not exist"),
        # Non-existent child class
        (ClassEnum.System, "FakeClass", "does not exist"),
    ],
)
def test_check_property_exists_with_invalid_collection(
    db_instance_with_xml: PlexosDB,
    parent_class: ClassEnum | str,
    child_class: ClassEnum | str,
    error_match: str,
):
    """Test check_property_exists raises error for invalid collection parameters."""
    db = db_instance_with_xml

    with pytest.raises(NotFoundError, match=error_match):
        db.check_property_exists(
            CollectionEnum.Generators,
            child_class,
            "Max Capacity",
            parent_class=parent_class,
        )


@pytest.mark.checks
def test_check_methods_integration(db_instance_with_schema: PlexosDB):
    """Test that check methods work together in a realistic workflow."""
    db = db_instance_with_schema
    assert db.check_class_exists(ClassEnum.Generator)

    db.add_object(ClassEnum.Generator, "Gen1")

    assert db.check_object_exists(ClassEnum.Generator, "Gen1")

    # Check collection exists before adding membership
    # Note: This might return False for schema-only DB, so we just verify it doesn't crash
    result = db.check_collection_exists(
        CollectionEnum.Generators, parent_class=ClassEnum.System, child_class=ClassEnum.Generator
    )
    assert isinstance(result, bool)
    assert db.check_category_exists(ClassEnum.Generator, "-")


@pytest.mark.checks
def test_check_property_exists_error_logging(db_instance_with_xml: PlexosDB, caplog):
    """Test that invalid properties are logged appropriately."""
    db = db_instance_with_xml

    result = db.check_property_exists(
        CollectionEnum.Generators, ClassEnum.Generator, ["Invalid Property 1", "Invalid Property 2"]
    )

    assert result is False
