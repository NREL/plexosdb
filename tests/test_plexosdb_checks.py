"""Test suite for check methods in PlexosDB.

This module tests all the check_* methods that validate existence of various
database entities like classes, collections, objects, categories, etc.
"""

from __future__ import annotations

import pytest


@pytest.mark.checks
@pytest.mark.parametrize(
    "class_enum",
    [
        "System",
        "Generator",
        "Node",
        "Scenario",
    ],
)
def test_check_class_exists_returns_true_for_valid_class(db_base, class_enum: str):
    """Verify check_class_exists returns True for valid database classes."""
    from plexosdb.enums import ClassEnum

    class_obj = getattr(ClassEnum, class_enum)
    assert db_base.check_class_exists(class_obj)


@pytest.mark.checks
@pytest.mark.parametrize(
    "collection_enum",
    [
        "Generators",
        "Nodes",
    ],
)
def test_check_collection_exists_returns_true_without_filters(db_base, collection_enum: str):
    """Verify check_collection_exists returns True for valid collections without filters."""
    from plexosdb.enums import CollectionEnum

    collection_obj = getattr(CollectionEnum, collection_enum)
    assert db_base.check_collection_exists(collection_obj)


@pytest.mark.checks
@pytest.mark.parametrize(
    "collection,parent,child,expected",
    [
        ("Generators", "System", "Generator", True),
        ("Generators", "System", "Node", False),
    ],
)
def test_check_collection_exists_with_parent_child_filters(
    db_base,
    collection: str,
    parent: str,
    child: str,
    expected: bool,
):
    """Verify check_collection_exists returns expected result with parent/child filters."""
    from plexosdb.enums import ClassEnum, CollectionEnum

    collection_obj = getattr(CollectionEnum, collection)
    parent_obj = getattr(ClassEnum, parent)
    child_obj = getattr(ClassEnum, child)

    result = db_base.check_collection_exists(collection_obj, parent_class=parent_obj, child_class=child_obj)
    assert result == expected


@pytest.mark.checks
@pytest.mark.parametrize(
    "param_name,invalid_value",
    [
        ("parent_class", "NonExistentParentClass"),
        ("child_class", "NonExistentChildClass"),
    ],
)
def test_check_collection_exists_raises_not_found_for_invalid_class(
    db_base,
    param_name: str,
    invalid_value: str,
):
    """Verify check_collection_exists raises NotFoundError for invalid class parameters."""
    from plexosdb.enums import CollectionEnum
    from plexosdb.exceptions import NotFoundError

    kwargs = {param_name: invalid_value}
    with pytest.raises(NotFoundError):
        db_base.check_collection_exists(CollectionEnum.Generators, **kwargs)  # type: ignore


@pytest.mark.checks
@pytest.mark.parametrize(
    "param_name,invalid_value",
    [
        ("parent_class", "FakeParent"),
        ("child_class", "FakeChild"),
    ],
)
def test_check_collection_exists_error_contains_helpful_message(
    db_base,
    param_name: str,
    invalid_value: str,
):
    """Verify error message contains helpful guidance and available methods."""
    from plexosdb.enums import CollectionEnum
    from plexosdb.exceptions import NotFoundError

    kwargs = {param_name: invalid_value}
    with pytest.raises(NotFoundError) as exc_info:
        db_base.check_collection_exists(CollectionEnum.Generators, **kwargs)  # type: ignore

    error_msg = str(exc_info.value)
    assert "does not exist" in error_msg
    assert "Cannot search for collection" in error_msg
    assert "list_classes()" in error_msg


@pytest.mark.checks
@pytest.mark.parametrize(
    "class_enum,object_name,should_exist",
    [
        ("System", "System", True),
        ("Generator", "NonExistentGenerator", False),
    ],
)
def test_check_object_exists_returns_expected_result(
    db_base,
    class_enum: str,
    object_name: str,
    should_exist: bool,
):
    """Verify check_object_exists returns True for existing objects and False otherwise."""
    from plexosdb.enums import ClassEnum

    class_obj = getattr(ClassEnum, class_enum)
    result = db_base.check_object_exists(class_obj, object_name)
    assert result == should_exist


@pytest.mark.checks
def test_check_object_exists_returns_true_after_adding_object(db_base):
    """Verify check_object_exists returns True for newly added objects."""
    from plexosdb.enums import ClassEnum

    db_base.add_object(ClassEnum.Generator, "TestGenerator")
    assert db_base.check_object_exists(ClassEnum.Generator, "TestGenerator")


@pytest.mark.checks
def test_check_object_exists_raises_not_found_for_invalid_class(db_base):
    """Verify check_object_exists raises NotFoundError for non-existent class."""
    from plexosdb.exceptions import NotFoundError

    with pytest.raises(NotFoundError):
        db_base.check_object_exists("NonExistentClass", "SomeObject")  # type: ignore


@pytest.mark.checks
@pytest.mark.parametrize(
    "object_name,category,should_exist",
    [
        ("Gen1", "-", True),
        ("Gen2", "Thermal", True),
        ("Gen3", "Hydro", True),
        ("Gen2", "Hydro", False),
        ("Gen3", "Thermal", False),
        ("NonExistent", "Thermal", False),
    ],
)
def test_check_object_exists_with_category_parameter(
    db_base,
    object_name: str,
    category: str,
    should_exist: bool,
):
    """Verify check_object_exists correctly filters by category."""
    from plexosdb.enums import ClassEnum

    db = db_base
    db.add_object(ClassEnum.Generator, "Gen1", category="-")
    db.add_object(ClassEnum.Generator, "Gen2", category="Thermal")
    db.add_object(ClassEnum.Generator, "Gen3", category="Hydro")

    result = db.check_object_exists(ClassEnum.Generator, object_name, category=category)
    assert result == should_exist


@pytest.mark.checks
@pytest.mark.parametrize(
    "category,setup_required,expected",
    [
        ("-", False, True),
        ("Thermal", True, True),
        ("NonExistent", False, False),
    ],
)
def test_check_category_exists_returns_expected_result(
    db_base,
    category: str,
    setup_required: bool,
    expected: bool,
):
    """Verify check_category_exists returns True for existing categories and False otherwise."""
    from plexosdb.enums import ClassEnum

    db = db_base
    db.add_object(ClassEnum.Generator, "Gen1")

    if setup_required:
        db.add_category(ClassEnum.Generator, category)

    result = db.check_category_exists(ClassEnum.Generator, category)
    assert result == expected


@pytest.mark.checks
def test_check_category_exists_raises_not_found_for_invalid_class(db_base):
    """Verify check_category_exists raises NotFoundError for non-existent class."""
    from plexosdb.exceptions import NotFoundError

    with pytest.raises(NotFoundError):
        db_base.check_category_exists("NonExistentClass", "-")  # type: ignore


@pytest.mark.checks
@pytest.mark.parametrize(
    "scenario_name,should_exist",
    [
        ("Base Case", True),
        ("NonExistent Scenario", False),
    ],
)
def test_check_scenario_exists_returns_expected_result(
    db_base,
    scenario_name: str,
    should_exist: bool,
):
    """Verify check_scenario_exists returns True for existing scenarios and False otherwise."""
    from plexosdb.enums import ClassEnum

    if should_exist:
        db_base.add_object(ClassEnum.Scenario, scenario_name)

    result = db_base.check_scenario_exists(scenario_name)
    assert result == should_exist


@pytest.mark.checks
def test_check_membership_exists_returns_true_for_valid_membership(db_base):
    """Verify check_membership_exists returns True for existing memberships."""
    from plexosdb.enums import ClassEnum, CollectionEnum

    db = db_base
    db.add_object(ClassEnum.Generator, "Gen1")
    db.add_object(ClassEnum.Node, "Node1")
    db.add_membership(ClassEnum.Generator, ClassEnum.Node, "Gen1", "Node1", CollectionEnum.Nodes)

    result = db.check_membership_exists(
        "Gen1",
        "Node1",
        parent_class=ClassEnum.Generator,
        child_class=ClassEnum.Node,
        collection=CollectionEnum.Nodes,
    )
    assert result


@pytest.mark.checks
@pytest.mark.parametrize(
    "parent_name,child_name",
    [
        ("NonExistentGen", "Node1"),
        ("Gen1", "NonExistentNode"),
    ],
)
def test_check_membership_exists_raises_not_found_for_nonexistent_objects(
    db_base,
    parent_name: str,
    child_name: str,
):
    """Verify check_membership_exists raises NotFoundError for non-existent objects."""
    from plexosdb.enums import ClassEnum, CollectionEnum
    from plexosdb.exceptions import NotFoundError

    db = db_base
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
    "invalid_param,invalid_value",
    [
        ("parent_class", "FakeClass"),
        ("child_class", "FakeClass"),
        ("collection", "FakeCollection"),
    ],
)
def test_check_membership_exists_raises_not_found_for_invalid_parameters(
    db_base,
    invalid_param: str,
    invalid_value: str,
):
    """Verify check_membership_exists raises NotFoundError for invalid class/collection parameters."""
    from plexosdb.enums import ClassEnum, CollectionEnum
    from plexosdb.exceptions import NotFoundError

    db = db_base
    db.add_object(ClassEnum.Generator, "Gen1")
    db.add_object(ClassEnum.Node, "Node1")

    base_kwargs = {
        "parent_class": ClassEnum.Generator,
        "child_class": ClassEnum.Node,
        "collection": CollectionEnum.Nodes,
    }
    base_kwargs[invalid_param] = invalid_value  # type: ignore

    with pytest.raises(NotFoundError):
        db.check_membership_exists("Gen1", "Node1", **base_kwargs)  # type: ignore


@pytest.mark.checks
@pytest.mark.parametrize(
    "property_names,should_exist",
    [
        ("Max Capacity", True),
        (["Max Capacity"], True),
        ("Invalid Property Name", False),
    ],
)
def test_check_property_exists_returns_expected_result(
    db_base,
    property_names: str | list[str],
    should_exist: bool,
):
    """Verify check_property_exists returns True for valid properties and False otherwise."""
    from plexosdb.enums import ClassEnum, CollectionEnum

    result = db_base.check_property_exists(CollectionEnum.Generators, ClassEnum.Generator, property_names)
    assert result == should_exist


@pytest.mark.checks
def test_check_property_exists_raises_not_found_for_invalid_collection(db_base):
    """Verify check_property_exists raises NotFoundError for invalid collection."""
    from plexosdb.exceptions import NotFoundError

    with pytest.raises(NotFoundError):
        db_base.check_property_exists(
            "FakeCollection",  # type: ignore
            "FakeClass",  # type: ignore
            "Max Capacity",
        )


@pytest.mark.checks
def test_check_property_exists_raises_not_found_for_invalid_parent_class(db_base):
    """Verify check_property_exists raises NotFoundError for invalid parent_class."""
    from plexosdb.enums import ClassEnum, CollectionEnum
    from plexosdb.exceptions import NotFoundError

    with pytest.raises(NotFoundError):
        db_base.check_property_exists(
            CollectionEnum.Generators,
            ClassEnum.Generator,
            "Max Capacity",
            parent_class="FakeClass",  # type: ignore
        )


@pytest.mark.checks
def test_check_property_exists_returns_false_for_invalid_properties(
    db_base,
):
    """Verify check_property_exists returns False for multiple invalid properties."""
    from plexosdb.enums import ClassEnum, CollectionEnum

    result = db_base.check_property_exists(
        CollectionEnum.Generators,
        ClassEnum.Generator,
        ["Invalid Property 1", "Invalid Property 2"],
    )
    assert result is False


@pytest.mark.checks
def test_check_class_exists_succeeds_in_workflow(db_base):
    """Verify check_class_exists returns True in realistic workflow."""
    from plexosdb.enums import ClassEnum

    assert db_base.check_class_exists(ClassEnum.Generator)


@pytest.mark.checks
def test_check_object_exists_succeeds_after_add_in_workflow(db_base):
    """Verify check_object_exists returns True after adding object in workflow."""
    from plexosdb.enums import ClassEnum

    db_base.add_object(ClassEnum.Generator, "Gen1")
    assert db_base.check_object_exists(ClassEnum.Generator, "Gen1")


@pytest.mark.checks
def test_check_collection_exists_returns_boolean_in_workflow(db_base):
    """Verify check_collection_exists returns boolean value without errors in workflow."""
    from plexosdb.enums import ClassEnum, CollectionEnum

    result = db_base.check_collection_exists(
        CollectionEnum.Generators,
        parent_class=ClassEnum.System,
        child_class=ClassEnum.Generator,
    )
    assert isinstance(result, bool)


@pytest.mark.checks
def test_check_category_exists_returns_true_for_default_category_in_workflow(
    db_base,
):
    """Verify check_category_exists returns True for default category in workflow."""
    from plexosdb.enums import ClassEnum

    assert db_base.check_category_exists(ClassEnum.Generator, "-")
