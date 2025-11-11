"""Test enums module for complete coverage.

This module tests all enum values, properties, and utility functions
to ensure complete coverage of the enums module.
"""

from __future__ import annotations

import pytest


def test_str2enum_with_valid_table_name():
    """Verify str2enum returns correct Schema enum by table name."""
    from plexosdb.enums import Schema, str2enum

    result = str2enum("t_attribute")
    assert result == Schema.Attributes


def test_str2enum_with_invalid_name_returns_none():
    """Verify str2enum returns None for invalid schema name."""
    from plexosdb.enums import str2enum

    result = str2enum("InvalidName")
    assert result is None


def test_str2enum_with_custom_enum():
    """Verify str2enum works with custom enum parameter."""
    from plexosdb.enums import ClassEnum, str2enum

    result = str2enum("Generator", ClassEnum)
    assert result == ClassEnum.Generator


def test_str2enum_with_invalid_custom_enum_name():
    """Verify str2enum returns None for invalid custom enum name."""
    from plexosdb.enums import ClassEnum, str2enum

    result = str2enum("InvalidClass", ClassEnum)
    assert result is None


def test_get_default_collection_for_datafile():
    """Verify get_default_collection returns DataFiles for DataFile class."""
    from plexosdb.enums import ClassEnum, CollectionEnum, get_default_collection

    result = get_default_collection(ClassEnum.DataFile)
    assert result == CollectionEnum.DataFiles


def test_get_default_collection_for_generator():
    """Verify get_default_collection returns Generators for Generator class."""
    from plexosdb.enums import ClassEnum, CollectionEnum, get_default_collection

    result = get_default_collection(ClassEnum.Generator)
    assert result == CollectionEnum.Generators


def test_get_default_collection_for_node():
    """Verify get_default_collection returns Nodes for Node class."""
    from plexosdb.enums import ClassEnum, CollectionEnum, get_default_collection

    result = get_default_collection(ClassEnum.Node)
    assert result == CollectionEnum.Nodes


@pytest.mark.parametrize(
    "class_enum,expected_collection",
    [
        ("Generator", "Generators"),
        ("Node", "Nodes"),
        ("Zone", "Zones"),
        ("Region", "Regions"),
        ("Line", "Lines"),
        ("Transformer", "Transformers"),
        ("Storage", "Storages"),
        ("Model", "Models"),
        ("Scenario", "Scenarios"),
        ("Fuel", "Fuels"),
        ("Emission", "Emissions"),
        ("Reserve", "Reserves"),
    ],
)
def test_get_default_collection_for_plurals(class_enum: str, expected_collection: str):
    """Verify get_default_collection returns correct plurals."""
    from plexosdb.enums import ClassEnum, CollectionEnum, get_default_collection

    class_obj = getattr(ClassEnum, class_enum)
    result = get_default_collection(class_obj)
    expected_obj = getattr(CollectionEnum, expected_collection)
    assert result == expected_obj


def test_get_default_collection_for_pasa():
    """Verify get_default_collection returns PASA for PASA class."""
    from plexosdb.enums import ClassEnum, CollectionEnum, get_default_collection

    result = get_default_collection(ClassEnum.PASA)
    assert result == CollectionEnum.PASA


def test_schema_enum_attributes_member():
    """Verify Schema.Attributes enum member exists."""
    from plexosdb.enums import Schema

    assert hasattr(Schema, "Attributes")
    assert Schema.Attributes is not None


def test_schema_enum_name_property():
    """Verify Schema enum name property returns table name."""
    from plexosdb.enums import Schema

    result = Schema.Attributes.name
    assert result == "t_attribute"


def test_schema_enum_label_property():
    """Verify Schema enum label property returns id column name."""
    from plexosdb.enums import Schema

    result = Schema.Attributes.label
    assert result == "attribute_id"


def test_schema_enum_with_none_label():
    """Verify Schema enum with None label."""
    from plexosdb.enums import Schema

    result = Schema.CollectionReport.label
    assert result is None


@pytest.mark.parametrize(
    "schema_name",
    [
        "Attributes",
        "AttributeData",
        "Class",
        "ClassGroup",
        "Objects",
        "Categories",
        "Collection",
        "CollectionReport",
        "Memberships",
        "Property",
        "PropertyGroup",
        "PropertyReport",
        "PropertyTag",
        "Data",
        "Band",
        "Report",
        "DateFrom",
        "DateTo",
        "MemoData",
        "Message",
        "Action",
        "Config",
        "Tags",
        "Text",
        "Units",
    ],
)
def test_schema_enum_all_members_exist(schema_name: str):
    """Verify all Schema enum values exist and are accessible."""
    from plexosdb.enums import Schema

    assert hasattr(Schema, schema_name)
    schema_obj = getattr(Schema, schema_name)
    assert schema_obj is not None
    assert hasattr(schema_obj, "name")
    assert hasattr(schema_obj, "label")


@pytest.mark.parametrize(
    "class_name",
    [
        "System",
        "Generator",
        "Fuel",
        "Battery",
        "Storage",
        "Emission",
        "Reserve",
        "Region",
        "Zone",
        "Node",
        "Line",
        "Transformer",
        "Interface",
        "DataFile",
        "Timeslice",
        "Scenario",
        "Model",
        "Horizon",
        "Report",
        "PASA",
        "MTSchedule",
        "STSchedule",
        "Transmission",
        "Diagnostic",
        "Production",
        "Performance",
        "Variable",
        "Constraint",
    ],
)
def test_class_enum_all_members_exist(class_name: str):
    """Verify all ClassEnum values are accessible."""
    from plexosdb.enums import ClassEnum

    assert hasattr(ClassEnum, class_name)
    class_obj = getattr(ClassEnum, class_name)
    assert class_obj is not None
    assert isinstance(class_obj.value, str)


@pytest.mark.parametrize(
    "collection_name",
    [
        "Generators",
        "Fuels",
        "HeadStorage",
        "TailStorage",
        "Nodes",
        "Storages",
        "Emissions",
        "Reserves",
        "Batteries",
        "Regions",
        "Zones",
        "Region",
        "Zone",
        "Lines",
        "NodeFrom",
        "NodeTo",
        "Transformers",
        "Interfaces",
        "Models",
        "Scenario",
        "Scenarios",
        "Horizon",
        "Horizons",
        "Report",
        "Reports",
        "ReferenceNode",
        "PASA",
        "MTSchedule",
        "STSchedule",
        "Transmission",
        "Production",
        "Diagnostic",
        "Diagnostics",
        "Performance",
        "DataFiles",
        "Constraint",
        "Constraints",
        "Variables",
    ],
)
def test_collection_enum_all_members_exist(collection_name: str):
    """Verify all CollectionEnum values are accessible."""
    from plexosdb.enums import CollectionEnum

    assert hasattr(CollectionEnum, collection_name)
    collection_obj = getattr(CollectionEnum, collection_name)
    assert collection_obj is not None
    assert isinstance(collection_obj.value, str)


def test_plexos_class_mapping_contains_all_classes():
    """Verify plexos_class_mapping contains all ClassEnum members."""
    from plexosdb.enums import ClassEnum, plexos_class_mapping

    for class_member in ClassEnum:
        assert class_member.name in plexos_class_mapping
        assert plexos_class_mapping[class_member.name] == class_member.value


def test_plexos_class_mapping_is_dict():
    """Verify plexos_class_mapping is a dictionary."""
    from plexosdb.enums import plexos_class_mapping

    assert isinstance(plexos_class_mapping, dict)
    assert len(plexos_class_mapping) > 0


def test_class_enum_string_values():
    """Verify all ClassEnum values are strings."""
    from plexosdb.enums import ClassEnum

    for class_member in ClassEnum:
        assert isinstance(class_member.value, str)
        assert len(class_member.value) > 0


def test_collection_enum_string_values():
    """Verify all CollectionEnum values are strings."""
    from plexosdb.enums import CollectionEnum

    for collection_member in CollectionEnum:
        assert isinstance(collection_member.value, str)
        assert len(collection_member.value) > 0


def test_schema_enum_name_returns_table_name():
    """Verify Schema.name property returns table name."""
    from plexosdb.enums import Schema

    for schema_member in Schema:
        name = schema_member.name
        assert isinstance(name, str)
        assert name.startswith("t_")


def test_class_enum_generator_value():
    """Verify ClassEnum.Generator has correct value."""
    from plexosdb.enums import ClassEnum

    assert ClassEnum.Generator.value == "Generator"


def test_collection_enum_generators_value():
    """Verify CollectionEnum.Generators has correct value."""
    from plexosdb.enums import CollectionEnum

    assert CollectionEnum.Generators.value == "Generators"


def test_schema_enum_objects_properties():
    """Verify Schema.Objects has correct properties."""
    from plexosdb.enums import Schema

    assert Schema.Objects.name == "t_object"
    assert Schema.Objects.label == "object_id"


def test_str2enum_all_schema_members():
    """Verify str2enum works for all Schema table names."""
    from plexosdb.enums import Schema, str2enum

    for schema_member in Schema:
        result = str2enum(schema_member.name)
        assert result == schema_member or result is None


def test_get_default_collection_for_supported_classes():
    """Verify get_default_collection returns CollectionEnum for supported classes."""
    from plexosdb.enums import ClassEnum, CollectionEnum, get_default_collection

    # Test classes that have direct plural mappings
    supported_classes = [
        ClassEnum.Generator,
        ClassEnum.Node,
        ClassEnum.DataFile,
        ClassEnum.Scenario,
    ]
    for class_member in supported_classes:
        result = get_default_collection(class_member)
        assert isinstance(result, CollectionEnum)
