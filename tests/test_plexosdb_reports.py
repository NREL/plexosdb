import pytest

from plexosdb.db import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum
from plexosdb.exceptions import NameError


def test_list_valid_property_report(db_instance_with_schema):
    db: PlexosDB = db_instance_with_schema
    result = db.list_valid_properties_report(
        CollectionEnum.Generators, parent_class_enum=ClassEnum.System, child_class_enum=ClassEnum.Generator
    )
    assert result
    assert len(result) == 1


def test_adding_reports(db_instance_with_schema):
    db: PlexosDB = db_instance_with_schema

    test_object = "test_report"
    property = "Units"
    _ = db.add_object(ClassEnum.Report, name=test_object)
    db.add_report(
        object_name=test_object,
        property=property,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
        child_class=ClassEnum.Generator,
    )

    with pytest.raises(NameError):
        db.add_report(
            object_name=test_object,
            property="WrongProperty",
            collection=CollectionEnum.Generators,
            parent_class=ClassEnum.System,
            child_class=ClassEnum.Generator,
        )


@pytest.mark.xfail(reason="Will fail until we develop get report")
def test_get_report(db_instance_with_schema):
    db: PlexosDB = db_instance_with_schema

    test_object = "TestGen"
    property = "Units"
    _ = db.add_object(ClassEnum.Report, name=test_object)
    db.add_report(
        object_name=test_object,
        property=property,
        collection=CollectionEnum.Generators,
        parent_class=ClassEnum.System,
        child_class=ClassEnum.Generator,
    )

    db = db.get_report(object_name=test_object, property=property)
