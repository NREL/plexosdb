from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from plexosdb.db import PlexosDB


def test_list_valid_property_report(db_base: PlexosDB):
    from plexosdb import ClassEnum, CollectionEnum

    db: PlexosDB = db_base
    result = db.list_valid_properties_report(
        CollectionEnum.Generators, parent_class_enum=ClassEnum.System, child_class_enum=ClassEnum.Generator
    )
    assert result
    assert len(result) > 1  # Reports vary by revision


def test_adding_reports(db_base: PlexosDB):
    from plexosdb import ClassEnum, CollectionEnum
    from plexosdb.exceptions import NameError

    db: PlexosDB = db_base

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
