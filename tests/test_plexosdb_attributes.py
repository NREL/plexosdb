from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from plexosdb.db import PlexosDB


def test_add_attribute(db_base: PlexosDB):
    from plexosdb.enums import ClassEnum

    db: PlexosDB = db_base
    attribute_id = db.get_attribute_id(ClassEnum.Generator, "Latitude")
    assert attribute_id
    assert attribute_id == 1

    _ = db.add_object(ClassEnum.Generator, "TestGen")
    attribute_id_insert = db.add_attribute(
        ClassEnum.Generator, "TestGen", attribute_name="Latitude", attribute_value=10.1
    )

    assert attribute_id == attribute_id_insert

    result = db.get_attribute(ClassEnum.Generator, object_name="TestGen", attribute_name="Latitude")[0]
    assert result
    assert result == 10.1


def test_list_attributes(db_base: PlexosDB):
    from plexosdb.enums import ClassEnum

    db: PlexosDB = db_base

    result = db.list_attributes(ClassEnum.Generator)
    assert result
    assert len(result) == 2
