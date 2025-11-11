"""Tests for PlexosDB.iterate_properties() method."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from plexosdb import PlexosDB


def test_iterate_properties_with_class_enum_only(db_with_topology: PlexosDB) -> None:
    from plexosdb import ClassEnum

    results = list(db_with_topology.iterate_properties(class_enum=ClassEnum.Generator))

    assert isinstance(results, list)


def test_iterate_properties_with_parent_class_only(db_with_topology: PlexosDB) -> None:
    from plexosdb import ClassEnum

    results = list(db_with_topology.iterate_properties(parent_class=ClassEnum.System))

    assert isinstance(results, list)


def test_iterate_properties_with_collection_validation(db_with_topology: PlexosDB) -> None:
    from plexosdb import ClassEnum, CollectionEnum

    results = list(
        db_with_topology.iterate_properties(
            class_enum=ClassEnum.Generator,
            parent_class=ClassEnum.System,
            collection=CollectionEnum.Generators,
        )
    )

    assert isinstance(results, list)


def test_iterate_properties_with_object_names_validation(db_thermal_gen: PlexosDB) -> None:
    from plexosdb import ClassEnum

    results = list(
        db_thermal_gen.iterate_properties(
            class_enum=ClassEnum.Generator,
            object_names="thermal-01",
        )
    )

    assert isinstance(results, list)


def test_iterate_properties_with_property_names_and_collection_class(
    db_thermal_gen: PlexosDB,
) -> None:
    from plexosdb import ClassEnum, CollectionEnum

    results = list(
        db_thermal_gen.iterate_properties(
            class_enum=ClassEnum.Generator,
            property_names="Max Capacity",
            collection=CollectionEnum.Generators,
        )
    )

    assert isinstance(results, list)


def test_iterate_properties_with_property_names_no_collection(db_thermal_gen: PlexosDB) -> None:
    results = list(
        db_thermal_gen.iterate_properties(
            property_names="Max Capacity",
        )
    )

    assert isinstance(results, list)


def test_iterate_properties_with_category_check(db_with_topology: PlexosDB) -> None:
    from plexosdb import ClassEnum
    from plexosdb.exceptions import NotFoundError

    with pytest.raises(NotFoundError, match="Category 'nonexistent' does not exist"):
        list(
            db_with_topology.iterate_properties(
                class_enum=ClassEnum.Generator,
                category="nonexistent",
            )
        )


def test_iterate_properties_yields_property_records(db_thermal_gen: PlexosDB) -> None:
    from plexosdb import ClassEnum

    results = list(
        db_thermal_gen.iterate_properties(
            class_enum=ClassEnum.Generator,
            object_names="thermal-01",
        )
    )

    assert len(results) > 0
    for record in results:
        assert isinstance(record, dict)
