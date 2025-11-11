from __future__ import annotations

from plexosdb import ClassEnum


def test_list_categories_returns_list(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    categories = db.list_categories(ClassEnum.Generator)

    assert isinstance(categories, list)


def test_list_categories_for_different_classes(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    gen_categories = db.list_categories(ClassEnum.Generator)
    node_categories = db.list_categories(ClassEnum.Node)

    assert isinstance(gen_categories, list)
    assert isinstance(node_categories, list)


def test_list_collections_without_filters(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    collections = db.list_collections()

    assert len(collections) > 0
    assert any(c["collection_name"] for c in collections)


def test_list_collections_with_parent_class_filter(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    collections = db.list_collections(parent_class=ClassEnum.Generator)

    assert isinstance(collections, list)


def test_list_collections_with_child_class_filter(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    collections = db.list_collections(child_class=ClassEnum.Node)

    assert isinstance(collections, list)


def test_list_collections_with_both_class_filters(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    collections = db.list_collections(parent_class=ClassEnum.Generator, child_class=ClassEnum.Node)

    assert isinstance(collections, list)


def test_get_object_data_ids_returns_ids(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Generator, "TestGen1")
    db.add_property(ClassEnum.Generator, "TestGen1", "Max Capacity", 100.0, band=1)

    data_ids = db.get_object_data_ids(ClassEnum.Generator, "TestGen1")

    assert isinstance(data_ids, list)
    assert len(data_ids) > 0


def test_get_object_data_ids_with_property_filter(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Generator, "TestGen2")
    db.add_property(ClassEnum.Generator, "TestGen2", "Max Capacity", 150.0, band=1)
    db.add_property(ClassEnum.Generator, "TestGen2", "Heat Rate", 9500.0, band=1)

    data_ids = db.get_object_data_ids(ClassEnum.Generator, "TestGen2", property_names=["Max Capacity"])

    assert isinstance(data_ids, list)
    assert len(data_ids) > 0


def test_get_object_data_ids_with_multiple_property_names(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Generator, "TestGen3")
    db.add_property(ClassEnum.Generator, "TestGen3", "Max Capacity", 200.0, band=1)
    db.add_property(ClassEnum.Generator, "TestGen3", "Heat Rate", 9500.0, band=1)
    db.add_property(ClassEnum.Generator, "TestGen3", "Fuel Price", 25.0, band=1)

    data_ids = db.get_object_data_ids(
        ClassEnum.Generator, "TestGen3", property_names=["Max Capacity", "Heat Rate"]
    )

    assert isinstance(data_ids, list)
    assert len(data_ids) > 0


def test_update_object_name(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Generator, "OriginalName")

    result = db.update_object(ClassEnum.Generator, "OriginalName", new_name="UpdatedName")

    assert result is True
    assert db.check_object_exists(ClassEnum.Generator, "UpdatedName")


def test_update_object_description(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Generator, "GenWithDesc")

    result = db.update_object(
        ClassEnum.Generator, "GenWithDesc", new_name="GenWithDesc", new_description="New description"
    )

    assert result is True


def test_update_object_name_and_description(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Generator, "FullUpdate")

    result = db.update_object(
        ClassEnum.Generator,
        "FullUpdate",
        new_name="FullUpdateNew",
        new_description="Complete update",
    )

    assert result is True
    assert db.check_object_exists(ClassEnum.Generator, "FullUpdateNew")
