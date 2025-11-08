from __future__ import annotations

from plexosdb import ClassEnum
from plexosdb.enums import CollectionEnum


def test_list_child_objects_returns_children(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Region, "Region1")
    db.add_object(ClassEnum.Node, "Node1")
    db.add_object(ClassEnum.Node, "Node2")

    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "Region1",
        "Node1",
        CollectionEnum.ReferenceNode,
    )
    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "Region1",
        "Node2",
        CollectionEnum.ReferenceNode,
    )

    children = db.list_child_objects(
        "Region1",
        parent_class=ClassEnum.Region,
        child_class=ClassEnum.Node,
        collection=CollectionEnum.ReferenceNode,
    )

    assert len(children) == 2


def test_list_child_objects_without_filters(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Region, "Region2")
    db.add_object(ClassEnum.Node, "Node3")
    db.add_object(ClassEnum.Node, "Node4")

    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "Region2",
        "Node3",
        CollectionEnum.ReferenceNode,
    )
    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "Region2",
        "Node4",
        CollectionEnum.ReferenceNode,
    )

    children = db.list_child_objects(
        "Region2",
        parent_class=ClassEnum.Region,
    )

    assert len(children) >= 2


def test_list_child_objects_with_child_class_filter(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Region, "Region3")
    db.add_object(ClassEnum.Node, "Node5")

    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "Region3",
        "Node5",
        CollectionEnum.ReferenceNode,
    )

    children = db.list_child_objects(
        "Region3",
        parent_class=ClassEnum.Region,
        child_class=ClassEnum.Node,
    )

    assert len(children) >= 1


def test_list_child_objects_returns_empty_for_no_children(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Region, "RegionNoChildren")

    children = db.list_child_objects(
        "RegionNoChildren",
        parent_class=ClassEnum.Region,
    )

    assert children == []


def test_list_child_objects_with_collection_filter(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Region, "RegionColl")
    db.add_object(ClassEnum.Node, "NodeColl")

    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "RegionColl",
        "NodeColl",
        CollectionEnum.ReferenceNode,
    )

    children = db.list_child_objects(
        "RegionColl",
        parent_class=ClassEnum.Region,
        collection=CollectionEnum.ReferenceNode,
    )

    assert len(children) >= 1
    assert "Node" in children[0]["collection_name"]


def test_list_parent_objects_returns_parents(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Region, "Region4")
    db.add_object(ClassEnum.Region, "Region5")
    db.add_object(ClassEnum.Node, "Zone2")

    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "Region4",
        "Zone2",
        CollectionEnum.ReferenceNode,
    )
    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "Region5",
        "Zone2",
        CollectionEnum.ReferenceNode,
    )

    parents = db.list_parent_objects(
        "Zone2",
        child_class=ClassEnum.Node,
        parent_class=ClassEnum.Region,
        collection=CollectionEnum.ReferenceNode,
    )

    assert len(parents) == 2


def test_list_parent_objects_without_filters(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Region, "Region6")
    db.add_object(ClassEnum.Node, "Zone3")

    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "Region6",
        "Zone3",
        CollectionEnum.ReferenceNode,
    )

    parents = db.list_parent_objects(
        "Zone3",
        child_class=ClassEnum.Node,
    )

    assert len(parents) >= 1


def test_list_parent_objects_with_parent_class_filter(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Region, "Region7")
    db.add_object(ClassEnum.Node, "Zone4")

    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "Region7",
        "Zone4",
        CollectionEnum.ReferenceNode,
    )

    parents = db.list_parent_objects(
        "Zone4",
        child_class=ClassEnum.Node,
        parent_class=ClassEnum.Region,
    )

    assert len(parents) >= 1


def test_list_parent_objects_returns_empty_for_system_only(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Node, "ZoneNoParents")

    parents = db.list_parent_objects(
        "ZoneNoParents",
        child_class=ClassEnum.Node,
        parent_class=ClassEnum.Region,
    )

    assert parents == []


def test_list_parent_objects_with_collection_filter(db_base: object) -> None:
    from plexosdb.db import PlexosDB

    db: PlexosDB = db_base

    db.add_object(ClassEnum.Region, "Region8")
    db.add_object(ClassEnum.Node, "Node12")

    db.add_membership(
        ClassEnum.Region,
        ClassEnum.Node,
        "Region8",
        "Node12",
        CollectionEnum.ReferenceNode,
    )

    parents = db.list_parent_objects(
        "Node12",
        child_class=ClassEnum.Node,
        collection=CollectionEnum.ReferenceNode,
    )

    assert len(parents) >= 1
    assert "Node" in parents[0]["collection_name"]
