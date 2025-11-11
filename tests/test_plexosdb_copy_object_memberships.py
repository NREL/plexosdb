"""Tests for PlexosDB.copy_object_memberships() method."""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from plexosdb import PlexosDB


def test_copy_object_memberships_empty(db_with_topology: PlexosDB) -> None:
    from plexosdb import ClassEnum

    db_with_topology.add_object(ClassEnum.Generator, "gen-original")
    db_with_topology.add_object(ClassEnum.Generator, "gen-copy")

    mapping = db_with_topology.copy_object_memberships(ClassEnum.Generator, "gen-original", "gen-copy")

    assert isinstance(mapping, dict)
    assert len(mapping) == 0


def test_copy_object_memberships_returns_dict_type(db_with_topology: PlexosDB) -> None:
    from plexosdb import ClassEnum

    db_with_topology.add_object(ClassEnum.Generator, "gen1")
    db_with_topology.add_object(ClassEnum.Generator, "gen2")

    result = db_with_topology.copy_object_memberships(ClassEnum.Generator, "gen1", "gen2")

    assert isinstance(result, dict)


def test_copy_object_memberships_existing_node_membership(
    db_with_topology: PlexosDB,
) -> None:
    from plexosdb import ClassEnum

    db_with_topology.add_object(ClassEnum.Generator, "solar-copy")

    mapping = db_with_topology.copy_object_memberships(ClassEnum.Generator, "solar-01", "solar-copy")

    assert isinstance(mapping, dict)


def test_copy_object_memberships_node_parent(db_with_topology: PlexosDB) -> None:
    from plexosdb import ClassEnum

    db_with_topology.add_object(ClassEnum.Node, "node-copy")

    mapping = db_with_topology.copy_object_memberships(ClassEnum.Node, "node-01", "node-copy")

    assert isinstance(mapping, dict)
