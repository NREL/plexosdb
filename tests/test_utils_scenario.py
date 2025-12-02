"""Tests for scenario utility functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from plexosdb import PlexosDB


def test_get_scenario_id_creates_new_scenario(db_with_topology: PlexosDB) -> None:
    """Test that get_scenario_id creates scenario if it doesn't exist."""
    from plexosdb.utils import get_scenario_id

    scenario_id = get_scenario_id(db_with_topology, "NewScenario")

    assert isinstance(scenario_id, int)
    assert scenario_id > 0

    scenario_exists = db_with_topology.check_scenario_exists("NewScenario")
    assert scenario_exists is True


def test_get_scenario_id_returns_existing_scenario(db_with_topology: PlexosDB) -> None:
    """Test that get_scenario_id returns existing scenario without creating new one."""
    from plexosdb.utils import get_scenario_id

    initial_id = db_with_topology.add_scenario("ExistingScenario")

    returned_id = get_scenario_id(db_with_topology, "ExistingScenario")

    assert returned_id == initial_id


def test_get_scenario_id_return_type(db_with_topology: PlexosDB) -> None:
    """Test that get_scenario_id returns integer type."""
    from plexosdb.utils import get_scenario_id

    scenario_id = get_scenario_id(db_with_topology, "TestScenario")

    assert isinstance(scenario_id, int)


def test_get_scenario_id_different_names(db_with_topology: PlexosDB) -> None:
    """Test that get_scenario_id handles various scenario names."""
    from plexosdb.utils import get_scenario_id

    names = ["Scenario-1", "Test_Scenario_2", "scenario with spaces", "S3"]

    ids = [get_scenario_id(db_with_topology, name) for name in names]

    assert all(isinstance(id_, int) and id_ > 0 for id_ in ids)

    assert len(set(ids)) == len(ids)

    for name in names:
        assert db_with_topology.check_scenario_exists(name) is True
