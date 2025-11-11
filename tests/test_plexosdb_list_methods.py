"""Tests for PlexosDB list_scenarios(), list_models(), and list_scenarios_by_model() methods."""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from plexosdb import PlexosDB


def test_list_scenarios_with_added_scenario(db_with_topology: PlexosDB) -> None:
    from plexosdb import ClassEnum

    db_with_topology.add_object(ClassEnum.Scenario, "Test Scenario")
    scenarios = db_with_topology.list_scenarios()

    assert "Test Scenario" in scenarios


def test_list_scenarios_multiple(db_with_scenarios: PlexosDB) -> None:
    scenarios = db_with_scenarios.list_scenarios()

    assert isinstance(scenarios, list)
    assert all(isinstance(s, str) for s in scenarios)
    assert "Base" in scenarios


def test_list_models_with_added_model(db_with_topology: PlexosDB) -> None:
    from plexosdb import ClassEnum

    db_with_topology.add_object(ClassEnum.Model, "Test Model")
    models = db_with_topology.list_models()

    assert "Test Model" in models


def test_list_models_returns_list(db_base: PlexosDB) -> None:
    models = db_base.list_models()

    assert isinstance(models, list)
    assert all(isinstance(m, str) for m in models)


def test_list_scenarios_by_model_with_base(db_with_scenarios: PlexosDB) -> None:
    from plexosdb import ClassEnum

    db_with_scenarios.add_object(ClassEnum.Model, "BaseModel")
    db_with_scenarios.add_membership(
        ClassEnum.Model,
        ClassEnum.Scenario,
        "BaseModel",
        "Base",
        "Scenarios",
    )

    scenarios = db_with_scenarios.list_scenarios_by_model("BaseModel")

    assert isinstance(scenarios, list)
    assert all(isinstance(s, str) for s in scenarios)


def test_list_scenarios_by_model_empty(db_with_topology: PlexosDB) -> None:
    from plexosdb import ClassEnum

    db_with_topology.add_object(ClassEnum.Model, "EmptyModel")

    scenarios = db_with_topology.list_scenarios_by_model("EmptyModel")

    assert isinstance(scenarios, list)
    assert len(scenarios) == 0
