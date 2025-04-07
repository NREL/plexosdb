import pytest

from plexosdb.db import PlexosDB
from plexosdb.enums import ClassEnum
from plexosdb.exceptions import NameError


def test_adding_scenaro(db_instance_with_schema):
    db: PlexosDB = db_instance_with_schema

    test_scenario = "Test"
    object_id = db.add_scenario(test_scenario)
    assert object_id

    scenario_id = db.get_scenario_id(test_scenario)
    assert object_id == scenario_id

    test_scenario = "Test"
    with pytest.raises(NameError):
        _ = db.add_scenario(test_scenario)

    test_object_name = "TestGen"
    test_property_name = "Max Capacity"
    test_property_value = 100.0
    _ = db.add_object(ClassEnum.Generator, test_object_name)
    data_id = db.add_property(
        ClassEnum.Generator, test_object_name, test_property_name, test_property_value, scenario=test_scenario
    )
    assert data_id
