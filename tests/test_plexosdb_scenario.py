import pytest

from plexosdb.db import PlexosDB
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
