import tempfile
from pathlib import Path

import pytest

from plexosdb.api import PlexosDB
from plexosdb.enums import ClassEnum
from plexosdb.exceptions import NameError

DB_FILENAME = "plexosdb.xml"


# Fixture: a basic seeded db instance.
@pytest.fixture(scope="function")
def db(data_folder):
    # Assumes that an in-memory DB or a test XML file is used.
    return PlexosDB(xml_fname=data_folder.joinpath(DB_FILENAME))  # Adjust xml_fname if needed.


def test_get_generator_properties(db: PlexosDB):
    props = db.get_generator_properties("SolarPV01", "Max Capacity")
    assert props, "Expected at least one result"
    assert isinstance(props[0]["value"], int | float)
    assert props[0]["value"] == 384.0


def test_get_all_generator_properties(db: PlexosDB):
    all_props = db.get_all_generator_properties()
    assert isinstance(all_props, list)
    assert any(prop["generator"] == "SolarPV01" for prop in all_props)


def test_modify_property_default(db: PlexosDB):
    """
    Test modifying a property in the default scenario.
    The update should modify the matching row.
    """
    default_props = db.get_generator_properties("SolarPV01", "Max Capacity")
    assert default_props, "No default properties found"
    initial_value = default_props[0]["value"]

    new_value = 300
    db.modify_property(
        object_type=ClassEnum.Generator,
        object_name="SolarPV01",
        property_name="Max Capacity",
        new_value=str(new_value),  # passing as string
        scenario=None,
        band=None,
    )
    updated_props = db.get_generator_properties("SolarPV01", "Max Capacity")
    assert any(r["value"] == new_value for r in updated_props)
    if len(updated_props) > 1:
        for rec in updated_props:
            if rec["value"] != new_value:
                assert rec["value"] == initial_value


def test_modify_property_new_scenario(db: PlexosDB):
    default_props = db.get_generator_properties("SolarPV01", "Max Capacity")
    assert default_props, "Default property missing"
    default_value = default_props[0]["value"]

    new_scenario = "NewScenario"
    new_value = 600
    db.modify_property(
        object_type=ClassEnum.Generator,
        object_name="SolarPV01",
        property_name="Max Capacity",
        new_value=str(new_value),  # passed as string
        scenario=new_scenario,
    )

    scenario_props = db.get_generator_properties("SolarPV01", "Max Capacity", scenario=new_scenario)
    assert scenario_props, f"No properties found for scenario {new_scenario}"
    assert scenario_props[0]["value"] == new_value

    default_after = db.get_generator_properties("SolarPV01", "Max Capacity")
    assert any(r["value"] == default_value for r in default_after), "Default row should remain unchanged"


def test_bulk_modify_properties(db: PlexosDB):
    updates = [
        {
            "object_type": ClassEnum.Generator,
            "object_name": "SolarPV01",
            "property_name": "Max Capacity",
            "new_value": 300,
            "scenario": None,
            "band": None,
        },
        {
            "object_type": ClassEnum.Generator,
            "object_name": "SolarPV01",
            "property_name": "Max Capacity",
            "new_value": 500,
            "scenario": "TestScenario",
            "band": "2",
        },
    ]
    db.bulk_modify_properties(updates)

    props_scenario = db.get_generator_properties("SolarPV01", "Max Capacity", scenario="TestScenario")
    assert props_scenario, "Scenario update missing"
    assert props_scenario[0]["value"] == 500

    props_default = db.get_generator_properties("SolarPV01", "Max Capacity")
    assert any(r["value"] == 300 for r in props_default), "Default update was not applied correctly"


def test_backup_database(db: PlexosDB):
    with tempfile.TemporaryDirectory() as tmpdirname:
        backup_path = Path(tmpdirname) / "backup.db"
        db.backup_database(backup_path)
        # Ensure the file is closed before checking its existence and size
        with open(backup_path, "rb"):
            pass
        assert backup_path.exists()
        assert backup_path.stat().st_size > 0


def test_to_xml(db: PlexosDB):
    with tempfile.TemporaryDirectory() as tmpdirname:
        xml_path = Path(tmpdirname) / "export.xml"
        db.to_xml(xml_path)
        # Check the file exists and is not empty.
        assert xml_path.exists()
        assert xml_path.stat().st_size > 0


def test_modify_property_invalid_property(db: PlexosDB):
    with pytest.raises(NameError):
        db.modify_property(
            object_type=ClassEnum.Generator,
            object_name="SolarPV01",
            property_name="NonExistentProperty",
            new_value="123",
        )


def test_modify_property_only_band(db: PlexosDB):
    props_before = db.get_generator_properties("SolarPV01", "Max Capacity")
    assert props_before, "No default property to begin with."
    old_value = props_before[0]["value"]

    db.modify_property(
        object_type=ClassEnum.Generator,
        object_name="SolarPV01",
        property_name="Max Capacity",
        new_value=None,  # No change to the value
        band="9",
        scenario=None,
    )

    props_after = db.get_generator_properties("SolarPV01", "Max Capacity")
    assert any(r["value"] == old_value for r in props_after), "Value should remain unmodified."


def test_bulk_modify_properties_scenario_only(db: PlexosDB):
    updates = [
        {
            "object_type": ClassEnum.Generator,
            "object_name": "SolarPV01",
            "property_name": "Max Capacity",
            "new_value": 777,
            "scenario": None,  # Default
            "band": None,
        },
        {
            "object_type": ClassEnum.Generator,
            "object_name": "SolarPV01",
            "property_name": "Max Capacity",
            "new_value": 888,
            "scenario": "NewTestingScenario",
            "band": None,
        },
    ]
    db.bulk_modify_properties(updates)

    props_default = db.get_generator_properties("SolarPV01", "Max Capacity")
    assert any(r["value"] == 777 for r in props_default), "Default scenario not updated."
    props_new_scen = db.get_generator_properties("SolarPV01", "Max Capacity", scenario="NewTestingScenario")
    assert props_new_scen and props_new_scen[0]["value"] == 888, "Scenario row not updated."


def test_modify_property_nonexistent_generator(db: PlexosDB):
    props_before = db.get_generator_properties("FakeGeneratorX", "Max Capacity")
    assert not props_before, "Generator unexpectedly found."

    db.modify_property(
        object_type=ClassEnum.Generator,
        object_name="FakeGeneratorX",
        property_name="Max Capacity",
        new_value="999",  # Should not update anything
    )
    props_after = db.get_generator_properties("FakeGeneratorX", "Max Capacity")
    assert not props_after, "No rows should have been updated."


def test_get_generator_properties_invalid_generator_names(db: PlexosDB):
    with pytest.raises(ValueError):
        db.get_generator_properties(42)


def test_get_generator_properties_empty(db: PlexosDB):
    with pytest.raises(ValueError):
        db.get_generator_properties([])


def test_get_generator_properties_invalid_property_names(db: PlexosDB):
    with pytest.raises(ValueError):
        db.get_generator_properties("SolarPV01", 42)


def test_modify_property_missing_parameters(db: PlexosDB):
    with pytest.raises(ValueError):
        db.modify_property(
            object_type=ClassEnum.Generator, object_name="", property_name="Max Capacity", new_value="100"
        )
    with pytest.raises(ValueError):
        db.modify_property(
            object_type=ClassEnum.Generator, object_name="SolarPV01", property_name="", new_value="100"
        )


def test_duplicate_data_row_invalid(db: PlexosDB, monkeypatch):
    def fake_query(query, params):
        return []

    monkeypatch.setattr(db, "query", fake_query)
    with pytest.raises(ValueError):
        db._db._duplicate_data_row(9999)


def test_bulk_modify_properties_exception(db: PlexosDB, monkeypatch):
    def raise_exception(*args, **kwargs):
        raise Exception("Test Exception")

    monkeypatch.setattr(db._db, "query", raise_exception)
    updates = [
        {
            "object_type": ClassEnum.Generator,
            "object_name": "SolarPV01",
            "property_name": "Max Capacity",
            "new_value": "300",
            "scenario": None,
            "band": None,
        }
    ]
    with pytest.raises(Exception, match="Test Exception"):
        db.bulk_modify_properties(updates)
