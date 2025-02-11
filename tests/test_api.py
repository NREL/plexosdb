import tempfile
from pathlib import Path

import pytest

from plexosdb.api import PlexosAPI
from plexosdb.enums import ClassEnum
from plexosdb.exceptions import PropertyNameError

DB_FILENAME = "plexosdb.xml"


# Fixture: a basic seeded api instance.
@pytest.fixture
def api(data_folder):
    # Assumes that an in-memory DB or a test XML file is used.
    return PlexosAPI(xml_fname=data_folder.joinpath(DB_FILENAME))  # Adjust xml_fname if needed.


def test_get_generator_properties(api: PlexosAPI):
    props = api.get_generator_properties("SolarPV01", property_names="Max Capacity")
    assert props, "Expected at least one result"
    assert isinstance(props[0]["value"], int | float)
    assert props[0]["value"] == 384.0


def test_get_all_generator_properties(api: PlexosAPI):
    all_props = api.get_all_generator_properties()
    assert isinstance(all_props, list)
    assert any(prop["generator"] == "SolarPV01" for prop in all_props)


def test_modify_property_default(api: PlexosAPI):
    """
    Test modifying a property in the default scenario.
    The update should modify the matching row.
    """
    default_props = api.get_generator_properties("SolarPV01", property_names="Max Capacity")
    assert default_props, "No default properties found"
    initial_value = default_props[0]["value"]

    new_value = 300
    api.modify_property(
        object_type=ClassEnum.Generator,
        object_name="SolarPV01",
        property_name="Max Capacity",
        new_value=str(new_value),  # passing as string
        scenario=None,
        band=None,
    )
    updated_props = api.get_generator_properties("SolarPV01", property_names="Max Capacity")
    assert any(r["value"] == new_value for r in updated_props)
    if len(updated_props) > 1:
        for rec in updated_props:
            if rec["value"] != new_value:
                assert rec["value"] == initial_value


def test_modify_property_new_scenario(api: PlexosAPI):
    default_props = api.get_generator_properties("SolarPV01", property_names="Max Capacity")
    assert default_props, "Default property missing"
    default_value = default_props[0]["value"]

    new_scenario = "NewScenario"
    new_value = 600
    api.modify_property(
        object_type=ClassEnum.Generator,
        object_name="SolarPV01",
        property_name="Max Capacity",
        new_value=str(new_value),  # passed as string
        scenario=new_scenario,
    )

    scenario_props = api.get_generator_properties("SolarPV01", "Max Capacity", scenario=new_scenario)
    assert scenario_props, f"No properties found for scenario {new_scenario}"
    assert scenario_props[0]["value"] == new_value

    default_after = api.get_generator_properties("SolarPV01", property_names="Max Capacity")
    assert any(r["value"] == default_value for r in default_after), "Default row should remain unchanged"


def test_bulk_modify_properties(api: PlexosAPI):
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
    api.bulk_modify_properties(updates)

    props_scenario = api.get_generator_properties(
        "SolarPV01", property_names="Max Capacity", scenario="TestScenario"
    )
    assert props_scenario, "Scenario update missing"
    assert props_scenario[0]["value"] == 500

    props_default = api.get_generator_properties("SolarPV01", property_names="Max Capacity")
    assert any(r["value"] == 300 for r in props_default), "Default update was not applied correctly"


def test_backup_database(api: PlexosAPI):
    with tempfile.TemporaryDirectory() as tmpdirname:
        backup_path = Path(tmpdirname) / "backup.db"
        api.backup_database(backup_path)
        # Ensure the file is closed before checking its existence and size
        with open(backup_path, "rb"):
            pass
        assert backup_path.exists()
        assert backup_path.stat().st_size > 0


def test_to_xml(api: PlexosAPI):
    with tempfile.TemporaryDirectory() as tmpdirname:
        xml_path = Path(tmpdirname) / "export.xml"
        api.to_xml(xml_path)
        # Check the file exists and is not empty.
        assert xml_path.exists()
        assert xml_path.stat().st_size > 0


def test_modify_property_invalid_property(api: PlexosAPI):
    with pytest.raises(PropertyNameError):
        api.modify_property(
            object_type=ClassEnum.Generator,
            object_name="SolarPV01",
            property_name="NonExistentProperty",
            new_value="123",
        )


def test_modify_property_only_band(api: PlexosAPI):
    props_before = api.get_generator_properties("SolarPV01", property_names="Max Capacity")
    assert props_before, "No default property to begin with."
    old_value = props_before[0]["value"]

    api.modify_property(
        object_type=ClassEnum.Generator,
        object_name="SolarPV01",
        property_name="Max Capacity",
        new_value=None,  # No change to the value
        band="9",
        scenario=None,
    )

    props_after = api.get_generator_properties("SolarPV01", property_names="Max Capacity")
    assert any(r["value"] == old_value for r in props_after), "Value should remain unmodified."


def test_bulk_modify_properties_scenario_only(api: PlexosAPI):
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
    api.bulk_modify_properties(updates)

    props_default = api.get_generator_properties("SolarPV01", "Max Capacity")
    assert any(r["value"] == 777 for r in props_default), "Default scenario not updated."
    props_new_scen = api.get_generator_properties("SolarPV01", "Max Capacity", scenario="NewTestingScenario")
    assert props_new_scen and props_new_scen[0]["value"] == 888, "Scenario row not updated."


def test_modify_property_nonexistent_generator(api: PlexosAPI):
    props_before = api.get_generator_properties("FakeGeneratorX", "Max Capacity")
    assert not props_before, "Generator unexpectedly found."

    api.modify_property(
        object_type=ClassEnum.Generator,
        object_name="FakeGeneratorX",
        property_name="Max Capacity",
        new_value="999",  # Should not update anything
    )
    props_after = api.get_generator_properties("FakeGeneratorX", "Max Capacity")
    assert not props_after, "No rows should have been updated."


def test_get_generator_properties_invalid_generator_names(api: PlexosAPI):
    with pytest.raises(ValueError):
        api.get_generator_properties(42)


def test_get_generator_properties_empty(api: PlexosAPI):
    with pytest.raises(ValueError):
        api.get_generator_properties([])


def test_get_generator_properties_invalid_property_names(api: PlexosAPI):
    with pytest.raises(ValueError):
        api.get_generator_properties("SolarPV01", property_names=42)


def test_modify_property_missing_parameters(api: PlexosAPI):
    with pytest.raises(ValueError):
        api.modify_property(
            object_type=ClassEnum.Generator, object_name="", property_name="Max Capacity", new_value="100"
        )
    with pytest.raises(ValueError):
        api.modify_property(
            object_type=ClassEnum.Generator, object_name="SolarPV01", property_name="", new_value="100"
        )


def test_duplicate_data_row_invalid(api: PlexosAPI, monkeypatch):
    def fake_query(query, params):
        return []

    monkeypatch.setattr(api.db, "query", fake_query)
    with pytest.raises(ValueError):
        api._duplicate_data_row(9999)


def test_bulk_modify_properties_exception(api: PlexosAPI, monkeypatch):
    def raise_exception(*args, **kwargs):
        raise Exception("Test Exception")

    monkeypatch.setattr(api.db, "query", raise_exception)
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
        api.bulk_modify_properties(updates)


def test_copy_object_memberships(api: PlexosAPI):
    object_type = ClassEnum.Generator
    original_memberships = api.db.get_memberships(
        "SolarPV01", object_class=object_type, include_system_membership=True
    )
    new_obj_id = api.copy_object("SolarPV01", "SolarPV01_copy", object_type)
    assert new_obj_id is not None, "copy_object did not return a valid new object id."

    copy_memberships = api.db.get_memberships(
        "SolarPV01_copy", object_class=object_type, include_system_membership=True
    )

    assert len(copy_memberships) == 1, f"Expected 1 membership for the copy, got {len(copy_memberships)}"

    parent_name = copy_memberships[0][2]
    assert parent_name == "System", f"Expected membership parent 'System', got '{parent_name}'"

    assert len(original_memberships) == len(
        copy_memberships
    ), "Membership count for original and copy do not match."
