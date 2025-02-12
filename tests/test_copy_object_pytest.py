import pytest
from plexosdb.api import PlexosAPI
from plexosdb.enums import ClassEnum, CollectionEnum

DB_FILENAME = "plexosdb.xml"


@pytest.fixture
def setup_database(data_folder):
    # Instantiate the API with an in-memory database.
    api = PlexosAPI(xml_fname=data_folder.joinpath(DB_FILENAME))

    # 1. Create a Node object of class Node.
    _ = api.db.add_object("Node1", ClassEnum.Node, CollectionEnum.Nodes)

    # 2. Create a generator "GenOriginal" in the regular generators collection.
    _ = api.db.add_object("GenOriginal", ClassEnum.Generator, CollectionEnum.Generators)

    # The system membership is automatically added on add_object.
    # Use add_property to add property data for GenOriginal.
    api.db.add_property(
        "GenOriginal",
        "Max Capacity",
        100,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
    )
    api.db.add_property(
        "GenOriginal", "Fuel Price", 1, object_class=ClassEnum.Generator, collection=CollectionEnum.Generators
    )
    api.db.add_property(
        "GenOriginal",
        "Fuel Price",
        1,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        scenario="TestScenario",
    )
    api.db.add_property(
        "GenOriginal",
        "Fuel Price",
        1,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        scenario="TestScenario",
        text={"DataFile": "TestText"},
    )

    # Attach a scenario to the "Units" property.
    # Assume that add_property stores the last inserted property data row id in last_insert_rowid.
    # units_data_id = api.db.last_insert_rowid()``
    # api._update_scenario_for_data(units_data_id, "test scenario")

    # 3. Create an emission object "CO2" for the emission generators collection.
    _ = api.db.add_object("CO2", ClassEnum.Emission, CollectionEnum.Emissions)

    # Instead of creating a separate membership for emissions,
    # use add_property with an override collection to add a property for GenOriginal
    # under the Emission.Generators context.

    # 4. Add a membership where GenOriginal is the parent of Node1.
    api.db.add_membership(
        "GenOriginal",
        "Node1",
        parent_class=ClassEnum.Generator,
        child_class=ClassEnum.Node,
        collection=CollectionEnum.Nodes,
    )
    api.db.add_membership(
        "CO2",
        "GenOriginal",
        parent_class=ClassEnum.Emission,
        child_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
    )

    api.db.add_property(
        "GenOriginal",
        "Production Rate",
        0.02,
        object_class=ClassEnum.Generator,
        parent_object_name="CO2",
        parent_class=ClassEnum.Emission,
        collection=CollectionEnum.Generators,
    )

    return api


def test_copy_object(setup_database):
    api = setup_database
    new_obj_id = api.copy_object("GenOriginal", "GenCopy", ClassEnum.Generator, copy_properties=True)
    assert new_obj_id is not None


def test_copy_object_memberships(setup_database):
    api = setup_database
    _ = api.copy_object("GenOriginal", "GenCopy", ClassEnum.Generator, copy_properties=True)

    # Verify that the new object has memberships corresponding to the original.
    orig_memberships = api.db.get_memberships("GenOriginal", object_class=ClassEnum.Generator)
    copy_memberships = api.db.get_memberships("GenCopy", object_class=ClassEnum.Generator)
    assert len(copy_memberships) >= len(
        orig_memberships
    ), "New object should have at least as many memberships as the original."

    # Verify that properties are copied.
    orig_props = api.db.get_properties("GenOriginal", ClassEnum.Generator)
    copy_props = api.db.get_properties("GenCopy", ClassEnum.Generator)
    assert len(orig_props) == len(
        copy_props
    ), "Copied object should have the same number of properties as the original."


def test_properties_with_scenario_and_text(setup_database):
    api = setup_database

    # Get original properties for GenOriginal.
    orig_props = api.db.get_properties("GenOriginal", ClassEnum.Generator)

    # Copy the object.
    new_obj_id = api.copy_object("GenOriginal", "GenCopy", ClassEnum.Generator, copy_properties=True)
    assert new_obj_id is not None

    # Get properties for the copied object.
    copy_props = api.db.get_properties("GenCopy", ClassEnum.Generator)

    # Check that the total count of properties matches.
    assert len(orig_props) == len(copy_props), "The copied object must have the same number of properties."

    # Check that at least one property with a scenario exists.
    orig_with_scenario = [prop for prop in orig_props if "TestScenario" in str(prop)]
    copy_with_scenario = [prop for prop in copy_props if "TestScenario" in str(prop)]
    assert orig_with_scenario, "Original object should have at least one property with a scenario."
    assert copy_with_scenario, "Copied object should also include the property with a scenario."

    # Check that at least one property with text exists.
    orig_with_text = [prop for prop in orig_props if "TestText" in str(prop)]
    copy_with_text = [prop for prop in copy_props if "TestText" in str(prop)]
    assert orig_with_text, "Original object should have at least one property with text."
    assert copy_with_text, "Copied object should also include the property with text."
