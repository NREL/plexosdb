import pytest

from plexosdb.api import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum

DB_FILENAME = "plexosdb.xml"


@pytest.fixture
def db(data_folder):
    db = PlexosDB(xml_fname=data_folder.joinpath(DB_FILENAME))
    _ = db._db.add_object("Node1", ClassEnum.Node, CollectionEnum.Nodes)
    _ = db._db.add_object("GenOriginal", ClassEnum.Generator, CollectionEnum.Generators)
    _ = db._db.add_object("CO2", ClassEnum.Emission, CollectionEnum.Emissions)
    db._db.add_membership(
        "GenOriginal",
        "Node1",
        parent_class=ClassEnum.Generator,
        child_class=ClassEnum.Node,
        collection=CollectionEnum.Nodes,
    )
    db._db.add_membership(
        "CO2",
        "GenOriginal",
        parent_class=ClassEnum.Emission,
        child_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
    )
    db._db.add_property(
        "GenOriginal",
        "Max Capacity",
        100,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
    )
    db._db.add_property(
        "GenOriginal", "Fuel Price", 1, object_class=ClassEnum.Generator, collection=CollectionEnum.Generators
    )
    db._db.add_property(
        "GenOriginal",
        "Fuel Price",
        1,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        scenario="TestScenario",
    )
    db._db.add_property(
        "GenOriginal",
        "Fuel Price",
        1,
        object_class=ClassEnum.Generator,
        collection=CollectionEnum.Generators,
        scenario="TestScenario",
        text={"DataFile": "TestText"},
    )
    db._db.add_property(
        "GenOriginal",
        "Production Rate",
        0.02,
        object_class=ClassEnum.Generator,
        parent_object_name="CO2",
        parent_class=ClassEnum.Emission,
        collection=CollectionEnum.Generators,
    )
    return db


def test_copy_object(db):
    db = db
    new_obj_id = db.copy_object("GenOriginal", "GenCopy", ClassEnum.Generator, copy_properties=True)
    assert new_obj_id is not None


def test_copy_object_memberships(db):
    _ = db.copy_object("GenOriginal", "GenCopy", ClassEnum.Generator, copy_properties=True)

    orig_memberships = db._db.get_memberships("GenOriginal", object_class=ClassEnum.Generator)
    copy_memberships = db._db.get_memberships("GenCopy", object_class=ClassEnum.Generator)
    assert len(copy_memberships) >= len(
        orig_memberships
    ), "New object should have at least as many memberships as the original."

    orig_props = db.get_generator_properties("GenOriginal")
    copy_props = db.get_generator_properties("GenCopy")
    assert len(orig_props) == len(
        copy_props
    ), "Copied object should have the same number of properties as the original."


def test_properties_with_scenario_and_text(db: PlexosDB):
    orig_props = db.get_generator_properties("GenOriginal")
    new_obj_id = db.copy_object("GenOriginal", "GenCopy", ClassEnum.Generator, copy_properties=True)
    assert new_obj_id is not None

    copy_props = db.get_generator_properties("GenCopy")

    assert len(orig_props) == len(copy_props), "The copied object must have the same number of properties."

    orig_with_scenario = [prop for prop in orig_props if "TestScenario" in str(prop)]
    copy_with_scenario = [prop for prop in copy_props if "TestScenario" in str(prop)]
    assert orig_with_scenario, "Original object should have at least one property with a scenario."
    assert copy_with_scenario, "Copied object should also include the property with a scenario."

    orig_with_text = [prop for prop in orig_props if "TestText" in str(prop)]
    copy_with_text = [prop for prop in copy_props if "TestText" in str(prop)]
    assert orig_with_text, "Original object should have at least one property with text."
    assert copy_with_text, "Copied object should also include the property with text."
