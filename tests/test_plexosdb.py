import pytest

from plexosdb.db import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum
from plexosdb.exceptions import NameError, NoPropertiesError


def test_smoke_test():
    from plexosdb.db import PlexosDB  # noqa: F401


def test_initialize_instance():
    """Test creating a SQLiteManager instance."""
    db = PlexosDB()
    assert db is not None
    assert getattr(db, "_db") is not None
    assert isinstance(db, PlexosDB)


@pytest.mark.empty_database
@pytest.mark.parametrize(
    "table_name",
    [
        "t_assembly",
        "t_class_group",
        "t_config",
        "t_property_group",
        "t_unit",
        "t_action",
        "t_message",
        "t_property_tag",
        "t_custom_rule",
        "t_class",
        "t_collection",
        "t_collection_report",
        "t_property",
        "t_property_report",
        "t_custom_column",
        "t_attribute",
        "t_category",
        "t_object",
        "t_memo_object",
        "t_report",
        "t_object_meta",
        "t_attribute_data",
        "t_membership",
        "t_memo_membership",
        "t_membership_meta",
        "t_data",
        "t_date_from",
        "t_date_to",
        "t_tag",
        "t_text",
        "t_memo_data",
        "t_data_meta",
        "t_band",
    ],
)
def test_schema_creation(db_instance_with_schema, table_name):
    tables = db_instance_with_schema.query("SELECT name FROM sqlite_master WHERE type='table'")
    table_names = [row[0] for row in tables]
    assert table_name in table_names
    table_name in db_instance_with_schema._db.tables


def test_get_plexos_version(db_instance_with_schema):
    db = db_instance_with_schema
    assert db.version == (9, 2)
    db.get_plexos_version() == (9, 2)


def test_plexosdb_constructor_from_xml(db_instance_with_xml):
    db = db_instance_with_xml
    assert isinstance(db, PlexosDB)
    result = db.query("SELECT * from t_object")
    assert result


def test_get_class_id(db_instance_with_xml):
    db = db_instance_with_xml
    result = db.get_class_id(ClassEnum.System)
    assert result == 1


def test_checks(db_instance_with_xml):
    db = db_instance_with_xml
    result = db.check_category_exists(ClassEnum.System, "-")
    assert result


def test_category_operations(db_instance_with_schema):
    db = db_instance_with_schema
    category_id = db.add_category(ClassEnum.Generator, "test_category")
    assert category_id

    category_id_original = db.add_category(ClassEnum.Generator, "test_category2")
    assert category_id_original == 2

    # Test that we return the same id if we add a repeated category
    category_id_repeated = db.add_category(ClassEnum.Generator, "test_category2")
    assert category_id_original == category_id_repeated

    categories = db.list_categories(ClassEnum.Generator)
    assert len(categories) == 2


def test_membership_operations(db_instance_with_schema):
    db: PlexosDB = db_instance_with_schema
    parent_object_name = "TestGen"
    parent_class = ClassEnum.Generator
    _ = db.add_object(parent_class, parent_object_name)
    child_object_name = "TestNode"
    child_class = ClassEnum.Node
    _ = db.add_object(child_class, child_object_name)
    collection = CollectionEnum.Nodes
    membership_id = db.add_membership(
        parent_class, child_class, parent_object_name, child_object_name, collection
    )
    assert membership_id == db.get_membership_id(parent_object_name, child_object_name, collection)

    memberships = db.get_object_memberships(child_object_name, class_enum=child_class)
    assert memberships
    assert len(memberships) == 1
    assert memberships[0]["membership_id"] == membership_id
    assert memberships[0]["child"] == child_object_name
    assert memberships[0]["parent"] == parent_object_name


def test_object_operations(db_instance_with_schema):
    db = db_instance_with_schema

    test_object_name = "TestGen"
    object_id = db.add_object(ClassEnum.Generator, test_object_name)
    assert object_id
    assert db.check_object_exists(ClassEnum.Generator, test_object_name)

    # Make sure we get the same object_id
    object_id_query = db.get_object_id(ClassEnum.Generator, test_object_name)
    assert object_id == object_id_query

    test_object_name = "TestGen2"
    test_object_category = "Thermal"
    object_id = db.add_object(ClassEnum.Generator, test_object_name, category=test_object_category)
    test_object_id = db.get_object_id(ClassEnum.Generator, test_object_name, category=test_object_category)
    assert object_id == test_object_id

    test_object_name = "TestGen3"
    test_object_category = "Thermal"
    test_object_description = "Thermal Generator for area 1."
    object_id = db.add_object(
        ClassEnum.Generator,
        test_object_name,
        category=test_object_category,
        description=test_object_description,
    )
    test_object_id = db.get_object_id(ClassEnum.Generator, test_object_name)
    assert object_id == test_object_id

    with pytest.raises(NoPropertiesError):
        _ = db.get_object_properties(ClassEnum.Generator, test_object_name, category=test_object_category)

    assert len(db.list_objects_by_class(ClassEnum.Generator)) == 3


@pytest.mark.adders
def test_add_property_to_object(db_instance_with_schema):
    db = db_instance_with_schema
    test_object_name = "TestGen"
    test_property_name = "Max Capacity"
    test_property_value = 100.0

    with pytest.raises(NameError):
        _ = db.add_property(ClassEnum.Generator, test_object_name, test_property_name, test_property_value)

    _ = db.add_object(ClassEnum.Generator, test_object_name)
    data_id = db.add_property(ClassEnum.Generator, test_object_name, test_property_name, test_property_value)
    object_properties = db.get_object_properties(ClassEnum.Generator, test_object_name)
    assert object_properties
    assert object_properties[0]["name"] == test_object_name
    assert object_properties[0]["property"] == test_property_name
    assert object_properties[0]["value"] == test_property_value

    test_property_name = "Max Capacity"
    test_property_value = 100.0
    scenario = "TestScenario"
    data_id_scenario = db.add_property(
        ClassEnum.Generator,
        test_object_name,
        test_property_name,
        test_property_value,
        scenario=scenario,
    )
    assert data_id != data_id_scenario
    object_properties_scenario = db.get_object_properties(ClassEnum.Generator, test_object_name)
    assert object_properties
    assert len(object_properties_scenario) == 2  # 2 properties so far
    assert object_properties_scenario[0]["name"] == test_object_name
    assert object_properties_scenario[0]["property"] == test_property_name
    assert object_properties_scenario[0]["value"] == test_property_value
    assert object_properties_scenario[0]["scenario"] is None
    assert object_properties_scenario[1]["name"] == test_object_name
    assert object_properties_scenario[1]["property"] == test_property_name
    assert object_properties_scenario[1]["scenario"] == scenario

    test_object_name = "TestGen"
    test_property_name = "Max Energy"
    test_property_value = 200.0
    data_id = db.add_property(ClassEnum.Generator, test_object_name, test_property_name, test_property_value)
    object_properties = db.get_object_properties(
        ClassEnum.Generator, test_object_name, property_names="Max Energy"
    )
    assert len(object_properties) == 1  # Only one property asked
    object_properties = db.get_object_properties(
        ClassEnum.Generator, test_object_name, property_names=["Max Energy", "Max Capacity"]
    )
    assert len(object_properties) == 3

    data_ids = db.get_object_data_ids(ClassEnum.Generator, test_object_name)
    assert data_id in data_ids

    with pytest.raises(KeyError):
        _ = db.get_object_data_ids(ClassEnum.Generator, test_object_name, category="bad_category")


def test_add_property_to_object_with_text(db_instance_with_schema):
    db = db_instance_with_schema
    test_object_name = "TestGen"
    test_property_name = "Max Capacity"
    test_property_value = 100.0
    text = {"DataFile": "NonExisting"}
    _ = db.add_object(ClassEnum.Generator, test_object_name)
    _ = db.add_property(
        ClassEnum.Generator, test_object_name, test_property_name, test_property_value, text=text
    )
    object_properties = db.get_object_properties(ClassEnum.Generator, test_object_name)
    assert object_properties
    assert object_properties[0]["name"] == test_object_name
    assert object_properties[0]["property"] == test_property_name
    assert object_properties[0]["value"] == test_property_value
    assert object_properties[0]["texts"] == "NonExisting"


def test_invalid_property(db_instance_with_schema):
    db = db_instance_with_schema
    test_object_name = "TestGen"
    test_property_name = "Max Energy"
    test_property_value = 200.0
    _ = db.add_object(ClassEnum.Generator, test_object_name)
    _ = db.add_property(ClassEnum.Generator, test_object_name, test_property_name, test_property_value)

    test_property_name = "Wrong Property"
    test_property_value = 100.0
    with pytest.raises(NameError):
        _ = db.add_property(ClassEnum.Generator, test_object_name, test_property_name, test_property_value)

    with pytest.raises(NameError):
        _ = db.get_object_properties(ClassEnum.Generator, test_object_name, property_names=test_property_name)


@pytest.mark.getters
def test_get_object_properties(db_instance_with_schema):
    db = db_instance_with_schema
    test_object_name = "TestGen"
    test_property_name = "Max Capacity"
    test_property_value = 100.0
    _ = db.add_object(ClassEnum.Generator, test_object_name)
    _ = db.add_property(ClassEnum.Generator, test_object_name, test_property_name, test_property_value)
    object_properties = db.get_object_properties(ClassEnum.Generator, test_object_name)
    assert object_properties
    assert object_properties[0]["name"] == test_object_name
    assert object_properties[0]["property"] == test_property_name
    assert object_properties[0]["value"] == test_property_value


@pytest.mark.listing
def test_list_scenarios(db_instance_with_schema):
    db = db_instance_with_schema
    test_object_name = "TestGen"
    test_property_name = "Max Capacity"
    test_property_value = 100.0
    test_scenario = "Ramping"
    _ = db.add_object(ClassEnum.Generator, test_object_name)
    _ = db.add_property(
        ClassEnum.Generator,
        test_object_name,
        test_property_name,
        test_property_value,
        scenario=test_scenario,
    )
    scenarios = db.list_scenarios()
    assert len(scenarios) == 1
    assert scenarios[0] == test_scenario


@pytest.mark.listing
def test_list_units(db_instance_with_schema):
    db = db_instance_with_schema
    units = db.list_units()
    assert len(units) == 3
    assert units[0][1] == "MW"


@pytest.mark.listing
def test_list_classes(db_instance_with_schema):
    db = db_instance_with_schema
    classes = db.list_classes()
    assert len(classes) == 8
    assert classes[0] == ClassEnum.System


@pytest.mark.export
def test_export_to_xml(db_instance_with_schema, tmp_path):
    db = db_instance_with_schema
    fpath = tmp_path / "test.xml"
    db.to_xml(fpath)
    assert fpath.exists()


@pytest.mark.export
def test_xml_round_trip(db_instance_with_schema, tmp_path):
    original_db = db_instance_with_schema
    fpath = tmp_path / "test.xml"
    original_db.to_xml(fpath)
    assert fpath.exists()

    deserialized_db = PlexosDB.from_xml(fpath)
    tables = [
        table[0] for table in original_db._db.iter_query("SELECT name from sqlite_master WHERE type='table'")
    ]
    for table_name in tables:
        assert len(original_db.query(f"SELECT * FROM {table_name}")) == len(
            deserialized_db.query(f"SELECT * FROM {table_name}")
        ), "Different number of rows encounter."


@pytest.mark.export
def test_xml_not_exist():
    with pytest.raises(FileNotFoundError):
        _ = PlexosDB.from_xml("not/existing/path")
