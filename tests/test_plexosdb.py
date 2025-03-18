import shutil
import uuid

import pytest

from plexosdb.enums import ClassEnum, CollectionEnum
from plexosdb.exceptions import PropertyNameError
from plexosdb.plexosdb import PlexosDB

DB_FILENAME = "plexosdb.xml"


def test_smoke_test():
    from plexosdb.plexosdb import PlexosDB  # noqa: F401


@pytest.fixture(scope="session")
def db_instance():
    """Create a base DB instance that lasts the entire test session."""
    db = PlexosDB()
    yield db


@pytest.fixture(scope="function")
def db_instance_with_schema() -> PlexosDB:
    """Create a base DB instance that lasts the entire test session."""
    db = PlexosDB()
    db.create_schema()
    with db._db.transaction():
        db._db.execute(
            "INSERT INTO t_class(class_id, name, description) VALUES (1, 'System', 'System class')"
        )
        db._db.execute(
            "INSERT INTO t_class(class_id, name, description) VALUES (2, 'Generator', 'Generator class')"
        )
        db._db.execute("INSERT INTO t_class(class_id, name, description) VALUES (3, 'Node', 'Node class')")
        db._db.execute(
            "INSERT INTO t_class(class_id, name, description) VALUES (4, 'Scenario', 'Scenario class')"
        )
        db._db.execute(
            "INSERT INTO t_object(object_id, name, class_id, GUID) VALUES (1, 'System', 1, ?)",
            (str(uuid.uuid4()),),
        )
        db._db.execute(
            "INSERT INTO t_collection(collection_id, parent_class_id, child_class_id, name) "
            "VALUES (1, 1, 2, 'Generators')"
        )
        db._db.execute(
            "INSERT INTO t_collection(collection_id, parent_class_id, child_class_id, name) "
            "VALUES (2, 1, 3, 'Nodes')"
        )
        db._db.execute(
            "INSERT INTO t_collection(collection_id, parent_class_id, child_class_id, name) "
            "VALUES (3, 2, 3, 'Nodes')"
        )
        db._db.execute(
            "INSERT INTO t_collection(collection_id, parent_class_id, child_class_id, name) "
            "VALUES (4, 1, 4, 'Scenario')"
        )
        db._db.execute("INSERT INTO t_unit(unit_id, value) VALUES (1,'MW')")
        db._db.execute("INSERT INTO t_unit(unit_id, value) VALUES (2,'MWh')")
        db._db.execute("INSERT INTO t_unit(unit_id, value) VALUES (3,'%')")
        db._db.execute(
            "INSERT INTO t_property(property_id, collection_id, unit_id, name) VALUES (1,1,1, 'Max Capacity')"
        )
        db._db.execute(
            "INSERT INTO t_property(property_id, collection_id, unit_id, name) VALUES (2,1,2, 'Max Energy')"
        )
        db._db.execute(
            "INSERT INTO t_property(property_id, collection_id, unit_id, name) "
            "VALUES (3,1,1, 'Rating Factor')"
        )
    yield db


@pytest.fixture()
def db_instance_with_xml(data_folder, tmp_path):
    """Create a base DB instance that lasts the entire test session."""
    xml_fname = data_folder / DB_FILENAME
    xml_copy = tmp_path / f"copy_{DB_FILENAME}"
    shutil.copy(xml_fname, xml_copy)
    db = PlexosDB.from_xml(xml_path=xml_copy)
    yield db
    xml_copy.unlink()


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
    table_names = [row["name"] for row in tables]
    assert table_name in table_names


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
    result = db.check_category_exists("-", ClassEnum.System)
    assert result


def test_category_operations(db_instance_with_schema):
    db = db_instance_with_schema
    category_id = db.add_category("test_category", class_name=ClassEnum.Generator)
    assert category_id

    category_id_original = db.add_category("test_category2", class_name=ClassEnum.Generator)
    assert category_id_original == 2

    # Test that we return the same id if we add a repeated category
    category_id_repeated = db.add_category("test_category2", class_name=ClassEnum.Generator)
    assert category_id_original == category_id_repeated

    categories = db.list_categories(ClassEnum.Generator)
    assert len(categories) == 2


def test_membership_operations(db_instance_with_schema):
    db = db_instance_with_schema
    parent_object_name = "TestGen"
    parent_class = ClassEnum.Generator
    _ = db.add_object(parent_object_name, parent_class)
    child_object_name = "TestNode"
    child_class = ClassEnum.Node
    _ = db.add_object(child_object_name, child_class)
    collection = CollectionEnum.Nodes
    membership_id = db.add_membership(
        parent_object_name, child_object_name, parent_class, child_class, collection
    )
    assert membership_id == db.get_membership_id(parent_object_name, child_object_name, collection)


# @pytest.mark.xfail(reason="Expected to fail until we fix the bug.")
def test_object_operations(db_instance_with_schema):
    db = db_instance_with_schema

    test_object_name = "TestGen"
    object_id = db.add_object(test_object_name, ClassEnum.Generator)
    assert object_id
    assert db.check_object_exists(test_object_name, ClassEnum.Generator)

    # Make sure we get the same object_id
    object_id_query = db.get_object_id(test_object_name, ClassEnum.Generator)
    assert object_id == object_id_query

    test_object_name = "TestGen2"
    test_object_category = "Thermal"
    object_id = db.add_object(test_object_name, ClassEnum.Generator, category=test_object_category)
    test_object = db.get_object_legacy(test_object_name, ClassEnum.Generator)
    assert test_object[0]["name"] == test_object_name

    # test_object_with_category = db.get_object_legacy(test_object_name, category="Thermal")
    # assert test_object == test_object_with_category

    test_object_name = "TestGen3"
    test_object_category = "Thermal"
    test_object_description = "Thermal Generator for area 1."
    object_id = db.add_object(
        test_object_name,
        ClassEnum.Generator,
        category=test_object_category,
        description=test_object_description,
    )
    test_object = db.get_object_legacy(test_object_name, ClassEnum.Generator)
    assert test_object[0]["name"] == test_object_name

    assert len(db.list_objects_by_class(ClassEnum.Generator)) == 3


def test_add_property_to_object(db_instance_with_schema):
    db = db_instance_with_schema
    test_object_name = "TestGen"
    test_property_name = "Max Capacity"
    test_property_value = 100.0
    _ = db.add_object(test_object_name, ClassEnum.Generator)
    data_id = db.add_property(
        test_object_name, test_property_name, test_property_value, object_class_enum=ClassEnum.Generator
    )
    object_properties = db.get_object_legacy(test_object_name, ClassEnum.Generator)
    assert object_properties
    assert object_properties[0]["name"] == test_object_name
    assert object_properties[0]["property"] == test_property_name
    assert object_properties[0]["property_value"] == test_property_value

    test_property_name = "Max Capacity"
    test_property_value = 100.0
    scenario = "TestScenario"
    data_id_scenario = db.add_property(
        test_object_name,
        test_property_name,
        test_property_value,
        object_class_enum=ClassEnum.Generator,
        scenario=scenario,
    )
    assert data_id != data_id_scenario
    object_properties_scenario = db.get_object_legacy(test_object_name, ClassEnum.Generator)
    assert object_properties
    assert len(object_properties_scenario) == 2  # 2 properties so far
    assert object_properties_scenario[0]["name"] == test_object_name
    assert object_properties_scenario[0]["property"] == test_property_name
    assert object_properties_scenario[0]["property_value"] == test_property_value
    assert object_properties_scenario[0]["scenario"] is None
    assert object_properties_scenario[1]["name"] == test_object_name
    assert object_properties_scenario[1]["property"] == test_property_name
    assert object_properties_scenario[1]["scenario"] == scenario

    test_object_name = "TestGen"
    test_property_name = "Max Energy"
    test_property_value = 200.0
    data_id = db.add_property(
        test_object_name, test_property_name, test_property_value, object_class_enum=ClassEnum.Generator
    )
    object_properties = db.get_object_legacy(
        test_object_name, ClassEnum.Generator, property_names="Max Energy"
    )
    assert len(object_properties) == 1  # Only one property asked
    object_properties = db.get_object_legacy(
        test_object_name, ClassEnum.Generator, property_names=["Max Energy", "Max Capacity"]
    )
    assert len(object_properties) == 3


def test_invalid_property(db_instance_with_schema):
    db = db_instance_with_schema
    test_object_name = "TestGen"
    test_property_name = "Wrong Property"
    test_property_value = 100.0
    _ = db.add_object(test_object_name, ClassEnum.Generator)
    with pytest.raises(PropertyNameError):
        _ = db.add_property(
            test_object_name, test_property_name, test_property_value, object_class_enum=ClassEnum.Generator
        )

    with pytest.raises(PropertyNameError):
        _ = db.get_object_legacy(test_object_name, ClassEnum.Generator, property_names=test_property_name)


def test_get_object_properties(db_instance_with_schema):
    db = db_instance_with_schema
    test_object_name = "TestGen"
    test_property_name = "Max Capacity"
    test_property_value = 100.0
    _ = db.add_object(test_object_name, ClassEnum.Generator)
    _ = db.add_property(
        test_object_name, test_property_name, test_property_value, object_class_enum=ClassEnum.Generator
    )
    object_properties = db.get_object_properties(test_object_name, ClassEnum.Generator)
    assert object_properties
    assert object_properties[0]["name"] == test_object_name
    assert object_properties[0]["property"] == test_property_name
    assert object_properties[0]["value"] == test_property_value
