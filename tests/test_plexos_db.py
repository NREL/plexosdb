import pytest
import sqlite3
import xml.etree.ElementTree as ET  # noqa: N817
from plexosdb.enums import ClassEnum, CollectionEnum, Schema
from plexosdb.sqlite import PlexosSQLite


@pytest.fixture(scope="module")
def sqlite_database() -> "PlexosSQLite":
    return PlexosSQLite()


def test_database_initialization():
    db = PlexosSQLite()

    assert isinstance(db, PlexosSQLite)
    assert db.DB_FILENAME == "plexos.db"
    assert db._conn


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
def test_create_table_schema(sqlite_database, table_name):
    with sqlite_database._conn as conn:
        result = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    assert result.fetchone() is not None


def test_get_id(sqlite_database):
    system_id = sqlite_database.get_id(Schema.Class, "System", class_id=ClassEnum.System)
    assert isinstance(system_id, int)
    assert system_id == 1  # System always have id 1

    # Test return none
    system_id = sqlite_database.get_id(Schema.Class, "NotexistingObject", class_id=ClassEnum.System)
    assert system_id is None


def test_add_category(sqlite_database):
    new_category = "new_generator_category"
    category_id = sqlite_database.add_category(new_category, class_id=ClassEnum.Generator)
    assert category_id
    result = sqlite_database.query(
        "SELECT category_id, name, class_id, rank from t_category where name = ?", (new_category,)
    )
    assert len(result) == 1  # Assert only one row returns
    result_tuple = result[0]
    assert result_tuple[0] == category_id
    assert result_tuple[1] == new_category

    new_category_2 = "new_generator_category_2"
    category_id = sqlite_database.add_category(new_category_2, class_id=ClassEnum.Generator)
    assert category_id
    result = sqlite_database.query(
        "SELECT category_id, name, class_id, rank from t_category where name = ?", (new_category_2,)
    )
    assert len(result) == 1  # Two categories
    result_tuple = result[0]
    assert result_tuple[0] == category_id
    assert result_tuple[1] == new_category_2

    # Testing adding same name of category
    category_id = sqlite_database.add_category(new_category, class_id=ClassEnum.Generator)
    assert category_id
    result = sqlite_database.query(
        "SELECT category_id, name, class_id, rank from t_category where name = ?", (new_category,)
    )
    assert len(result) == 2  # Two categories
    result_tuple_1 = result[0]
    result_tuple_2 = result[1]
    assert result_tuple_1[3] != result_tuple_2[3]


def test_add_object(sqlite_database):
    # Insert simple object
    object_name = "Model_2012"
    object_description = "This is an awesome model"
    object_id = sqlite_database.add_object(
        object_name, ClassEnum.Model, CollectionEnum.SystemModel, description=object_description
    )
    assert object_id
    assert isinstance(object_id, int)

    # Assert that object inserted is the same
    result = sqlite_database.query(
        "SELECT object_id, name, class_id, description from t_object where name = ?", (object_name,)
    )[0]
    assert result[0] == object_id
    assert result[1] == object_name
    assert result[2] == ClassEnum.Model
    assert result[3] == object_description

    # Assert hat object has the system membership id
    memberships = sqlite_database.query(
        """SELECT
            parent_class_id,
            child_class_id,
            parent_object_id,
            child_object_id,
            collection_id
            FROM t_membership
            WHERE child_object_id = ?
        """,
        (object_id,),
    )
    assert len(memberships) == 1
    membership_tuple = memberships[0]
    system_class_id = sqlite_database.get_id(Schema.Class, "System", class_id=ClassEnum.System)
    assert membership_tuple[0] == ClassEnum.System
    assert membership_tuple[1] == ClassEnum.Model
    assert membership_tuple[2] == system_class_id
    assert membership_tuple[3] == object_id
    assert membership_tuple[4] == int(CollectionEnum.SystemModel.value)


def test_add_atribute(sqlite_database):
    # Raise error if object does not exists
    with pytest.raises(sqlite3.IntegrityError):
        _ = sqlite_database.add_attribute(
            object_name="Test",
            object_class=ClassEnum.Model,
            attribute_class=ClassEnum.Model,
            attribute_name="Random Number Seed",
            attribute_value=1,
        )

    object_name = "Model_2012"

    attribute_value = 1
    attribute_name = "Random Number Seed"
    object_class = ClassEnum.Model
    attribute_class = ClassEnum.Model
    attribute_id = sqlite_database.get_id(Schema.Attributes, attribute_name, class_id=attribute_class)
    object_id = sqlite_database.get_id(Schema.Objects, object_name, class_id=object_class)
    _ = sqlite_database.add_attribute(
        object_name=object_name,
        object_class=ClassEnum.Model,
        attribute_class=ClassEnum.Model,
        attribute_name="Random Number Seed",
        attribute_value=attribute_value,
    )
    attribute = sqlite_database.query(
        "SELECT object_id, attribute_id FROM t_attribute_data where object_id = ?", (object_id,)
    )
    assert len(attribute) == 1
    attribute_tuple = attribute[0]
    assert attribute_tuple[0] == object_id
    assert attribute_tuple[1] == attribute_id


def test_add_property(sqlite_database):
    object_name = "generator"
    object_description = "Awesome gnerator"
    object_id = sqlite_database.add_object(
        object_name, ClassEnum.Generator, CollectionEnum.SystemGenerators, description=object_description
    )
    assert object_id
    assert isinstance(object_id, int)

    property = "Max Capacity"
    collection = CollectionEnum.SystemGenerators
    property_id = sqlite_database.get_id(Schema.Property, property, collection_id=collection)
    value = 100
    data_id = sqlite_database.add_property(
        object_name,
        property,
        value,
        object_class=ClassEnum.Generator,
        collection=collection,
    )
    result = sqlite_database.query(
        "SELECT data_id, membership_id, property_id, value from t_data where data_id = ?", (data_id,)
    )
    assert result
    result_tuple = result[0]
    assert result_tuple[0] == data_id
    assert result_tuple[2] == property_id
    assert result_tuple[3] == value

    property = "Fuel Price"
    collection = CollectionEnum.SystemGenerators
    property_id = sqlite_database.get_id(Schema.Property, property, collection_id=collection)
    value = 100

    # Test data files
    data_id = sqlite_database.add_property(
        object_name,
        property,
        value,
        object_class=ClassEnum.Generator,
        collection=collection,
        text={"Data File": "test.csv"},
    )


def test_add_report(caplog, sqlite_database):
    input_report = {
        "child_class": "Generator",
        "collection": "Generators",
        "object": "base_report",
        "parent_class": "System",
        "phase_id": 4,
        "property": "Generation",
    }

    # Check that we raise the warning if you try to add a report withouth aan object on the system.
    sqlite_database.add_report(**input_report)
    assert "WARNING" in caplog.text

    object_id = sqlite_database.add_object("base_report", ClassEnum.Report, CollectionEnum.SystemReport)
    sqlite_database.add_report(**input_report)

    result = sqlite_database.query("SELECT object_id from t_report where object_id = ?", (object_id,))
    assert result
    assert len(result) == 1
    assert result[0][0] == object_id

    # Test that raises an error when the property is wrong.
    with pytest.raises(KeyError):
        input_report["property"] = "Wrong Property Name"
        sqlite_database.add_report(**input_report)


def test_add_property_from_records(sqlite_database):
    records = [
        {"name": "gen1", "Max Capacity": 100},
        {"name": "gen2", "Max Capacity": 200},
        {"name": "gen3", "Max Capacity": 300},
    ]

    # Asser that we can not add properties for non-existant objects
    with pytest.raises(KeyError):
        sqlite_database.add_property_from_records(
            records,
            parent_class=ClassEnum.System,
            collection=CollectionEnum.SystemGenerators,
            scenario="Test",
        )

    for record in records:
        _ = sqlite_database.add_object(record["name"], ClassEnum.Generator, CollectionEnum.SystemGenerators)
    sqlite_database.add_property_from_records(
        records,
        parent_class=ClassEnum.System,
        collection=CollectionEnum.SystemGenerators,
        scenario="Test",
    )


def test_create_table_element(sqlite_database):
    # Example input data
    root = ET.Element("root")
    column_types = {"id": "INT", "name": "VARCHAR", "active": "BIT"}
    table_name = "table"
    rows = [(1, "John", 1), (2, "Doe", 0)]

    # Call the function
    sqlite_database._create_table_element(root, column_types, table_name, rows)

    # Assert the generated XML structure
    assert len(root) == 2  # Assuming 2 rows are added
    for i, row in enumerate(rows):
        row_element = root[i]
        assert row_element.tag == table_name
        assert len(row_element) == len(column_types)
        for (column_name, column_type), column_value in zip(column_types.items(), row):
            column_element = row_element.find(column_name)
            if column_element is not None:
                if column_type == "BIT":
                    if column_value == 1:
                        assert column_element.text == "true"
                    elif column_value == 0:
                        assert column_element.text == "false"
                else:
                    assert column_element.text == str(column_value)


def test_to_xml(sqlite_database, tmp_path):
    fname = "testing"
    fpath = tmp_path / fname
    sqlite_database.to_xml(fpath=fpath)
    assert fpath.exists()
