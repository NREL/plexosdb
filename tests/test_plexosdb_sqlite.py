import pytest
import xml.etree.ElementTree as ET  # noqa: N817
from plexosdb.enums import ClassEnum, CollectionEnum, Schema
from plexosdb.sqlite import PlexosSQLite

DB_FILENAME = "plexosdb.xml"


@pytest.mark.skip(reason="Requires master file")
def db_empty() -> "PlexosSQLite":
    return PlexosSQLite()


@pytest.fixture
def db(data_folder) -> "PlexosSQLite":
    return PlexosSQLite(xml_fname=data_folder.joinpath(DB_FILENAME))


def test_database_initialization(db):
    assert isinstance(db, PlexosSQLite)
    assert db.DB_FILENAME == "plexos.db"
    assert db._conn


@pytest.mark.skip(reason="Requires master file")
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
def test_create_table_schema(db_empty, table_name):
    with db_empty._conn as conn:
        result = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    assert result.fetchone() is not None


@pytest.mark.get_functions
def test_check_id_exists(db):
    # Check that system exists
    system_check = db.check_id_exists(Schema.Class, "System")
    assert isinstance(system_check, bool)
    assert system_check

    # Return false if something does not exists
    system_check = db.check_id_exists(Schema.Class, "NotExistingObject", class_name=ClassEnum.System)
    assert isinstance(system_check, bool)
    assert not system_check

    # Check that returns ValueError if multiple object founds
    with pytest.raises(ValueError):
        _ = db.check_id_exists(Schema.Objects, "SolarPV01", class_name=ClassEnum.Generator)


@pytest.mark.get_functions
def test_get_id(db):
    system_id = db._get_id(Schema.Class, "System")
    assert isinstance(system_id, int)
    assert system_id == 1  # System always have id 1

    system_id = db._get_id(Schema.Class, "System", class_name=ClassEnum.System)
    assert isinstance(system_id, int)
    assert system_id == 1  # System always have id 1

    # Test return none
    with pytest.raises(KeyError):
        _ = db._get_id(Schema.Class, "NotexistingObject")

    # Test using collection id
    # collection_id = db._get_id(Schema.Collection, "Generators", collection_name=CollectionEnum.Generators)


@pytest.mark.get_functions
def test_get_collection_id(db):
    collection_name = CollectionEnum.Generators
    parent_class = ClassEnum.System
    collection_id = db.get_collection_id(collection_name, parent_class=parent_class)

    collection_query = f"""
    SELECT
        collection_id
    FROM
        t_collection as collection
    LEFT JOIN
        t_class AS parent_class ON parent_class.class_id = collection.parent_class_id
    WHERE
        collection.name = '{collection_name}'
    AND
        parent_class.name = '{parent_class}'
    """

    collection_id_query = db.query(collection_query)[0][0]
    assert collection_id == collection_id_query

    collection_name = CollectionEnum.Generators
    parent_class = ClassEnum.Emission
    child_class = ClassEnum.Generator
    collection_id = db.get_collection_id(collection_name, parent_class=parent_class, child_class=child_class)

    collection_query = f"""
    SELECT
        collection_id
    FROM
        t_collection as collection
    LEFT JOIN
        t_class AS parent_class ON parent_class.class_id = collection.parent_class_id
    LEFT JOIN
        t_class AS child_class ON child_class.class_id = collection.child_class_id
    WHERE
        collection.name = '{collection_name}'
    AND
        parent_class.name = '{parent_class}'
    AND
        child_class.name = '{child_class}'
    """
    collection_id_query = db.query(collection_query)[0][0]
    assert collection_id == collection_id_query

    # Assert that return of multiple collections
    with pytest.raises(ValueError):
        _ = db.get_collection_id(CollectionEnum.Generators)


@pytest.mark.get_functions
def test_get_object_id(db):
    gen_01_name = "gen1"
    gen_id = db.add_object(
        gen_01_name, ClassEnum.Generator, CollectionEnum.Generators, description="Test Gen"
    )
    assert gen_id

    gen_id_get = db.get_object_id(gen_01_name, class_name=ClassEnum.Generator)
    assert gen_id == gen_id_get

    # Add generator with same name different category
    gen_01_name = "gen1"
    category_name = "PV Gens"
    gen_id = db.add_object(
        gen_01_name,
        ClassEnum.Generator,
        CollectionEnum.Generators,
        description="Test Gen",
        category_name=category_name,
    )
    with pytest.raises(ValueError):
        _ = db.get_object_id(gen_01_name, class_name=ClassEnum.Generator)

    # Now actually filter by category
    object_id = db.get_object_id(gen_01_name, class_name=ClassEnum.Generator, category_name=category_name)
    assert object_id
    assert gen_id == object_id


@pytest.mark.get_functions
def test_get_memberships(db):
    # Test Node
    node_name = "Node 1"
    node_id = db.add_object(node_name, ClassEnum.Node, CollectionEnum.Nodes, description="Test Node")
    assert node_id

    # Test generator
    gen_01_name = "gen1"
    gen_id = db.add_object(
        gen_01_name, ClassEnum.Generator, CollectionEnum.Generators, description="Test Gen"
    )
    assert gen_id

    # Add membership to node_id
    db.add_membership(
        gen_01_name,
        node_name,
        parent_class=ClassEnum.Generator,
        child_class=ClassEnum.Node,
        collection=CollectionEnum.Nodes,
    )

    memberships = db.get_memberships(gen_01_name, object_class=ClassEnum.Generator)

    assert memberships
    assert memberships[0][2] == gen_01_name
    assert memberships[0][3] == node_name

    # Check that if not a single generator is passed it breaks
    with pytest.raises(KeyError):
        _ = db.get_memberships("FakeGen", object_class=ClassEnum.Generator)

    # Add a second generator
    gen_02_name = "gen2"
    gen_id = db.add_object(
        gen_02_name, ClassEnum.Generator, CollectionEnum.Generators, description="Test Gen2"
    )
    membership_id = db.add_membership(
        gen_02_name,
        node_name,
        parent_class=ClassEnum.Generator,
        child_class=ClassEnum.Node,
        collection=CollectionEnum.Nodes,
    )

    membership_id_check = db.get_membership_id(
        child_name=node_name,
        parent_name=gen_02_name,
        child_class=ClassEnum.Node,
        parent_class=ClassEnum.Generator,
        collection=CollectionEnum.Nodes,
    )

    assert membership_id_check == membership_id

    # Get membership for both generators
    memberships = db.get_memberships(gen_01_name, gen_02_name, object_class=ClassEnum.Generator)

    assert memberships
    assert len(memberships) == 2
    assert memberships[0][2] == gen_01_name
    assert memberships[0][3] == node_name
    assert memberships[1][2] == gen_02_name
    assert memberships[1][3] == node_name

    # Test KeyError from get_membership_id w/o membership
    gen_03_name = "gen3"
    gen_id = db.add_object(
        gen_03_name, ClassEnum.Generator, CollectionEnum.Generators, description="Test Gen3"
    )
    with pytest.raises(KeyError):
        _ = db.get_membership_id(
            child_name=node_name,
            parent_name=gen_03_name,
            child_class=ClassEnum.Node,
            parent_class=ClassEnum.Generator,
            collection=CollectionEnum.Nodes,
        )


@pytest.mark.get_functions
def test_get_property_id(db):
    property = "F Price"
    collection = CollectionEnum.Generators
    collection = CollectionEnum.Generators
    parent_class = ClassEnum.System
    child_class = ClassEnum.Generator

    # Test wrong property on get_property
    with pytest.raises(KeyError):
        _ = db.get_property_id(
            property, collection=collection, parent_class=parent_class, child_class=child_class
        )


@pytest.mark.add_functions
def test_add_category(db):
    new_category = "new_generator_category"
    category_id = db.add_category(new_category, class_name=ClassEnum.Generator)
    assert category_id
    result = db.query(
        "SELECT category_id, name, class_id, rank FROM t_category WHERE name = ?", (new_category,)
    )
    assert len(result) == 1  # Assert only one row returns
    result_tuple = result[0]
    assert result_tuple[0] == category_id
    assert result_tuple[1] == new_category

    new_category_2 = "new_generator_category_2"
    category_id = db.add_category(new_category_2, class_name=ClassEnum.Generator)
    assert category_id
    result = db.query(
        "SELECT category_id, name, class_id, rank FROM t_category WHERE name = ?", (new_category_2,)
    )
    assert len(result) == 1  # Two categories
    result_tuple = result[0]
    assert result_tuple[0] == category_id
    assert result_tuple[1] == new_category_2

    # Testing adding same name of category
    category_id = db.add_category(new_category, class_name=ClassEnum.Generator)
    assert category_id
    result = db.query(
        "SELECT category_id, name, class_id, rank FROM t_category WHERE name = ?", (new_category,)
    )
    assert len(result) == 2  # Two categories
    result_tuple_1 = result[0]
    result_tuple_2 = result[1]
    assert result_tuple_1[3] != result_tuple_2[3]

    # Test add categorry on wrong


@pytest.mark.add_functions
def test_add_object(db):
    # Insert simple object
    object_name = "Model_2012"
    object_description = "This is an awesome model"
    object_id = db.add_object(
        object_name, ClassEnum.Model, CollectionEnum.Models, description=object_description
    )
    assert object_id
    assert isinstance(object_id, int)
    assert db.check_id_exists(Schema.Objects, object_name, class_name=ClassEnum.Model)

    # Assert that object inserted is the same
    object_query = """
    SELECT
        object_id,
        name,
        class_id,
        description
    FROM
        t_object
    WHERE
        name = ?
    """
    result = db.query(object_query, (object_name,))[0]
    assert result[0] == object_id
    assert result[1] == object_name
    assert result[3] == object_description

    # Assert that object has the system membership id
    membership_query = """
    SELECT
        parent_class_id,
        child_class_id,
        parent_object_id,
        child_object_id,
        collection_id
    FROM
        t_membership
    WHERE
        child_object_id = ?
    """

    memberships = db.query(
        membership_query,
        (object_id,),
    )
    assert len(memberships) == 1
    membership_tuple = memberships[0]
    system_class_id = db._get_id(Schema.Class, "System", class_name=ClassEnum.System)
    system_object_id = db.get_object_id("System", class_name=ClassEnum.System)
    class_id = db._get_id(Schema.Class, "Model")
    model_collection_id = db.get_collection_id(CollectionEnum.Models, parent_class=ClassEnum.System)
    assert membership_tuple[0] == system_class_id
    assert membership_tuple[1] == class_id
    assert membership_tuple[2] == system_object_id
    assert membership_tuple[3] == object_id
    assert membership_tuple[4] == model_collection_id


@pytest.mark.add_functions
def test_add_atribute(db):
    # Raise error if object does not exists
    with pytest.raises(KeyError):
        _ = db.add_attribute(
            object_name="Test",
            object_class=ClassEnum.Model,
            attribute_class=ClassEnum.Model,
            attribute_name="Random Number Seed",
            attribute_value=1,
        )

    object_name = "Model_2012"
    object_description = "This is an awesome model"
    if not db.check_id_exists(Schema.Objects, object_name):
        object_id = db.add_object(
            object_name, ClassEnum.Model, CollectionEnum.Models, description=object_description
        )

    attribute_value = 1
    attribute_name = "Random Number Seed"
    object_class = ClassEnum.Model
    attribute_class = ClassEnum.Model
    attribute_id = db._get_id(Schema.Attributes, attribute_name, class_name=attribute_class)
    object_id = db.get_object_id(object_name, class_name=object_class)
    _ = db.add_attribute(
        object_name=object_name,
        object_class=ClassEnum.Model,
        attribute_class=ClassEnum.Model,
        attribute_name="Random Number Seed",
        attribute_value=attribute_value,
    )
    attribute = db.query(
        "SELECT object_id, attribute_id FROM t_attribute_data where object_id = ?", (object_id,)
    )
    assert len(attribute) == 1
    attribute_tuple = attribute[0]
    assert attribute_tuple[0] == object_id
    assert attribute_tuple[1] == attribute_id


@pytest.mark.add_functions
def test_add_property(db):
    object_name = "generator"
    object_description = "Awesome generator"
    object_id = db.add_object(
        object_name, ClassEnum.Generator, CollectionEnum.Generators, description=object_description
    )
    assert object_id
    assert isinstance(object_id, int)

    property = "Max Capacity"
    collection = CollectionEnum.Generators
    parent_class = ClassEnum.System
    child_class = ClassEnum.Generator
    property_id = db.get_property_id(
        property,
        collection=collection,
        parent_class=parent_class,
        child_class=child_class,
    )
    value = 100
    data_id = db.add_property(
        object_name,
        property,
        value,
        object_class=ClassEnum.Generator,
        parent_class=parent_class,
        collection=collection,
    )
    result = db.query(
        "SELECT data_id, membership_id, property_id, value from t_data where data_id = ?", (data_id,)
    )
    assert result
    result_tuple = result[0]
    assert result_tuple[0] == data_id
    assert result_tuple[2] == property_id
    assert result_tuple[3] == value

    property = "Fuel Price"
    collection = CollectionEnum.Generators
    parent_class = ClassEnum.System
    child_class = ClassEnum.Generator
    property_id = db.get_property_id(
        property, collection=collection, parent_class=parent_class, child_class=child_class
    )
    value = 100

    # Test data files
    data_id = db.add_property(
        object_name,
        property,
        value,
        object_class=ClassEnum.Generator,
        parent_class=ClassEnum.System,
        collection=collection,
        text={"Data File": "test.csv"},
    )
    assert data_id

    # Test Scenarios
    scenario = "Awesome Scenario"
    property = "Fuel Price"
    collection = CollectionEnum.Generators
    parent_class = ClassEnum.System
    child_class = ClassEnum.Generator
    property_id = db.get_property_id(
        property, collection=collection, parent_class=parent_class, child_class=child_class
    )
    value = 100

    data_id = db.add_property(
        object_name,
        property,
        value,
        object_class=ClassEnum.Generator,
        parent_class=ClassEnum.System,
        collection=collection,
        text={"Data File": "test.csv"},
        scenario=scenario,
    )
    assert data_id

    # Test wrong property on add_property
    property = "F Price"
    collection = CollectionEnum.Generators
    collection = CollectionEnum.Generators
    parent_class = ClassEnum.System
    child_class = ClassEnum.Generator
    with pytest.raises(KeyError):
        _ = db.add_property(
            object_name,
            property,
            value,
            object_class=ClassEnum.Generator,
            parent_class=ClassEnum.System,
            collection=collection,
            text={"Data File": "test.csv"},
            scenario=scenario,
        )


@pytest.mark.add_functions
def test_add_report(db):
    input_report = {
        "child_class": ClassEnum.Generator,
        "collection": CollectionEnum.Generators,
        "object_name": "base_report",
        "parent_class": ClassEnum.System,
        "phase_id": 4,
        "property": "Generation",
    }

    # Check that we raise the warning if you try to add a report without a object on the system.
    with pytest.raises(KeyError):
        db.add_report(**input_report)

    object_id = db.add_object("base_report", ClassEnum.Report, CollectionEnum.Reports)
    db.add_report(**input_report)

    result = db.query("SELECT object_id from t_report where object_id = ?", (object_id,))
    assert result
    assert len(result) == 1
    assert result[0][0] == object_id

    # Test that raises an error when the property is wrong.
    with pytest.raises(KeyError):
        input_report["property"] = "Wrong Property Name"
        db.add_report(**input_report)


@pytest.mark.add_functions
def test_add_property_from_records(db):
    records = [
        {"name": "gen1", "Max Capacity": 100},
        {"name": "gen2", "Max Capacity": 200},
        {"name": "gen3", "Max Capacity": 300},
    ]

    # Asser that we can not add properties for non-existant objects
    with pytest.raises(KeyError):
        db.add_property_from_records(
            records,
            parent_class=ClassEnum.System,
            collection=CollectionEnum.Generators,
            scenario="Test",
        )

    for record in records:
        _ = db.add_object(record["name"], ClassEnum.Generator, CollectionEnum.Generators)
    db.add_property_from_records(
        records,
        parent_class=ClassEnum.System,
        collection=CollectionEnum.Generators,
        scenario="Test",
    )


def test_create_table_element(db):
    # Example input data
    root = ET.Element("root")
    column_types = {"id": "INT", "name": "VARCHAR", "active": "BIT"}
    table_name = "table"
    rows = [(1, "John", 1), (2, "Doe", 0)]

    # Call the function
    db._create_table_element(root, column_types, table_name, rows)

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


def test_to_xml(db, tmp_path):
    fname = "testing"
    fpath = tmp_path / fname
    db.to_xml(fpath=fpath)
    assert fpath.exists()


def test_save(db, tmp_path):
    fname = "testing"
    fpath = tmp_path / fname
    db.save(fpath=fpath)
    assert fpath.exists()
