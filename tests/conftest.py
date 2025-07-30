import shutil
import uuid
from collections.abc import Generator
from typing import Any

import pytest
from _pytest.logging import LogCaptureFixture
from loguru import logger

from plexosdb import PlexosDB
from plexosdb.db_manager import SQLiteManager

DATA_FOLDER = "tests/data"
DB_FILENAME = "plexosdb.xml"
TEST_SCHEMA = (
    "CREATE TABLE generators (id INTEGER PRIMARY KEY, name TEXT, capacity REAL, fuel_type TEXT);"
    "CREATE TABLE properties (id INTEGER PRIMARY KEY, generator_id INTEGER, property_name TEXT, "
    "value REAL, FOREIGN KEY(generator_id) REFERENCES generators(id));"
)


@pytest.fixture
def data_folder(pytestconfig):
    return pytestconfig.rootpath.joinpath(DATA_FOLDER)


@pytest.fixture
def caplog(caplog: LogCaptureFixture):
    handler_id = logger.add(
        caplog.handler,
        format="{message}",
        level=0,
        filter=lambda record: record["level"].no >= caplog.handler.level,
        enqueue=False,  # Set to 'True' if your test is spawning child processes.
    )
    yield caplog
    logger.remove(handler_id)


@pytest.fixture(scope="session")
def db_instance():
    """Create a base DB instance that lasts the entire test session."""
    db = PlexosDB()
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
            "INSERT INTO t_class(class_id, name, description) VALUES (5, 'DataFile', 'DataFile class')"
        )
        db._db.execute(
            "INSERT INTO t_class(class_id, name, description) VALUES (6, 'Storage', 'Storage class')"
        )
        db._db.execute(
            "INSERT INTO t_class(class_id, name, description) VALUES (7, 'Report', 'Report class')"
        )
        db._db.execute("INSERT INTO t_class(class_id, name, description) VALUES (8, 'Model', 'Model class')")
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
            "VALUES (4, 1, 4, 'Scenarios')"
        )
        db._db.execute(
            "INSERT INTO t_collection(collection_id, parent_class_id, child_class_id, name) "
            "VALUES (5, 1, 6, 'Storages')"
        )
        db._db.execute(
            "INSERT INTO t_collection(collection_id, parent_class_id, child_class_id, name) "
            "VALUES (6, 1, 8, 'Models')"
        )
        db._db.execute(
            "INSERT INTO t_collection(collection_id, parent_class_id, child_class_id, name) "
            "VALUES (7, 8, 7, 'Models')"
        )
        db._db.execute(
            "INSERT INTO t_collection(collection_id, parent_class_id, child_class_id, name) "
            "VALUES (8, 1, 7, 'Reports')"
        )
        db._db.execute("INSERT INTO t_unit(unit_id, value) VALUES (1,'MW')")
        db._db.execute("INSERT INTO t_unit(unit_id, value) VALUES (2,'MWh')")
        db._db.execute("INSERT INTO t_unit(unit_id, value) VALUES (3,'%')")
        db._db.execute(
            "INSERT INTO t_collection(collection_id, parent_class_id, child_class_id, name) "
            "VALUES (?, ?, ?, ?)",
            (9, 8, 4, "Scenarios"),
        )
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
        db._db.execute("INSERT INTO t_config(element, value) VALUES ('Version', '9.2')")
        db._db.execute("INSERT INTO t_attribute(attribute_id, class_id, name) VALUES( 1, 2, 'Latitude')")
        db._db.execute(
            "INSERT INTO t_property_report(property_id, collection_id, name) VALUES (1, 1, 'Units')"
        )
    yield db


@pytest.fixture(scope="function")
def db_manager_instance_empty_with_schema() -> Generator[SQLiteManager[Any], None, None]:
    db: PlexosDB = PlexosDB()
    db.create_schema()
    yield db._db
    db._db.close()


@pytest.fixture(scope="function")
def db_manager_instance_empty():
    """Create a fresh empty DB instance for each test."""
    # Create a completely fresh database for each test
    db = SQLiteManager()
    db.executescript(TEST_SCHEMA)
    yield db
    db.close()


@pytest.fixture(scope="function")
def db_manager_instance_populated():
    """Create a populated DB instance for testing queries."""
    # Create a fresh database and populate it
    db = SQLiteManager()
    db.executescript(TEST_SCHEMA)

    # Add generators
    db.execute(
        "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", ("Coal Plant 1", 500.0, "Coal")
    )
    db.execute(
        "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", ("Gas Plant 1", 300.0, "Gas")
    )
    db.execute(
        "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", ("Wind Farm 1", 150.0, "Wind")
    )

    # Add properties
    db.execute(
        "INSERT INTO properties (generator_id, property_name, value) VALUES (?, ?, ?)",
        (1, "Heat Rate", 9500.0),
    )
    db.execute(
        "INSERT INTO properties (generator_id, property_name, value) VALUES (?, ?, ?)",
        (1, "Variable Cost", 25.5),
    )
    db.execute(
        "INSERT INTO properties (generator_id, property_name, value) VALUES (?, ?, ?)",
        (2, "Heat Rate", 7200.0),
    )

    yield db
    db.close()


@pytest.fixture(scope="function")
def db_path_on_disk(tmp_path):
    """Create a temporary file path for on-disk database testing."""
    return tmp_path / "test_db.sqlite"
