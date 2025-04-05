import shutil
import uuid

import pytest
from _pytest.logging import LogCaptureFixture
from loguru import logger

from plexosdb import PlexosDB

DATA_FOLDER = "tests/data"
DB_FILENAME = "plexosdb.xml"


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
        db._db.execute("INSERT INTO t_config(element, value) VALUES ('Version', '9.2')")
    yield db
