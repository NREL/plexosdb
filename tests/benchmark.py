import pytest

from plexosdb.plexosdb import PlexosDB
from plexosdb.sqlite import PlexosSQLite

DB_FILENAME = "plexosdb.xml"


def plexosdb_execute_many_insert(xml_fname):
    db = PlexosDB.from_xml(xml_path=xml_fname)
    return db


def plexosdb_legacy_insert(xml_fname):
    db = PlexosSQLite(xml_fname=str(xml_fname))
    return db


@pytest.mark.parametrize(
    "load_function",
    [plexosdb_execute_many_insert, plexosdb_legacy_insert],
    ids=["executemany", "legacy-insert"],
)
def test_database_loading(benchmark, load_function, request):
    data_folder = request.getfixturevalue("data_folder")
    benchmark(load_function, xml_fname=data_folder)
