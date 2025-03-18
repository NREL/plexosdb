import pytest

from plexosdb.enums import ClassEnum
from plexosdb.plexosdb import PlexosDB

DB_FILENAME = "plexosdb.xml"


@pytest.fixture
def plexos_db(request):
    data_folder = request.getfixturevalue("data_folder")
    return PlexosDB.from_xml(xml_path=data_folder / DB_FILENAME)


def benchmark_get_object_legacy(db, object_name="SolarPV01", class_enum=ClassEnum.Generator):
    """Benchmark the legacy get_object method."""
    return db.get_object_legacy(object_name, class_enum)


def benchmark_get_object_properties(db, object_name="SolarPV01", class_enum=ClassEnum.Generator):
    """Benchmark the get_object_properties method."""
    return db.get_object_properties(object_name, class_enum)


@pytest.mark.parametrize(
    "benchmark_function",
    [benchmark_get_object_legacy, benchmark_get_object_properties],
    ids=["legacy-method", "properties-method"],
)
def test_get_object_methods(benchmark, benchmark_function, plexos_db):
    """Compare performance of different methods to get object properties."""
    benchmark(benchmark_function, plexos_db)


# Additional test with more parameters
@pytest.mark.parametrize(
    "benchmark_function",
    [benchmark_get_object_legacy, benchmark_get_object_properties],
    ids=["legacy-method", "properties-method"],
)
def test_get_generator_properties(benchmark, benchmark_function, plexos_db):
    """Compare performance when retrieving properties for a generator object."""
    # Find a generator object in the database
    generators = plexos_db.list_objects_by_class(ClassEnum.Generator)
    if generators:
        generator_name = generators[0]
        benchmark(benchmark_function, plexos_db, generator_name, ClassEnum.Generator)
    else:
        pytest.skip("No generators found in the test database")
