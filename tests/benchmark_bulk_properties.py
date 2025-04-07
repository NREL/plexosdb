import random

import pytest

from plexosdb.db import PlexosDB
from plexosdb.enums import ClassEnum, CollectionEnum
from plexosdb.utils import batched, prepare_sql_data_params

DB_FILENAME = "plexosdb.xml"


def setup_system(db, generators: int = 500):
    for _ in range(generators):
        db.add_object(ClassEnum.Generator, name=f"Generator_{_}")

    records = [
        {
            "name": f"Generator_{_}",
            "Max Capacity": random.random(),
            "Max Energy": random.random(),
        }
        for _ in range(generators)
    ]
    return db, records


def add_from_records(db: PlexosDB, records):
    db.add_properties_from_records(
        records, object_class=ClassEnum.Generator, collection=CollectionEnum.Generators, scenario="Test"
    )
    return db


def add_from_records_legacy(db: PlexosDB, records, chunksize=1_000):
    object_class = ClassEnum.Generator
    collection = CollectionEnum.Generators
    parent_class = ClassEnum.System
    scenario = None
    collection_id = db.get_collection_id(
        collection, parent_class_enum=parent_class, child_class_enum=object_class
    )
    collection_properties = db.query(
        f"select name, property_id from t_property where collection_id={collection_id}"
    )
    component_names = tuple(d["name"] for d in records)
    memberships = db.get_memberships_system(component_names, object_class=object_class)

    if not memberships:
        raise KeyError(
            "Object do not exists on the database yet. "
            "Make sure you use `add_object` before adding properties."
        )

    params = prepare_sql_data_params(records, memberships=memberships, property_mapping=collection_properties)
    # Make properties dynamic on plexos
    with db._db.transaction():
        filter_property_ids = [d[1] for d in params]
        for property_id in filter_property_ids:
            db._db.execute("UPDATE t_property set is_dynamic=1 where property_id = ?", (property_id,))
            db._db.execute("UPDATE t_property set is_enabled=1 where property_id = ?", (property_id,))

    with db._db.transaction():
        db._db.executemany("INSERT into t_data(membership_id, property_id, value) values (?,?,?)", params)

    if scenario is not None and (scenario_id := db.get_scenario_id(scenario)):
        for batch in batched(params, chunksize):
            place_holders = ", ".join(["(?, ?, ?)"] * len(batch))
            scenario_query = f"""
                INSERT into t_tag(data_id, object_id)
                SELECT
                    data_id as data_id,
                    {scenario_id} as object_id
                FROM
                  t_data
                WHERE (membership_id, property_id, value) in ({place_holders});
            """
            params = [data for row in batch for data in row]
            db._db.execute(scenario_query, params=params)
        return


@pytest.mark.parametrize(
    "ingest_function",
    [add_from_records, add_from_records_legacy],
    ids=["original", "optimized"],
)
@pytest.mark.parametrize("system_size", [10, 1000, 10_000], ids=["small", "medium", "large"])
def test_database_loading(benchmark, ingest_function, system_size, request):
    db = request.getfixturevalue("db_instance_with_schema")

    # Create a setup function that returns the arguments for ingest_function
    def setup_with_size():
        return setup_system(db, generators=system_size)

    # Create a wrapper function that unpacks the tuple to positional arguments
    def wrapper():
        db_instance, records_list = setup_with_size()
        return ingest_function(db_instance, records_list)

    benchmark.pedantic(wrapper, setup=lambda: None)
