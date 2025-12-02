"""Util functions for plexosdb."""

from __future__ import annotations

import ast
from collections.abc import Iterable, Iterator
from importlib.resources import files
from itertools import islice
from typing import TYPE_CHECKING, Any

from loguru import logger

if TYPE_CHECKING:
    from plexosdb import ClassEnum, CollectionEnum, PlexosDB
    from plexosdb.db_manager import SQLiteManager


def batched(iterable: Iterable[Any], n: int) -> Iterator[tuple[Any, ...]]:
    """Implement batched iterator.

    https://docs.python.org/3/library/itertools.html#itertools.batched
    """
    it = iter(iterable)
    return iter(lambda: tuple(islice(it, n)), ())


def validate_string(value: str) -> Any:
    """Validate string and convert it to python object.

    This function also tries to parse floats or ints.

    Parameters
    ----------
    value: Any
        String value to be converted to Python Object

    Note
    ----
    The ast is slow due to the multiple cases. Use it only on simple for loops
    as this could become a bottleneck.
    """
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    if value == "true" or value == "TRUE":
        return True
    if value == "false" or value == "FALSE":
        return False
    try:
        value = ast.literal_eval(value)
    except:  # noqa: E722
        logger.trace("Could not parse {}", value)
    finally:
        return value


def no_space(a: str, b: str) -> int:
    """Collate function for catching strings with spaces."""
    if a.replace(" ", "") == b.replace(" ", ""):
        return 0
    if a.replace(" ", "") < b.replace(" ", ""):
        return -1
    return 1


def normalize_names(*args: str | Iterable[str]) -> list[str]:
    """Normalize a name or list of names into a unique list of strings.

    Parameters
    ----------
    names : str or Iterable[str]
        A string or an iterable of strings to normalize

    Returns
    -------
    list[str]
        A deduplicated list of the provided names

    Raises
    ------
    ValueError
        If the input is neither a string nor an iterable of strings
    """
    names: Iterable[Any]
    if len(args) == 1 and hasattr(args[0], "__iter__") and not isinstance(args[0], str):
        names = args[0]
    else:
        names = args
    return list(set(str(name) for name in names if name is not None))


def get_sql_query(query_name: str) -> str:
    """Load SQL query from package.

    Parameters
    ----------
    query_name : str
        Name of the query file to load from plexosdb.queries

    Returns
    -------
    str
        Content of the SQL query file as a string
    """
    fpath = files("plexosdb.queries").joinpath(query_name)
    return fpath.read_text(encoding="utf-8-sig")


def prepare_sql_data_params(
    records: list[dict[str, float]],
    memberships: list[dict[str, Any]],
    property_mapping: list[tuple[str, int]],
) -> list[tuple[int, int, Any]]:
    """Create list of tuples for data ingestion.

    Parameters
    ----------
    records : list[dict[str, float]]
        List of records where each record is a dictionary containing 'name'
        and property values
    memberships : list[dict[str, int]]
        List of membership dictionaries with 'name' and 'membership_id' keys
    property_mapping : list[tuple[str, int]]
        List of tuples mapping property names to property IDs

    Returns
    -------
    list[tuple[int, int, Any]]
        List of tuples containing (membership_id, property_id, value)
        for database insertion
    """
    property_id_map = {prop: pid for prop, pid in property_mapping}
    name_to_membership = {membership["name"]: membership["membership_id"] for membership in memberships}
    return [
        (name_to_membership[record["name"]], property_id_map[prop], value)
        for record in records
        if record["name"] in name_to_membership
        for prop, value in record.items()
        if prop != "name" and prop in property_id_map
    ]


def create_membership_record(
    object_ids: Iterable[int],
    child_object_class_id: int,
    parent_object_id: int,
    parent_object_class_id: int,
    collection_id: int,
) -> list[dict[str, int]]:
    """Create membership records for database insertion.

    Parameters
    ----------
    object_ids : Iterable[int]
        Iterable of child object IDs to create memberships for
    child_object_class_id : int
        Class ID for the child objects
    parent_object_id : int
        ID of the parent object
    parent_object_class_id : int
        Class ID for the parent object
    collection_id : int
        ID of the collection to which the membership belongs

    Returns
    -------
    list[dict[str, int]]
        List of dictionaries representing membership records ready
        for database insertion
    """
    return [
        {
            "parent_class_id": parent_object_class_id,
            "parent_object_id": parent_object_id,
            "collection_id": collection_id,
            "child_class_id": child_object_class_id,
            "child_object_id": object_id,
        }
        for object_id in object_ids
    ]


def prepare_properties_params(
    db: PlexosDB,
    records: list[dict[str, Any]],
    object_class: ClassEnum,
    collection: CollectionEnum,
    parent_class: ClassEnum,
) -> tuple[list[tuple[int, int, Any]], list[tuple[str, int]]]:
    """Prepare SQL parameters for property insertion.

    Parameters
    ----------
    db : PlexosDB
        Database instance
    records : list[dict]
        List of property records
    object_class : ClassEnum
        Class enumeration of the objects
    collection : CollectionEnum
        Collection enumeration for the properties
    parent_class : ClassEnum
        Parent class enumeration

    Returns
    -------
    tuple[list[tuple], list]
        Tuple of (params, collection_properties)
    """
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
    return params, collection_properties


def insert_property_data(
    db: PlexosDB, params: list[tuple[int, int, Any]]
) -> dict[tuple[int, int, Any], tuple[int, str]]:
    """Insert property data and return mapping of data IDs to object names.

    Parameters
    ----------
    db : PlexosDB
        Database instance
    params : list[tuple]
        List of (membership_id, property_id, value) tuples

    Returns
    -------
    dict
        Mapping of (membership_id, property_id, value) to (data_id, obj_name)
    """
    filter_property_ids = [d[1] for d in params]
    for property_id in filter_property_ids:
        db._db.execute("UPDATE t_property set is_dynamic=1 where property_id = ?", (property_id,))
        db._db.execute("UPDATE t_property set is_enabled=1 where property_id = ?", (property_id,))

    db._db.executemany("INSERT into t_data(membership_id, property_id, value) values (?,?,?)", params)

    data_ids_query = """
        SELECT d.data_id, o.name
        FROM t_data d
        JOIN t_membership m ON d.membership_id = m.membership_id
        JOIN t_object o ON m.child_object_id = o.object_id
        WHERE d.membership_id = ? AND d.property_id = ? AND d.value = ?
    """
    data_id_map = {}
    for membership_id, property_id, value in params:
        result = db._db.fetchone(data_ids_query, (membership_id, property_id, value))
        if result:
            data_id_map[(membership_id, property_id, value)] = (result[0], result[1])
    return data_id_map


def insert_scenario_tags(
    db: PlexosDB, scenario: str, params: list[tuple[int, int, Any]], chunksize: int
) -> None:
    """Insert scenario tags for property data.

    Parameters
    ----------
    db : PlexosDB
        Database instance
    scenario : str
        Scenario name
    params : list[tuple]
        List of (membership_id, property_id, value) tuples
    chunksize : int
        Number of records to process in each batch
    """
    if scenario is None:
        return

    if not db.check_scenario_exists(scenario):
        scenario_id = db.add_scenario(scenario)
    else:
        scenario_id = db.get_scenario_id(scenario)

    for batch in batched(params, chunksize):
        batched_list = list(batch)
        scenario_query = f"""
            INSERT into t_tag(data_id, object_id)
            SELECT
                d.data_id as data_id,
                {scenario_id} as object_id
            FROM
              t_data d
            WHERE d.membership_id = ? AND d.property_id = ? AND d.value = ?
        """
        db._db.executemany(scenario_query, batched_list)


def add_texts_for_properties(
    db: PlexosDB,
    params: list[tuple[int, int, Any]],
    data_id_map: dict[tuple[int, int, Any], tuple[int, str]],
    records: list[dict[str, Any]],
    field_name: str,
    text_class: ClassEnum,
) -> None:
    """Add text data for properties from specified field.

    Parameters
    ----------
    db : PlexosDB
        Database instance
    params : list[tuple]
        List of (membership_id, property_id, value) tuples
    data_id_map : dict
        Mapping of (membership_id, property_id, value) to (data_id, obj_name)
    records : list[dict]
        Original records containing the text field
    field_name : str
        Name of the field in records containing text data
    text_class : ClassEnum
        ClassEnum for the text data
    """
    text_map = {rec["name"]: rec[field_name] for rec in records if field_name in rec}
    for membership_id, property_id, value in params:
        data_id, obj_name = data_id_map.get((membership_id, property_id, value), (None, None))
        if data_id and obj_name and obj_name in text_map:
            db.add_text(text_class, text_map[obj_name], data_id)


def build_data_id_map(
    db: SQLiteManager, params: list[tuple[int, int, Any]]
) -> dict[tuple[int, int, Any], tuple[int, str]]:
    """Build mapping of (membership_id, property_id, value) to (data_id, obj_name).

    Parameters
    ----------
    db_manager : DBManager
        Database manager instance for executing queries
    params : list[tuple]
        List of (membership_id, property_id, value) tuples

    Returns
    -------
    dict
        Mapping of (membership_id, property_id, value) to (data_id, obj_name)
    """
    data_ids_query = """
        SELECT d.data_id, o.name
        FROM t_data d
        JOIN t_membership m ON d.membership_id = m.membership_id
        JOIN t_object o ON m.child_object_id = o.object_id
        WHERE d.membership_id = ? AND d.property_id = ? AND d.value = ?
    """
    data_id_map = {}
    for membership_id, property_id, value in params:
        result = db.fetchone(data_ids_query, (membership_id, property_id, value))
        if result:
            data_id_map[(membership_id, property_id, value)] = (result[0], result[1])
    return data_id_map


def get_scenario_id(db: PlexosDB, scenario: str) -> int:
    """Get or create scenario ID.

    Parameters
    ----------
    db_manager : DBManager
        Database manager instance
    scenario : str
        Scenario name

    Returns
    -------
    int
        Scenario object ID
    """
    if not db.check_scenario_exists(scenario):
        return db.add_scenario(scenario)
    return db.get_scenario_id(scenario)
