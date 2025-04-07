"""Util functions for plexosdb."""

import ast
from collections.abc import Iterable
from importlib.resources import files
from itertools import islice
from typing import Any

from loguru import logger


def batched(iterable, n):
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


def normalize_names(*args) -> list[str]:
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
    if len(args) == 1 and hasattr(args[0], "__iter__") and not isinstance(args[0], str):
        names = args[0]
    else:
        names = args
    return list(set(str(name) for name in names if name is not None))


def get_sql_query(query_name: str):
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
