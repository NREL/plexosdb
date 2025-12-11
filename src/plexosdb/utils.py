"""Util functions for plexosdb."""

from __future__ import annotations

import ast
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from importlib.resources import files
from itertools import islice
from typing import TYPE_CHECKING, Any

from loguru import logger

from .exceptions import NotFoundError

if TYPE_CHECKING:
    from plexosdb import ClassEnum, CollectionEnum, PlexosDB
    from plexosdb.db_manager import SQLiteManager


@dataclass
class PreparedPropertiesResult:
    """Prepared inputs for bulk property insertion."""

    params: list[tuple[int, int, Any]]
    collection_properties: list[tuple[str, int]]
    metadata_map: dict[tuple[int, int, Any], dict[str, Any]]
    normalized_records: list[dict[str, Any]]
    deprecated_format_used: bool


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


def _flatten_property_records(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    """Normalize incoming property records into a flat list."""
    normalized_records: list[dict[str, Any]] = []
    deprecated_format_used = False

    for record in records:
        object_name = record.get("name")
        if "properties" in record:
            deprecated_format_used = True
            record_text = record.get("datafile_text")
            record_timeslice = record.get("timeslice")
            for prop_name, prop_value in record.get("properties", {}).items():
                if isinstance(prop_value, dict):
                    value = prop_value.get("value")
                    band = prop_value.get("band") or prop_value.get("Band")
                    date_from = prop_value.get("date_from")
                    date_to = prop_value.get("date_to")
                    datafile_text = prop_value.get("datafile_text", record_text)
                    timeslice = prop_value.get("timeslice", record_timeslice)
                else:
                    value = prop_value
                    band = None
                    date_from = None
                    date_to = None
                    datafile_text = record_text
                    timeslice = record_timeslice

                normalized_records.append(
                    {
                        "name": object_name,
                        "property": prop_name,
                        "value": value,
                        "band": band,
                        "date_from": date_from,
                        "date_to": date_to,
                        "datafile_text": datafile_text,
                        "timeslice": timeslice,
                        "source_format": "nested",
                    }
                )
        elif "property" in record and "value" in record:
            normalized_records.append(
                {
                    "name": object_name,
                    "property": record.get("property"),
                    "value": record.get("value"),
                    "band": record.get("band"),
                    "date_from": record.get("date_from"),
                    "date_to": record.get("date_to"),
                    "datafile_text": record.get("datafile_text"),
                    "timeslice": record.get("timeslice"),
                    "source_format": "flat",
                }
            )
        else:
            raise ValueError(
                "Each record must include either a 'properties' dict or 'property'/'value' keys. "
                "Reshape the input to one of those forms before adding properties."
            )

    return normalized_records, deprecated_format_used


def plan_property_inserts(
    db: PlexosDB,
    records: list[dict[str, Any]],
    *,
    object_class: ClassEnum,
    collection: CollectionEnum,
    parent_class: ClassEnum,
) -> PreparedPropertiesResult:
    """Prepare SQL parameters for property insertion."""
    normalized_records, deprecated_format_used = _flatten_property_records(records)
    if not normalized_records:
        return PreparedPropertiesResult([], [], {}, [], deprecated_format_used)

    collection_id = db.get_collection_id(
        collection, parent_class_enum=parent_class, child_class_enum=object_class
    )
    collection_properties = _fetch_collection_properties(db, collection_id=collection_id)
    name_to_membership = _resolve_membership_map(db, normalized_records, object_class=object_class)
    property_id_map = {prop: pid for prop, pid in collection_properties}

    params, metadata_map = _build_property_rows(
        normalized_records, name_to_membership=name_to_membership, property_id_map=property_id_map
    )

    return PreparedPropertiesResult(
        params, collection_properties, metadata_map, normalized_records, deprecated_format_used
    )


def _fetch_collection_properties(db: PlexosDB, *, collection_id: int) -> list[tuple[str, int]]:
    """Fetch property rows for a collection as (name, id) tuples."""
    return db.query(f"select name, property_id from t_property where collection_id={collection_id}")


def _resolve_membership_map(
    db: PlexosDB,
    normalized_records: list[dict[str, Any]],
    *,
    object_class: ClassEnum,
) -> dict[str, int]:
    """Resolve membership ids for each object name."""
    component_names = tuple({d["name"] for d in normalized_records if d.get("name") is not None})
    try:
        memberships = db.get_memberships_system(component_names, object_class=object_class)
    except Exception as exc:
        missing = ", ".join(sorted(name for name in component_names if name))
        raise NotFoundError(
            f"Objects not found: {missing}. Add them with `add_object` or `add_objects` before "
            "adding properties."
        ) from exc

    if not memberships:
        missing = ", ".join(sorted(name for name in component_names if name))
        raise NotFoundError(
            f"Objects not found: {missing}. Add them with `add_object` or `add_objects` before "
            "adding properties."
        )

    return {membership["name"]: membership["membership_id"] for membership in memberships}


def _build_property_rows(
    normalized_records: list[dict[str, Any]],
    *,
    name_to_membership: dict[str, int],
    property_id_map: dict[str, int],
) -> tuple[list[tuple[int, int, Any]], dict[tuple[int, int, Any], dict[str, Any]]]:
    """Build parameter tuples and metadata for normalized records."""
    params: list[tuple[int, int, Any]] = []
    metadata_map: dict[tuple[int, int, Any], dict[str, Any]] = {}

    for record in normalized_records:
        membership_id = name_to_membership.get(record["name"])
        if not membership_id:
            continue

        property_id = property_id_map.get(record["property"])
        if not property_id:
            continue

        param_key = (membership_id, property_id, record["value"])
        params.append(param_key)
        metadata_map[param_key] = {
            "band": record.get("band"),
            "date_from": record.get("date_from"),
            "date_to": record.get("date_to"),
            "datafile_text": record.get("datafile_text"),
            "timeslice": record.get("timeslice"),
            "property_name": record.get("property"),
            "object_name": record.get("name"),
        }

    return params, metadata_map


def insert_property_values(
    db: PlexosDB,
    params: list[tuple[int, int, Any]],
    *,
    metadata_map: dict[tuple[int, int, Any], dict[str, Any]] | None = None,
) -> dict[tuple[int, int, Any], tuple[int, str]]:
    """Insert property data and return mapping of data IDs to object names.

    Parameters
    ----------
    db : PlexosDB
        Database instance
    params : list[tuple]
        List of (membership_id, property_id, value) tuples
    metadata_map : dict | None, optional
        Mapping of params to metadata (band, date_from, date_to), by default None

    Returns
    -------
    dict
        Mapping of (membership_id, property_id, value) to (data_id, obj_name)
    """
    if not params:
        return {}

    unique_property_ids = {property_id for _, property_id, _ in params}
    property_params = [(property_id,) for property_id in unique_property_ids]
    db._db.executemany("UPDATE t_property set is_dynamic=1 where property_id = ?", property_params)
    db._db.executemany("UPDATE t_property set is_enabled=1 where property_id = ?", property_params)

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
            data_id = result[0]
            obj_name = result[1]
            data_id_map[(membership_id, property_id, value)] = (data_id, obj_name)

    if metadata_map:
        _persist_metadata_for_data(db, metadata_map=metadata_map, data_id_map=data_id_map)

    return data_id_map


def apply_scenario_tags(
    db: PlexosDB,
    params: list[tuple[int, int, Any]],
    /,
    *,
    scenario: str,
    chunksize: int,
) -> None:
    """Insert scenario tags for property data.

    Parameters
    ----------
    db : PlexosDB
        Database instance
    params : list[tuple]
        List of (membership_id, property_id, value) tuples
    scenario : str
        Scenario name
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


def insert_property_texts(
    db: PlexosDB,
    params: list[tuple[int, int, Any]],
    /,
    *,
    data_id_map: dict[tuple[int, int, Any], tuple[int, str]],
    records: list[dict[str, Any]],
    field_name: str,
    text_class: ClassEnum,
    metadata_map: dict[tuple[int, int, Any], dict[str, Any]] | None = None,
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
    metadata_map : dict | None, optional
        Metadata map keyed by param tuple to drive property-specific text mapping
    """
    text_map = _build_text_lookup(records, field_name=field_name)
    class_id = db.get_class_id(text_class)
    texts_to_insert = _collect_text_rows(
        params, data_id_map, metadata_map=metadata_map, text_map=text_map, class_id=class_id
    )

    if texts_to_insert:
        db._db.executemany(
            "INSERT INTO t_text(data_id, class_id, value) VALUES (?,?,?)",
            texts_to_insert,
        )


def _persist_metadata_for_data(
    db: PlexosDB,
    *,
    metadata_map: dict[tuple[int, int, Any], dict[str, Any]],
    data_id_map: dict[tuple[int, int, Any], tuple[int, str]],
) -> None:
    """Attach band and date metadata for inserted data rows."""
    bands_to_insert: list[tuple[int, int]] = []
    dates_from_to_insert: list[tuple[int, str]] = []
    dates_to_to_insert: list[tuple[int, str]] = []

    for key, metadata in metadata_map.items():
        data_entry = data_id_map.get(key)
        if not data_entry:
            continue

        data_id = data_entry[0]
        band = metadata.get("band")
        date_from = metadata.get("date_from")
        date_to = metadata.get("date_to")

        if band is not None:
            bands_to_insert.append((data_id, band))

        _append_date_if_present(dates_from_to_insert, data_id, date_value=date_from, label="date_from")
        _append_date_if_present(dates_to_to_insert, data_id, date_value=date_to, label="date_to")

    if bands_to_insert:
        db._db.executemany("INSERT INTO t_band(data_id, band_id) VALUES (?, ?)", bands_to_insert)
    if dates_from_to_insert:
        db._db.executemany("INSERT INTO t_date_from(data_id, date) VALUES (?, ?)", dates_from_to_insert)
    if dates_to_to_insert:
        db._db.executemany("INSERT INTO t_date_to(data_id, date) VALUES (?, ?)", dates_to_to_insert)


def _append_date_if_present(
    target: list[tuple[int, str]], data_id: int, *, date_value: datetime | None, label: str
) -> None:
    """Validate and append date metadata when provided."""
    if date_value is None:
        return
    if not isinstance(date_value, datetime):
        raise TypeError(f"{label} must be a datetime object")
    target.append((data_id, date_value.isoformat()))


def _build_text_lookup(
    records: list[dict[str, Any]], *, field_name: str
) -> dict[tuple[str, str | None], Any]:
    """Create a lookup of object/property combinations to text values."""
    text_map: dict[tuple[str, str | None], Any] = {}
    for rec in records:
        obj_name = rec.get("name")
        if obj_name is None:
            continue

        if field_name in rec:
            text_map[(obj_name, rec.get("property"))] = rec[field_name]

        if "properties" in rec and field_name in rec:
            text_map[(obj_name, None)] = rec[field_name]

        for prop_name, prop_value in rec.get("properties", {}).items():
            if isinstance(prop_value, dict) and field_name in prop_value:
                text_map[(obj_name, prop_name)] = prop_value[field_name]

    return text_map


def _collect_text_rows(
    params: list[tuple[int, int, Any]],
    data_id_map: dict[tuple[int, int, Any], tuple[int, str]],
    *,
    metadata_map: dict[tuple[int, int, Any], dict[str, Any]] | None,
    text_map: dict[tuple[str, str | None], Any],
    class_id: int,
) -> list[tuple[int, int, Any]]:
    """Convert params and metadata into t_text insert rows."""
    texts_to_insert: list[tuple[int, int, Any]] = []

    for membership_id, property_id, value in params:
        data_id, obj_name = data_id_map.get((membership_id, property_id, value), (None, None))
        if not data_id or not obj_name:
            continue

        property_name = (
            metadata_map.get((membership_id, property_id, value), {}).get("property_name")
            if metadata_map
            else None
        )
        lookup_keys = [(obj_name, property_name), (obj_name, None)]
        for lookup in lookup_keys:
            if lookup in text_map:
                texts_to_insert.append((data_id, class_id, text_map[lookup]))
                break

    return texts_to_insert


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
