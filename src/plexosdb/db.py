"""Main API for interacting with the Plexos database schema."""

import sqlite3
import sys
import uuid
from collections.abc import Iterable, Iterator
from importlib.resources import files
from pathlib import Path
from string import Template
from typing import Any, Literal, TypedDict, cast

from loguru import logger

from .checks import check_memberships_from_records
from .db_manager import SQLiteManager
from .enums import ClassEnum, CollectionEnum, Schema, get_default_collection, str2enum
from .exceptions import (
    NameError,
    NoPropertiesError,
    NotFoundError,
)
from .utils import create_membership_record, no_space, normalize_names, prepare_sql_data_params
from .xml_handler import XMLHandler

if sys.version_info >= (3, 12):
    from itertools import batched
else:
    from .utils import batched

SQLITE_BACKEND_KWARGS = {"in_memory"}
CHECK_QUERY = "SELECT 1 FROM ${schema} ${where_clause}"
PLEXOS_DEFAULT_SCHEMA = fpath = files("plexosdb").joinpath("schema.sql").read_text(encoding="utf-8-sig")
PROPERTY_QUERY = files("plexosdb.queries").joinpath("object_properties.sql").read_text(encoding="utf-8-sig")


class PropertyRecord(TypedDict, total=False):
    """Type definition for property records returned by iterate_properties.

    Attributes
    ----------
    parent_class : str
        Name of the parent class
    child_class : str
        Name of the child class
    parent_id : int
        ID of the parent object
    object_id : int
        ID of the child object
    name : str
        Name of the object
    category : str
        Category name of the object
    property : str
        Property name
    unit : str
        Unit of measurement
    value : float | str | None
        Property value
    band : int
        Band ID (defaults to 1)
    date_from : str | None
        Start date for the property
    date_to : str | None
        End date for the property
    text : str | None
        Associated text data
    text_class_name : str | None
        Class name of the text
    timeslice_id : str | None
        Name of associated timeslice
    datafile_name : str | None
        Name of associated data file
    datafile_id : int | None
        ID of associated data file
    variable_name : str | None
        Name of associated variable
    variable_id : int | None
        ID of associated variable
    action : str | None
        Action symbol
    scenario_name : str | None
        Name of associated scenario
    """


class PlexosDB:
    """High-level API for PlexosDB schema manipulation."""

    def __init__(
        self,
        fpath_or_conn: Path | str | sqlite3.Connection | None = None,
        new_db: bool = False,
        **kwargs,
    ) -> None:
        """Initialize the API using an XML file or other data sources.

        Parameters
        ----------
        xml_fname : str | None
            The XML filename to ingest data from. If None, uses in-memory DB.
        **kwargs : dict
            Additional keyword arguments for the backend.
        """
        sqlite_kwargs = {key: value for key, value in kwargs.items() if key in SQLITE_BACKEND_KWARGS}
        self._db: SQLiteManager = SQLiteManager(fpath_or_conn=fpath_or_conn, **sqlite_kwargs)
        self._db.add_collation("NOSPACE", no_space)

        self._version = None
        if not new_db:
            self._version = self._get_plexos_version()

    @property
    def version(self) -> tuple[int, ...] | None:
        """Get the PLEXOS version of the loaded model."""
        if not self._version:
            self._version = self._get_plexos_version()
        return self._version

    def _get_plexos_version(self) -> tuple[int, ...] | None:
        """Initialize the PLEXOS version from the database."""
        try:
            result = self._db.fetchone("SELECT value FROM t_config WHERE element = 'Version'")
        except sqlite3.OperationalError:
            return None
        if not result:
            return None
        return tuple(map(int, result[0].split(".")))

    @classmethod
    def from_xml(cls, xml_path: str | Path, schema: str | None = None, **kwargs) -> "PlexosDB":
        """Construct a PlexosDB instance from an XML file.

        This factory method creates a new PlexosDB instance and populates it with data
        from the provided XML file. It creates the database schema and processes all
        valid XML elements into their corresponding database tables.

        Parameters
        ----------
        xml_path : str | Path
            Path to the XML file to import data from
        schema: str | None
            SQL schema to initialize the database
        **kwargs : dict
            Additional keyword arguments to pass to the PlexosDB constructor

        Returns
        -------
        PlexosDB
            A new PlexosDB instance initialized with data from the XML file

        Raises
        ------
        FileNotFoundError
            If the specified XML file does not exist

        See Also
        --------
        PlexosDB : PLEXOS SQLite manager
        create_schema : Creates the database schema
        XMLHandler.parse : Parses the XML file
        str2enum : Converts XML tag names to schema enumerations

        Notes
        -----
        This method groups records by their column structure to handle varying record
        formats in the XML data. Each group of records with consistent structure is
        processed separately to avoid SQL errors from column mismatches.

        This constructor assumes we are creating a new database. If you want to add
        data to an existing database, you should use other methods.

        Examples
        --------
        >>> db = PlexosDB.from_xml("model.xml")
        >>> print(db.version)
        '8.3.0'
        """
        if not isinstance(xml_path, Path):
            xml_path = Path(xml_path)

        if not xml_path.exists():
            msg = (
                "Input XML file does not exist. "
                "Make sure that you are passing the correct location of the `xml_fname`."
            )
            raise FileNotFoundError(msg)

        instance = cls(new_db=True, **kwargs)
        instance.create_schema(schema=schema)

        # Temporarily disable foreign key constraints for bulk XML import
        instance._db.execute("PRAGMA foreign_keys = OFF")

        xml_handler = XMLHandler.parse(fpath=xml_path)
        xml_tags = set([e.tag for e in xml_handler.root])  # Extract set of valid tags from xml
        for tag in xml_tags:
            # Only parse valid schemas that we maintain.
            # NOTE: If there are some missing tables, we need to add them to the Enums.
            schema_enum = str2enum(tag)
            if not schema_enum:
                continue

            record_dict = xml_handler.get_records(schema_enum)
            if not record_dict:  # Skip if no records
                continue

            # Group records by column structure to avoid mismatches
            column_groups: dict[frozenset[str], list[dict[str, Any]]] = {}
            for record in record_dict:
                # Create a hashable key from the record's column names
                column_key = frozenset(record.keys())
                if column_key not in column_groups:
                    column_groups[column_key] = []
                column_groups[column_key].append(record)

            # Process each group of consistently structured records separately
            for columns, records in column_groups.items():
                column_names = list(columns)
                placeholders = ", ".join([f":{s}" for s in column_names])
                columns_sql = ", ".join([f"`{key}`" for key in column_names])
                query = f"INSERT INTO {tag} ({columns_sql}) values({placeholders})"
                logger.trace("{}", query)

                insert_result = instance._db.executemany(query, records)
                if not insert_result:
                    logger.warning(f"No rows inserted for {tag} with columns {column_names}")

        # Re-enable foreign key constraints after import
        instance._db.execute("PRAGMA foreign_keys = ON")

        return instance

    def add_attribute(
        self,
        object_class: ClassEnum,
        /,
        object_name: str,
        *,
        attribute_name: str,
        attribute_value: str | float | int,
    ) -> int:
        """Add attribute to a given object.

        Attributes are different from properties. They live on a separate table
        and are mostly used for model configuration.

        Parameters
        ----------
        object_class
            ClassEnum from the object to be added. E.g., for generators class_id=ClassEnum.Generators
        object_name : str
            Name to be added to the object
        attribute_name
            Valid name of the attribute to be added for the given class
        attribute_value
            Value to be added to the attribute

        Returns
        -------
        int
            attribute_id

        Notes
        -----
        By default, we add all objects to the system membership.
        """
        object_id = self.get_object_id(object_class, name=object_name)
        attribute_id = self.get_attribute_id(object_class, name=attribute_name)
        params = (object_id, attribute_id, attribute_value)
        placeholders = ", ".join("?" * len(params))
        query = (
            f"INSERT INTO {Schema.AttributeData.name}(object_id, attribute_id, value) VALUES({placeholders})"
        )
        attribute_id = self._db.execute(query, params)
        return attribute_id

    def add_band(
        self,
        data_id: int,
        band_id: int,
        /,
        *,
        state: int | None = None,
    ) -> None:
        """Add a band to a property data record.

        Parameters
        ----------
        data_id : int
            ID of the data record to add the band to
        band_id : int
            ID of the band to add
        state : int | None, optional
            State value, by default None
        """
        raise NotImplementedError

    def add_category(self, class_enum: ClassEnum, /, name: str) -> int:
        """Add a new category for a given class.

        Creates a new category for the specified class. If the category already exists,
        it returns the ID of the existing category.

        Parameters
        ----------
        class_name : ClassEnum
            Class enumeration for the category, e.g., for generators use ClassEnum.Generator
        name : str
            Name of the category to be added

        Returns
        -------
        int
            The ID of the created or existing category

        See Also
        --------
        check_category_exists : Check if a category exists for a specific class
        get_category_id : Get the ID for an existing category
        get_category_max_id : Get the maximum rank ID for a class's categories
        get_class_id : Get the ID for a class

        Notes
        -----
        The database schema does not enforce uniqueness on the `class_id`, which means
        multiple categories with the same name can exist for different classes. The category's
        unique identifier is automatically handled by the database.

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_category("mycategory", ClassEnum.Generator)
        1
        """
        if self.check_category_exists(class_enum, name):
            logger.debug("Category `{}` for `{}` already exist. Returning id instead.", name, class_enum)
            return self.get_category_id(class_enum, name)

        class_id = self.get_class_id(class_enum)
        rank = self.get_category_max_id(class_enum) or 1  # Default to max rank of 1 if no category exists
        params = (class_id, name, rank)
        placeholders = ", ".join("?" * len(params))
        query = f"INSERT INTO {Schema.Categories.name}(class_id, name, rank) VALUES({placeholders})"
        result = self._db.execute(query, params)
        assert result
        return self._db.last_insert_rowid()

    def add_custom_column(
        self,
        class_enum: ClassEnum,
        name: str,
        /,
        *,
        position: int | None = None,
    ) -> int:
        """Add a custom column to a class."""
        raise NotImplementedError

    def add_datafile_tag(
        self,
        data_id: int,
        file_path: str,
        /,
        *,
        description: str | None = None,
    ) -> int:
        """Add a Data File tag to a property data record."""
        raise NotImplementedError

    def add_membership(
        self,
        parent_class_enum: ClassEnum,
        child_class_enum: ClassEnum,
        parent_object_name: str,
        child_object_name: str,
        collection_enum: CollectionEnum,
    ) -> int:
        """Add a membership between two objects for a given collection.

        Creates a relationship between parent and child objects within the specified collection.

        Parameters
        ----------
        parent_class_enum : ClassEnum
            Class enumeration of the parent object
        child_class_enum : ClassEnum
            Class enumeration of the child object
        parent_object_name : str
            Name of the parent object
        child_object_name : str
            Name of the child object
        collection_enum : CollectionEnum
            Collection enumeration defining the relationship type

        Returns
        -------
        int
            ID of the created membership

        See Also
        --------
        get_class_id : Get the ID for a class
        get_object_id : Get the ID for an object
        get_collection_id : Get the ID for a collection
        get_membership_id : Get the ID for an existing membership

        Notes
        -----
        This method establishes relationships between objects in the PLEXOS model.
        The database enforces uniqueness for parent-child-collection combinations.

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Region, "Parent")
        >>> db.add_object(ClassEnum.Node, "Child")
        >>> db.add_membership(ClassEnum.Region, ClassEnum.Node, "Parent", "Child")
        1
        """
        parent_class_id = self.get_class_id(parent_class_enum)
        child_class_id = self.get_class_id(child_class_enum)
        parent_object_id = self.get_object_id(parent_class_enum, parent_object_name)
        child_object_id = self.get_object_id(child_class_enum, child_object_name)
        collection_id = self.get_collection_id(collection_enum, parent_class_enum, child_class_enum)

        # NOTE: Measure if this is faster than passing the ids
        query = f"""
        INSERT INTO {Schema.Memberships.name}
            (parent_class_id,parent_object_id, collection_id, child_class_id, child_object_id)
        VALUES
            (?, ?, ?, ?, ?)
        """
        params = (parent_class_id, parent_object_id, collection_id, child_class_id, child_object_id)
        query_status = self._db.execute(query, params)
        assert query_status, "Membership already exists for the parent and object combination."
        self._db.execute("UPDATE t_collection set is_enabled=1 where collection_id = ?", (collection_id,))
        return self._db.last_insert_rowid()

    def add_memberships_from_records(
        self,
        records: list[dict[str, int]],
        /,
        *,
        chunksize: int = 10_000,
    ) -> bool:
        """Bulk insert multiple memberships from a list of records.

        Efficiently adds multiple membership relationships between objects in batches.
        This method is much more efficient than calling `add_membership` multiple times.

        Parameters
        ----------
        records : list[dict[str, int]]
            List of membership records. Each record should be a dictionary with these fields:
            - 'parent_class_id': int - ID of the parent class
            - 'parent_object_id': int - ID of the parent object
            - 'collection_id': int - ID of the collection
            - 'child_class_id': int - ID of the child class
            - 'child_object_id': int - ID of the child object
        chunksize : int, optional
            Number of records to process in each batch, by default 10_000.
            Useful for controlling memory usage with large datasets.

        Returns
        -------
        bool
            True if the operation was successful

        Raises
        ------
        KeyError
            If any records are missing required fields

        See Also
        --------
        add_membership : Add a single membership between two objects
        check_memberships_from_records : Validate membership records format
        create_membership_record : Helper to create membership record dictionaries

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> # Create parent and children objects
        >>> parent_id = db.add_object(ClassEnum.Region, "Region1")
        >>> child_ids = []
        >>> for i in range(5):
        ...     child_ids.append(db.add_object(ClassEnum.Node, f"Node{i}"))
        >>> # Prepare membership records
        >>> parent_class_id = db.get_class_id(ClassEnum.Region)
        >>> child_class_id = db.get_class_id(ClassEnum.Node)
        >>> collection_id = db.get_collection_id(
        ...     CollectionEnum.RegionNodes,
        ...     parent_class_enum=ClassEnum.Region,
        ...     child_class_enum=ClassEnum.Node,
        ... )
        >>> # Create records
        >>> records = []
        >>> for child_id in child_ids:
        ...     records.append(
        ...         {
        ...             "parent_class_id": parent_class_id,
        ...             "parent_object_id": parent_id,
        ...             "collection_id": collection_id,
        ...             "child_class_id": child_class_id,
        ...             "child_object_id": child_id,
        ...         }
        ...     )
        >>> # Bulk insert all memberships at once
        >>> db.add_memberships_from_records(records)
        True
        """
        if not check_memberships_from_records(records):
            msg = "Some of the records do not have all the required fields. "
            msg += "Check construction of records."
            raise KeyError(msg)
        query = f"""
        INSERT INTO {Schema.Memberships.name}
            (parent_class_id,parent_object_id, collection_id, child_class_id, child_object_id)
        VALUES
            (:parent_class_id, :parent_object_id, :collection_id, :child_class_id, :child_object_id)
        """
        query_status = self._db.executemany(query, records)
        assert query_status
        logger.debug("Added {} memberships.", len(records))
        return True

    def add_metadata(
        self,
        entity_type: Literal["object", "membership", "data"],
        entity_id: int,
        class_name: str,
        property_name: str,
        value: str,
        /,
        *,
        state: int | None = None,
    ) -> None:
        """Add metadata to an entity (object, membership, or data)."""
        raise NotImplementedError

    def add_object(
        self,
        class_enum: ClassEnum,
        /,
        name: str,
        *,
        description: str | None = None,
        category: str | None = None,
        collection_enum: CollectionEnum | None = None,
    ) -> int:
        """Add an object to the database and append a system membership.

        Creates a new object in the database with the given name and class. Automatically
        creates a system membership for the object.

        Parameters
        ----------
        name : str
            Name of the object to be added
        class_enum : ClassEnum
            Class enumeration of the object, e.g., ClassEnum.Generator for a generator object
        category : str, optional
            Category of the object, by default "-"
        description : str | None, optional
            Description of the object, by default None
        collection_enum : CollectionEnum | None, optional
            Collection for the system membership. If None, a default collection is determined
            based on the class, by default None

        Returns
        -------
        int
            ID of the created object

        Raises
        ------
        sqlite.IntegrityError
            If an object is inserted without a unique name/class pair

        See Also
        --------
        check_category_exists : Check if a category exists for a specific class
        add_category : Add a new category for a given class
        get_default_collection : Get the default collection for a class
        add_membership : Add a membership between two objects

        Notes
        -----
        All objects need to have a system membership. If you want to add additional
        membership(s) to other object(s), consider `add_membership` or `add_memberships`.

        The database enforces uniqueness on the combination of name and class_id.

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> object_id = db.add_object("TestGenerator", ClassEnum.Generator)
        1
        """
        category_id = None
        category = category or "-"
        if not self.check_category_exists(class_enum, category):
            category_id = self.add_category(class_enum, category)

        category_id = category_id or self.get_category_id(class_enum, category)
        class_id = self.get_class_id(class_enum)
        params = (name, class_id, category_id, str(uuid.uuid4()), description)
        placeholders = ", ".join("?" * len(params))
        query = (
            f"INSERT INTO {Schema.Objects.name}(name, class_id, category_id, GUID, description) "
            f"VALUES({placeholders})"
        )
        query_result = self._db.execute(query, params)
        assert query_result
        object_id = self._db.last_insert_rowid()

        if not collection_enum:
            collection_enum = get_default_collection(class_enum)
        _ = self.add_membership(ClassEnum.System, class_enum, "System", name, collection_enum)
        return object_id

    def add_objects(self, *object_names, class_enum: ClassEnum, category: str | None = None) -> None:
        """Add multiple objects of the same class to the database in bulk.

        This method efficiently adds multiple objects to the database in a single operation,
        which is much more performant than calling add_object() multiple times. It also
        creates system memberships for all objects in bulk.

        Parameters
        ----------
        object_names : Iterable[str]
            Names of the objects to be added
        class_enum : ClassEnum
            Class enumeration of the objects
        category : str | None, optional
            Category of the objects, by default None (which uses "-" as default category)

        Returns
        -------
        None

        See Also
        --------
        add_object : Add a single object
        add_memberships_from_records : Add multiple memberships in bulk
        normalize_names : Normalize object names for consistency
        get_default_collection : Get the default collection for a class

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> # Add multiple generators at once
        >>> generator_names = ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5"]
        >>> db.add_objects(generator_names, class_enum=ClassEnum.Generator, category="Thermal")
        """
        category_id = None
        category = category or "-"

        if not self.check_category_exists(class_enum, category):
            category_id = self.add_category(class_enum, category)

        class_id = self.get_class_id(class_enum)
        names = normalize_names(*object_names)
        params = [(name, class_id, category_id, str(uuid.uuid4())) for name in names]

        query = f"INSERT INTO {Schema.Objects.name}(name, class_id, category_id, GUID) "
        query += "VALUES(?,?,?,?)"
        query_result = self._db.executemany(query, params)
        assert query_result

        # Add system memberships in bulk
        collection_enum = get_default_collection(class_enum)
        object_ids = self.get_objects_id(names, class_enum=class_enum)
        parent_class_id = self.get_class_id(ClassEnum.System)
        parent_object_id = self.get_object_id(ClassEnum.System, "System")
        collection_id = self.get_collection_id(
            collection_enum, parent_class_enum=ClassEnum.System, child_class_enum=class_enum
        )
        membership_records = create_membership_record(
            object_ids,
            child_object_class_id=class_id,
            parent_object_class_id=parent_class_id,
            parent_object_id=parent_object_id,
            collection_id=collection_id,
        )
        self.add_memberships_from_records(membership_records)
        return

    def _add_texts_for_properties(
        self,
        params: list[tuple],
        data_id_map: dict,
        text_map: dict,
        text_class: ClassEnum,
    ) -> None:
        """Bulk add text data for each property record."""
        for membership_id, property_id, value in params:
            data_id, obj_name = data_id_map.get((membership_id, property_id, value), (None, None))
            if data_id and obj_name and text_map.get(obj_name):
                self.add_text(text_class, text_map[obj_name], data_id)

    def add_properties_from_records(
        self,
        records: list[dict],
        /,
        *,
        object_class: ClassEnum,
        parent_class: ClassEnum = ClassEnum.System,
        collection: CollectionEnum,
        scenario: str,
        text_class: ClassEnum | None = None,
        text_field: str = "text",
        chunksize: int = 10_000,
    ) -> None:
        """Bulk insert multiple properties from a list of records, with optional text data.

        Efficiently adds multiple properties to multiple objects in batches using
        transactions, which is significantly faster than calling add_property repeatedly.

        Each record should contain an 'object_name' field and property-value pairs.
        If a text field is present and text_class is provided, text data will be attached
        to the property data records.

        All properties are automatically marked as dynamic and enabled in the database.

        Parameters
        ----------
        records : list[dict]
            List of records where each record is a dictionary with:
            - 'object_name': str - Name of the object
            - property1: value1, property2: value2, ... - Property name-value pairs
        object_class : ClassEnum
            Class enumeration of the objects (e.g., ClassEnum.Generator)
        parent_class : ClassEnum, optional
            Parent class enumeration, by default ClassEnum.System
        collection : CollectionEnum
            Collection enumeration for the properties (e.g., CollectionEnum.Generators)
        scenario : str
            Scenario name to associate with all properties
        text_class : ClassEnum | None, optional
            ClassEnum for the text data (e.g., ClassEnum.File). If None, text is ignored.
        text_field : str, optional
            Name of the field in each record containing the text data, by default "text"
        chunksize : int, optional
            Number of records to process in each batch to control memory usage,
            by default 10_000

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If records are improperly formatted or missing required fields
        KeyError
            If an object name doesn't exist in the database

        See Also
        --------
        add_property : Add a single property
        check_property_exists : Check if properties exist for a collection
        add_memberships_from_records : Bulk insert multiple memberships
        prepare_sql_data_params : Utility for preparing SQL parameters from records

        Notes
        -----
        This method uses SQLite transactions to ensure all insertions are atomic.
        If an error occurs during processing, the entire transaction is rolled back.

        Properties are inserted directly using SQL for optimal performance, and all
        added properties are automatically marked as dynamic and enabled.

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> # Create objects first
        >>> db.add_object(ClassEnum.Generator, "Generator1")
        >>> db.add_object(ClassEnum.Generator, "Generator2")
        >>> # Prepare property records
        >>> records = [
        ...     {"name": "Generator1", "Max Capacity": 100.0, "text": "/path_to_file/gen1.txt"},
        ...     {"name": "Generator2", "Max Capacity": 150.0, "text": "/path_to_file/gen2.txt"},
        ... ]
        >>> # Add properties in bulk
        >>> db.add_properties_from_records(
        ...     records,
        ...     object_class=ClassEnum.Generator,
        ...     collection=CollectionEnum.Generators,
        ...     scenario="Base Case",
        ...     text_class=ClassEnum.DataFile,
        ...     text_field="text",
        ... )
        """
        if not records:
            logger.warning("No records provided for bulk property and text insertion")
            return

        collection_id = self.get_collection_id(
            collection, parent_class_enum=parent_class, child_class_enum=object_class
        )
        collection_properties = self.query(
            f"select name, property_id from t_property where collection_id={collection_id}"
        )
        component_names = tuple(d["name"] for d in records)
        memberships = self.get_memberships_system(component_names, object_class=object_class)

        if not memberships:
            raise KeyError(
                "Object do not exists on the database yet."
                "Make sure you use `add_object` before adding properties."
            )

        params = prepare_sql_data_params(
            records, memberships=memberships, property_mapping=collection_properties
        )

        # Map object name to text value for quick lookup
        text_map = {rec["name"]: rec.get(text_field) for rec in records}

        with self._db.transaction():
            filter_property_ids = [d[1] for d in params]
            for property_id in filter_property_ids:
                self._db.execute("UPDATE t_property set is_dynamic=1 where property_id = ?", (property_id,))
                self._db.execute("UPDATE t_property set is_enabled=1 where property_id = ?", (property_id,))

            self._db.executemany(
                "INSERT into t_data(membership_id, property_id, value) values (?,?,?)", params
            )

            data_ids_query = """
                SELECT d.data_id, o.name
                FROM t_data d
                JOIN t_membership m ON d.membership_id = m.membership_id
                JOIN t_object o ON m.child_object_id = o.object_id
                WHERE d.membership_id = ? AND d.property_id = ? AND d.value = ?
            """
            data_id_map = {}
            for membership_id, property_id, value in params:
                result = self._db.fetchone(data_ids_query, (membership_id, property_id, value))
                if result:
                    data_id_map[(membership_id, property_id, value)] = (result[0], result[1])

            # Insert scenario tags
            if scenario is not None:
                if not self.check_scenario_exists(scenario):
                    scenario_id = self.add_scenario(scenario)
                else:
                    scenario_id = self.get_scenario_id(scenario)
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
                    self._db.executemany(scenario_query, batched_list)

            # Insert text data for each property record using a helper
            if text_class:
                self._add_texts_for_properties(params, data_id_map, text_map, text_class)

        logger.debug(f"Successfully processed {len(records)} property and text records in batches")
        return

    def add_property(
        self,
        object_class_enum: ClassEnum,
        /,
        object_name: str,
        name: str,
        value: str | int | float,
        *,
        scenario: str | None = None,
        band: str | int | None = None,
        date_from: str | None = None,
        text: dict[ClassEnum, Any] | None = None,
        collection_enum: CollectionEnum | None = None,
        parent_class_enum: ClassEnum | None = None,
        parent_object_name: str | None = None,
    ) -> int:
        """Add a property for a given object in the database.

        Adds a property with the specified value to an object, optionally associating
        it with a scenario, band, date range, and text data.

        Parameters
        ----------
        object_class_enum : ClassEnum
            Class enumeration of the object
        object_name : str
            Name of the object to add the property to
        name : str
            Name of the property to add
        value : str | int | float
            Value to assign to the property
        scenario : str | None, optional
            Name of the scenario to associate with this property, by default None
        band : str | int | None, optional
            Band to associate with this property, by default None
        date_from : str | None, optional
            Start date for this property, by default None
        date_to : str | None, optional
            End date for this property, by default None
        text : dict[ClassEnum, Any] | None, optional
            Additional text data to associate with this property, by default None
        collection_enum : CollectionEnum | None, optional
            Collection enumeration for the property. If None, a default is determined
            based on the object class, by default None
        parent_class_enum : ClassEnum | None, optional
            Class enumeration of the parent object. If None, defaults to ClassEnum.System,
            by default None
        parent_object_name : str | None, optional
            Name of the parent object. If None, defaults to "System", by default None

        Returns
        -------
        int
            ID of the created property data record

        Raises
        ------
        NameError
            If the property name does not exist for the specified collection
            If the object name does not exist

        See Also
        --------
        get_object_id : Get the ID for an object
        get_default_collection : Get the default collection for a class
        list_valid_properties : List valid property names for a collection
        get_property_id : Get the ID for a property
        get_membership_id : Get the ID for a membership
        check_scenario_exists : Check if a scenario exists
        add_object : Add a new object to the database

        Notes
        -----
        The method validates that the property name is valid for the given collection.
        If a scenario is provided but doesn't exist, it will be created automatically.
        Text data can include additional information associated with different classes.

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Generator, "Generator1")
        >>> db.add_property(ClassEnum.Generator, "Generator1", "Max Capacity", 100.0)
        1
        """
        # Ensure object exist
        if not self.check_object_exists(object_class_enum, object_name):
            msg = f"Object = `{object_name}` does not exist on the system. "
            f"Check available objects for class `{object_class_enum}` using `list_objects_by_class`"
            raise NotFoundError(msg)
        _ = self.get_object_id(object_class_enum, object_name)

        if not collection_enum:
            collection_enum = get_default_collection(object_class_enum)

        parent_class_enum = parent_class_enum or ClassEnum.System

        valid_properties = self.list_valid_properties(
            collection_enum, child_class_enum=object_class_enum, parent_class_enum=parent_class_enum
        )
        if name not in valid_properties:
            msg = (
                f"Property {name} does not exist for collection: {collection_enum}. "
                f"Run `self.list_valid_properties({collection_enum}) to verify valid properties."
            )
            raise NameError(msg)

        property_id = self.get_property_id(
            name,
            parent_class_enum=parent_class_enum,
            child_class_enum=object_class_enum,
            collection_enum=collection_enum,
        )
        membership_id = self.get_membership_id(parent_object_name or "System", object_name, collection_enum)

        query = f"INSERT INTO {Schema.Data.name}(membership_id, property_id, value) values (?, ?, ?)"
        result = self._db.execute(query, (membership_id, property_id, value))
        assert result
        data_id = self._db.last_insert_rowid()

        if scenario is not None:
            if not self.check_scenario_exists(scenario):
                scenario_id = self.add_scenario(scenario)
            else:
                scenario_id = self.get_scenario_id(scenario)
            scenario_query = "INSERT INTO t_tag(object_id,data_id) VALUES (?,?)"
            result = self._db.execute(scenario_query, (scenario_id, data_id))

        # Text could contain multiple keys, if so we add all of them with a execute many.
        if text is not None:
            for key, value in text.items():
                text_result = self.add_text(key, value, data_id)
                assert text_result
        return data_id

    def add_scenario(self, name: str, category: str | None = None) -> int:
        """Add a scenario object in the database.

        Parameters
        ----------
        name: str
            Name of the scenario
        category: str | None
            Name of the category for the scenario

        See Also
        --------
        get_scenario_id : Get the ID for a scenario
        list_scenarios : List all available scenarios in the database

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_scenario("TestScenario")
        >>> db.list_scenarios()
        [("TestScenario")]
        """
        if self.check_scenario_exists(name):
            msg = f"Scenario = `{name}` exist on the database. "
            "Select a different name."
            raise NameError(msg)
        object_id = self.add_object(
            ClassEnum.Scenario, name=name, category=category, collection_enum=CollectionEnum.Scenarios
        )
        return object_id

    def add_report(
        self,
        /,
        *,
        object_name: str,
        property: str,
        collection: CollectionEnum,
        parent_class: ClassEnum,
        child_class: ClassEnum,
        phase_id: int = 4,
        report_period: bool | None = None,
        report_summary: bool | None = True,
        report_statistics: bool | None = None,
        report_samples: bool | None = None,
        write_flat_files: bool = False,
    ) -> None:
        """Add a report configuration to the database.

        Creates a new report in the database with the specified properties and output options.
        Reports define what data will be available for post-processing after simulation runs.

        Parameters
        ----------
        object_name : str
            Name of the report object to add the configuration to
        property : str
            Name of the property to report on
        collection : CollectionEnum
            Collection enumeration that contains the property
        parent_class : ClassEnum
            Parent class enumeration for the collection
        child_class : ClassEnum
            Child class enumeration for the collection
        phase_id : int, optional
            Phase ID for the report (1=ST, 2=MT, 3=PASA, 4=LT), by default 4 (Long Term)
        report_period : bool | None, optional
            Whether to report period data, by default None
        report_summary : bool | None, optional
            Whether to report summary data, by default True
        report_statistics : bool | None, optional
            Whether to report statistics, by default None
        report_samples : bool | None, optional
            Whether to report samples, by default None
        write_flat_files : bool, optional
            Whether to output flat files, by default False

        Raises
        ------
        NameError
            If the specified property does not exist for the collection

        See Also
        --------
        get_object_id : Get the ID for an object
        get_collection_id : Get the ID for a collection
        list_valid_properties_report : List valid report properties for a collection

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Report, "GeneratorOutput")
        >>> db.add_report(
        ...     object_name="GeneratorOutput",
        ...     property="Generation",
        ...     collection=CollectionEnum.Generators,
        ...     parent_class=ClassEnum.System,
        ...     child_class=ClassEnum.Generator,
        ...     report_period=True,
        ...     report_summary=True,
        ... )
        """
        object_id = self.get_object_id(ClassEnum.Report, object_name)
        collection_id = self.get_collection_id(
            collection, parent_class_enum=parent_class, child_class_enum=child_class
        )
        valid_properties = self.list_valid_properties_report(
            collection, parent_class_enum=parent_class, child_class_enum=child_class
        )
        if property not in valid_properties:
            msg = (
                f"Property {property} does not exist for collection: {collection}. "
                "Check valid properties for the report type using `list_valid_properties_report`."
            )
            raise NameError(msg)

        # NOTE: We can migrate this to its own `get_property_report_id` if needed.
        property_id = self.query(
            "select property_id from t_property_report where collection_id = ? and name = ?",
            (collection_id, property),
        )[0][0]

        report_query = """
        INSERT INTO
        t_report(object_id, property_id, phase_id, report_period,
        report_summary, report_statistics, report_samples, write_flat_files)
        VALUES (?,?,?,?,?,?,?,?)
        """
        self._db.execute(
            report_query,
            (
                object_id,
                property_id,
                phase_id,
                report_period,
                report_summary,
                report_statistics,
                report_samples,
                write_flat_files,
            ),
        )
        return

    def add_text(
        self,
        text_class: ClassEnum,
        text_value: str | int | float,
        data_id: int,
    ) -> int:
        """Add text data to a property data record.

        Parameters
        ----------
        text_class : ClassEnum
            Name of the Text class to be added
        text_value : str
            Value of the text to be added
        data_id : int
            Data to be tagged with the Text object

        Raises
        ------
        AssertionError
            If the class does not exist

        Returns
        -------
        bool
            True if the query was succesfull
        """
        class_id = self.get_class_id(text_class)
        query_string = "INSERT INTO t_text(class_id, value, data_id) VALUES(?,?,?)"
        return self._db.execute(query_string, (class_id, text_value, data_id))

    def add_time_slice(
        self,
        name: str,
        /,
        *,
        interval: str,
        start_datetime: str,
        end_datetime: str,
        description: str | None = None,
    ) -> int:
        """Add a time slice definition to the database."""
        raise NotImplementedError

    def add_variable_tag(
        self,
        data_id: int,
        variable_name: str,
        /,
        *,
        value: str | float | None = None,
    ) -> int:
        """Add a Variable tag to a property data record."""
        raise NotImplementedError

    def backup_database(self, target_path: str | Path) -> None:
        """Backup the in-memory database to a file."""
        raise NotImplementedError

    def check_attribute_exists(
        self, attribute_name: str, /, *, object_name: str, object_class: ClassEnum
    ) -> bool:
        """Check if an attribute exists for a specific object."""
        raise NotImplementedError

    def check_category_exists(self, class_enum: ClassEnum, name: str) -> bool:
        """Check if a category exists for a specific class.

        Determines whether a category with the given name exists for the specified class.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration to check the category for
        name : str
            Name of the category to check

        Returns
        -------
        bool
            True if the category exists, False otherwise

        Raises
        ------
        NotFoundError
            If class_enum does not exist in the database. This indicates a programming
            error - you cannot check categories for a non-existent class.

        See Also
        --------
        get_class_id : Get the ID for a class
        add_category : Add a new category
        check_class_exists : Check if a class exists

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_category(ClassEnum.Generator, "my_category")
        >>> db.check_category_exists(ClassEnum.Generator, "my_category")
        True
        >>> db.check_category_exists(ClassEnum.Generator, "nonexistent")
        False
        """
        # Validate class exists first
        if not self.check_class_exists(class_enum):
            msg = (
                f"Class '{class_enum}' does not exist. "
                "Cannot check category for non-existent class. "
                "Use `list_classes()` to see available classes."
            )
            raise NotFoundError(msg)

        query = f"SELECT 1 FROM {Schema.Categories.name} WHERE name = ? AND class_id = ?"
        class_id = self.get_class_id(class_enum)
        return bool(self._db.query(query, (name, class_id)))

    def check_class_exists(self, class_enum: ClassEnum) -> bool:
        """Check if a class exists in the database.

        Determines whether a class with the given enumeration exists in the schema.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration to check

        Returns
        -------
        bool
            True if the class exists, False otherwise

        See Also
        --------
        get_class_id : Get the ID for a class
        list_classes : List all available classes

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.check_class_exists(ClassEnum.Generator)
        True
        >>> db.check_class_exists(ClassEnum.Generator)
        True
        """
        query = f"SELECT 1 FROM {Schema.Class.name} WHERE name = ?"
        return bool(self._db.query(query, (class_enum,)))

    def check_collection_exists(
        self,
        collection_enum: CollectionEnum,
        /,
        *,
        parent_class: ClassEnum | None = None,
        child_class: ClassEnum | None = None,
    ) -> bool:
        """Check if a collection exists in the database.

        Determines whether a collection with the given enumeration exists, optionally
        filtered by parent and/or child class.

        Parameters
        ----------
        collection_enum : CollectionEnum
            Collection enumeration to check
        parent_class : ClassEnum | None, optional
            Parent class enumeration to filter by, by default None
        child_class : ClassEnum | None, optional
            Child class enumeration to filter by, by default None

        Returns
        -------
        bool
            True if the collection exists (matching all specified criteria), False otherwise

        Raises
        ------
        NotFoundError
            If parent_class or child_class is specified but does not exist in the database.
            This indicates a programming error - you cannot search for a collection
            associated with a non-existent class.

        See Also
        --------
        get_collection_id : Get the ID for a collection
        list_collections : List all available collections
        check_class_exists : Check if a class exists

        Notes
        -----
        The method returns False only when the collection itself doesn't exist or doesn't
        match the specified parent/child class criteria. If you explicitly pass a parent_class
        or child_class that doesn't exist, it raises NotFoundError because this is a
        programming error - you cannot look for a collection for a non-existing class.

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.check_collection_exists(CollectionEnum.Generators)
        True
        >>> db.check_collection_exists(
        ...     CollectionEnum.Generators, parent_class=ClassEnum.System, child_class=ClassEnum.Generator
        ... )
        True
        >>> # This returns False - collection exists but not for this combination
        >>> db.check_collection_exists(CollectionEnum.Generators, parent_class=ClassEnum.Region)
        False
        >>> # This raises NotFoundError - the class itself doesn't exist
        >>> db.check_collection_exists(CollectionEnum.Generators, parent_class=ClassEnum.InvalidClass)
        NotFoundError: Parent class 'InvalidClass' does not exist
        """
        conditions = ["name = ?"]
        params: list[str | int] = [str(collection_enum)]

        if parent_class and not self.check_class_exists(parent_class):
            msg = (
                f"Parent class '{parent_class}' does not exist. "
                "Cannot search for collection with non-existent parent class. "
                "Use `list_classes()` to see available classes."
            )
            raise NotFoundError(msg)

        if parent_class:
            parent_class_id = self.get_class_id(parent_class)
            conditions.append("parent_class_id = ?")
            params.append(parent_class_id)

        if child_class is not None and not self.check_class_exists(child_class):
            msg = (
                f"Child class '{child_class}' does not exist. "
                "Cannot search for collection with non-existent child class. "
                "Use `list_classes()` to see available classes."
            )
            raise NotFoundError(msg)

        if child_class:
            child_class_id = self.get_class_id(child_class)
            conditions.append("child_class_id = ?")
            params.append(child_class_id)

        where_clause = " AND ".join(conditions)
        query = f"SELECT 1 FROM {Schema.Collection.name} WHERE {where_clause}"
        return bool(self._db.query(query, tuple(params)))

    def check_membership_exists(
        self,
        parent_object_name: str,
        child_object_name: str,
        /,
        *,
        parent_class: ClassEnum,
        child_class: ClassEnum,
        collection: CollectionEnum,
    ) -> bool:
        """Check if a membership exists between two objects.

        Determines whether a membership relationship exists between the specified
        parent and child objects within the given collection.

        Parameters
        ----------
        parent_object_name : str
            Name of the parent object
        child_object_name : str
            Name of the child object
        parent_class : ClassEnum
            Class enumeration of the parent object
        child_class : ClassEnum
            Class enumeration of the child object
        collection : CollectionEnum
            Collection enumeration defining the relationship type

        Returns
        -------
        bool
            True if the membership exists, False otherwise

        See Also
        --------
        add_membership : Add a membership between two objects
        get_membership_id : Get the ID for an existing membership
        get_object_id : Get the ID for an object
        get_collection_id : Get the ID for a collection

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Region, "Region1")
        >>> db.add_object(ClassEnum.Node, "Node1")
        >>> db.add_membership(
        ...     parent_class_enum=ClassEnum.Region,
        ...     child_class_enum=ClassEnum.Node,
        ...     parent_object_name="Region1",
        ...     child_object_name="Node1",
        ...     collection_enum=CollectionEnum.ReferenceNode,
        ... )
        >>> db.check_membership_exists(
        ...     "Region1",
        ...     "Node1",
        ...     parent_class=ClassEnum.Region,
        ...     child_class=ClassEnum.Node,
        ...     collection=CollectionEnum.ReferenceNode,
        ... )
        True
        >>> db.check_membership_exists(
        ...     "Region1",
        ...     "Node100",
        ...     parent_class=ClassEnum.Region,
        ...     child_class=ClassEnum.Node,
        ...     collection=CollectionEnum.ReferenceNode,
        ... )
        False
        """
        # Validate classes and collection exist - raise NotFoundError if not
        # This is a programming error if passing non-existent classes
        if not self.check_class_exists(parent_class):
            msg = (
                f"Parent class '{parent_class}' does not exist. "
                "Cannot check membership for non-existent parent class. "
                "Use `list_classes()` to see available classes."
            )
            raise NotFoundError(msg)

        if not self.check_class_exists(child_class):
            msg = (
                f"Child class '{child_class}' does not exist. "
                "Cannot check membership for non-existent child class. "
                "Use `list_classes()` to see available classes."
            )
            raise NotFoundError(msg)

        if not self.check_collection_exists(collection, parent_class=parent_class, child_class=child_class):
            msg = (
                f"Collection '{collection}' does not exist for "
                f"parent_class={parent_class} and child_class={child_class}. "
                "Check available collections using `list_collections()`"
            )
            raise NotFoundError(msg)

        # Now try to get object IDs - if objects don't exist, return False
        # (that's what we're checking for)
        parent_object_id = self.get_object_id(parent_class, parent_object_name)
        child_object_id = self.get_object_id(child_class, child_object_name)
        collection_id = self.get_collection_id(collection, parent_class, child_class)

        query = """
        SELECT 1 FROM t_membership
        WHERE parent_object_id = ?
        AND child_object_id = ?
        AND collection_id = ?
        """
        result = bool(self._db.query(query, (parent_object_id, child_object_id, collection_id)))
        return bool(result)

    def check_object_exists(
        self, class_enum: ClassEnum, /, name: str, *, category: str | None = None
    ) -> bool:
        """Check if an object exists in the database.

        Determines whether an object with the given name and class exists,
        optionally filtered by category.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration of the object
        name : str
            Name of the object to check
        category : str | None, optional
            Category name to filter by, by default None

        Returns
        -------
        bool
            True if the object exists (and matches category if specified), False otherwise

        Raises
        ------
        NotFoundError
            If class_enum does not exist in the database. This indicates a programming
            error - you cannot check objects for a non-existent class.

        See Also
        --------
        get_class_id : Get the ID for a class
        get_object_id : Get the ID for an object
        add_object : Add an object to the database
        check_class_exists : Check if a class exists
        check_category_exists : Check if a category exists

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Generator, "TestObject")
        >>> db.check_object_exists(ClassEnum.Generator, "TestObject")
        True
        >>> db.check_object_exists(ClassEnum.Generator, "NonExistent")
        False
        >>> # Check with category
        >>> db.add_object(ClassEnum.Generator, "Gen2", category="Thermal")
        >>> db.check_object_exists(ClassEnum.Generator, "Gen2", category="Thermal")
        True
        >>> db.check_object_exists(ClassEnum.Generator, "Gen2", category="Hydro")
        False
        """
        if not self.check_class_exists(class_enum):
            msg = (
                f"Class '{class_enum}' does not exist. "
                "Cannot check object for non-existent class. "
                "Use `list_classes()` to see available classes."
            )
            raise NotFoundError(msg)

        class_id = self.get_class_id(class_enum)

        # Build query based on whether category is specified
        if category is None:
            query = f"SELECT 1 FROM {Schema.Objects.name} WHERE name = ? AND class_id = ?"
            params: tuple[str, int] | tuple[str, int, str] = (name, class_id)
        else:
            # If category is specified, join with categories table
            query = f"""
            SELECT 1 FROM {Schema.Objects.name} obj
            JOIN {Schema.Categories.name} cat ON obj.category_id = cat.category_id
            WHERE obj.name = ? AND obj.class_id = ? AND cat.name = ?
            """
            params = (name, class_id, category)

        return bool(self._db.query(query, params))

    def check_property_exists(
        self,
        collection_enum: CollectionEnum,
        /,
        object_class: ClassEnum,
        property_names: str | Iterable[str],
        *,
        parent_class: ClassEnum | None = None,
    ) -> bool:
        """Check if properties exist for a specific collection and class.

        Verifies that all specified property names are valid for the given collection and class.

        Parameters
        ----------
        collection_enum : CollectionEnum
            Collection enumeration the properties should belong to
        object_class : ClassEnum
            Class enumeration of the object
        property_names : str | Iterable[str]
            Property name or names to check
        parent_class : ClassEnum | None, optional
            Class enumeration of the parent object, by default None

        Returns
        -------
        bool
            True if all properties exist, False otherwise

        See Also
        --------
        list_valid_properties : Get list of valid property names for a collection
        normalize_names : Normalize property names for checking

        Notes
        -----
        If any property in the list is invalid, the function returns False and logs
        the invalid properties.

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.check_property_exists(CollectionEnum.Generators, ClassEnum.Generator, "Max Capacity")
        True
        >>> db.check_property_exists(CollectionEnum.Generators, ClassEnum.Generator, ["Invalid Property"])
        False
        """
        # Validate parent class exists (if specified)
        if parent_class and not self.check_class_exists(parent_class):
            msg = (
                f"Parent class '{parent_class}' does not exist. "
                "Cannot check properties for non-existent parent class. "
                "Use `list_classes()` to see available classes."
            )
            raise NotFoundError(msg)

        # Validate object class exists
        if not self.check_class_exists(object_class):
            msg = (
                f"Child class '{object_class}' does not exist. "
                "Cannot check properties for non-existent child class. "
                "Use `list_classes()` to see available classes."
            )
            raise NotFoundError(msg)

        # Validate collection exists
        if not self.check_collection_exists(
            collection_enum, parent_class=parent_class or ClassEnum.System, child_class=object_class
        ):
            msg = (
                f"Collection '{collection_enum}' does not exist for "
                f"parent_class={parent_class or ClassEnum.System} and child_class={object_class}. "
                "Check available collections using `list_collections()`"
            )
            raise NotFoundError(msg)

        property_names = normalize_names(property_names)
        valid_props = self.list_valid_properties(
            collection_enum,
            parent_class_enum=parent_class or ClassEnum.System,
            child_class_enum=object_class,
        )
        invalid = [prop for prop in property_names if prop not in valid_props]
        if invalid:
            logger.error("Invalid properties {} for collection {}", property_names, collection_enum)
            return False
        return True

    def check_scenario_exists(self, name: str) -> bool:
        """Check if a scenario exists in the database.

        Determines whether a scenario with the given name exists.

        Parameters
        ----------
        name : str
            Name of the scenario to check

        Returns
        -------
        bool
            True if the scenario exists, False otherwise

        See Also
        --------
        get_class_id : Get the ID for a class
        ClassEnum.Scenario : Scenario class enumeration

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object("Base Scenario", ClassEnum.Scenario)
        >>> db.check_scenario_exists("Base Scenario")
        True
        >>> db.check_scenario_exists("Nonexistent Scenario")
        False
        """
        query = f"SELECT 1 FROM {Schema.Objects.name} WHERE name = ? AND class_id = ?"
        class_id = self.get_class_id(ClassEnum.Scenario)
        return bool(self._db.query(query, (name, class_id)))

    def copy_object(
        self,
        object_class: ClassEnum,
        original_object_name: str,
        new_object_name: str,
        copy_properties: bool = True,
    ) -> int:
        """Copy an object and its properties, tags, and texts."""
        object_id = self.get_object_id(object_class, name=original_object_name)
        category_id = self.query("SELECT category_id from t_object WHERE object_id = ?", (object_id,))
        category = self.query("SELECT name from t_category WHERE category_id = ?", (category_id[0][0],))
        new_object_id = self.add_object(object_class, new_object_name, category=category[0][0])
        membership_mapping = self.copy_object_memberships(
            object_class=object_class, original_name=new_object_name, new_name=new_object_name
        )

        # If we do not find a membership, we just look for the system membership
        if not membership_mapping:
            membership_mapping = {}
            system_membership_id = self.list_object_memberships(object_class, original_object_name)[0][
                "membership_id"
            ]
            new_membership_id = self.list_object_memberships(object_class, new_object_name)[0][
                "membership_id"
            ]
            membership_mapping[system_membership_id] = new_membership_id

        data_ids = self.get_object_data_ids(object_class, name=original_object_name)
        if not data_ids and copy_properties:
            logger.debug(f"No properties found for {original_object_name}")
            return new_object_id

        self._copy_object_properties(membership_mapping=membership_mapping)

        return new_object_id

    def copy_object_memberships(
        self, object_class: ClassEnum, original_name: str, new_name: str
    ) -> dict[int, int]:
        """Copy all existing memberships of old object to the new object."""
        membership_mapping: dict[int, int] = {}
        all_memberships = self.list_object_memberships(
            object_class, original_name, exclude_system_membership=True
        )
        for membership in all_memberships:
            parent_name = membership["parent_name"]
            child_name = membership["child_name"]
            parent_class = ClassEnum[membership["parent_class_name"]]
            child_class = ClassEnum[membership["child_class_name"]]
            collection = CollectionEnum[membership["collection_name"]]

            # Determine if original object was parent or child
            if child_name == original_name:
                # Original object is child, new object will be child
                old_id = self.get_membership_id(parent_name, original_name, collection)
                try:
                    new_id = self.add_membership(parent_class, child_class, parent_name, new_name, collection)
                    membership_mapping[old_id] = new_id
                except Exception as e:
                    logger.warning(f"Could not create child membership: {e}")

            elif parent_name == original_name:
                # Original object is parent, new object will be parent
                old_id = self.get_membership_id(original_name, child_name, collection)
                try:
                    new_id = self.add_membership(parent_class, child_class, new_name, child_name, collection)
                    membership_mapping[old_id] = new_id
                except Exception as e:
                    logger.warning(f"Could not create parent membership: {e}")
        if not membership_mapping:
            msg = "`{}.{}` do not have any memberships."
            logger.warning(msg, object_class, original_name)
        return membership_mapping

    def _copy_object_properties(self, membership_mapping: dict[int, int]):
        """Copy all property data from original object to new object efficiently.

        Parameters
        ----------
        membership_mapping : dict[int, int]
            Mapping from original membership IDs to new membership IDs

        Returns
        -------
        bool
            True if successful
        """
        if not membership_mapping:
            return True

        with self._db.transaction():
            self._db.execute("DROP TABLE IF EXISTS temp_mapping")
            self._db.execute("CREATE TEMPORARY TABLE temp_mapping (old_id INTEGER, new_id INTEGER)")

            for old_id, new_id in membership_mapping.items():
                self._db.execute("INSERT INTO temp_mapping VALUES (?, ?)", (old_id, new_id))

            self._db.execute("""
            INSERT INTO t_data (membership_id, property_id, value, state)
                SELECT
                    tm.new_id, d.property_id, d.value, d.state
                FROM
                    t_data d
                JOIN temp_mapping tm ON d.membership_id = tm.old_id
            """)

            self._db.execute("DROP TABLE IF EXISTS temp_data_mapping")
            self._db.execute("CREATE TEMPORARY TABLE temp_data_mapping (old_id INTEGER, new_id INTEGER)")

            self._db.execute("""
                INSERT INTO temp_data_mapping
                SELECT old_d.data_id AS old_id, new_d.data_id AS new_id
                FROM t_data old_d
                JOIN temp_mapping tm ON old_d.membership_id = tm.old_id
                JOIN t_data new_d ON
                    new_d.membership_id = tm.new_id AND
                    new_d.property_id = old_d.property_id AND
                    new_d.value = old_d.value
                WHERE new_d.data_id NOT IN (SELECT data_id FROM t_tag)
            """)

            # Copy tags using data ID mapping
            self._db.execute("""
                INSERT INTO t_tag (data_id, object_id, state, action_id)
                SELECT tdm.new_id, t.object_id, t.state, t.action_id
                FROM t_tag t
                JOIN temp_data_mapping tdm ON t.data_id = tdm.old_id
            """)

            # Copy text data
            self._db.execute("""
                INSERT INTO t_text (data_id, class_id, value, state, action_id)
                SELECT tdm.new_id, t.class_id, t.value, t.state, t.action_id
                FROM t_text t
                JOIN temp_data_mapping tdm ON t.data_id = tdm.old_id
            """)

            # Copy band data
            self._db.execute("""
                INSERT INTO t_band (data_id, band_id, state)
                SELECT tdm.new_id, b.band_id, b.state
                FROM t_band b
                JOIN temp_data_mapping tdm ON b.data_id = tdm.old_id
            """)

            self._db.execute("DROP TABLE IF EXISTS temp_mapping")
            self._db.execute("DROP TABLE IF EXISTS temp_data_mapping")
        return True

    def create_object_scenario(
        self,
        object_name: str,
        scenario_name: str,
        properties: dict[str, Any],
        /,
        *,
        object_class: ClassEnum,
        description: str | None = None,
        collection: CollectionEnum | None = None,
        parent_class: ClassEnum | None = None,
        base_scenario: str | None = None,
    ) -> int:
        """Create a new scenario with specific property values for an object."""
        raise NotImplementedError

    def create_schema(self, schema: str | None = None) -> bool:
        """Create database schema from SQL script.

        Initializes the database schema by executing SQL statements, either from
        the default schema or from a provided schema string.

        Parameters
        ----------
        schema : str | None, optional
            Direct SQL schema content to execute. If None, uses the default schema,
            by default None

        Returns
        -------
        bool
            True if the creation succeeded, False if it failed

        See Also
        --------
        _db.executescript : Execute a SQL script in the database
        PLEXOS_DEFAULT_SCHEMA : Default schema SQL content

        Notes
        -----
        This is typically the first method called after initializing a new PlexosDB
        instance, as it sets up all the required tables for the database.

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        True
        """
        if not schema:
            logger.debug("Using default schema")
            return self._db.executescript(PLEXOS_DEFAULT_SCHEMA)
        return self._db.executescript(schema)

    def delete_attribute(
        self,
        attribute_name: str,
        /,
        *,
        object_name: str,
        object_class: ClassEnum,
    ) -> None:
        """Delete an attribute from an object."""
        raise NotImplementedError

    def delete_category(self, category: str, /, *, class_name: ClassEnum) -> None:
        """Delete a category from the database."""
        raise NotImplementedError

    def delete_membership(
        self,
        parent_object_name: str,
        child_object_name: str,
        /,
        *,
        parent_class: ClassEnum,
        child_class: ClassEnum,
        collection: CollectionEnum,
    ) -> None:
        """Delete a membership between two objects."""
        raise NotImplementedError

    def delete_metadata(
        self,
        entity_type: Literal["object", "membership", "data"],
        entity_id: int,
        class_name: str,
        property_name: str,
    ) -> None:
        """Delete metadata from an entity."""
        raise NotImplementedError

    def delete_object(self, class_enum: ClassEnum, /, *, name: str) -> None:
        """Delete an object and its memberships from the database.

        Default behaviour is to remove all the references of the object including memberships and data.
        """
        object_id = self.get_object_id(class_enum, name=name)
        delete_query = "DELETE FROM t_object WHERE object_id = ?"

        # Handle delete in transaction in case an error happens.
        with self._db.transaction():
            self._db.execute(delete_query, (object_id,))
        return

    def delete_property(
        self,
        object_class: ClassEnum,
        object_name: str,
        /,
        *,
        property_name: str,
        collection: CollectionEnum | None = None,
        parent_class: ClassEnum | None = None,
        scenario: str | None = None,
    ) -> None:
        """Delete a property from an object.

        Removes a specific property from an object, including all associated data
        such as tags (scenarios), text data, and date ranges. If a scenario is
        specified, only the property data associated with that scenario is deleted.

        Parameters
        ----------
        object_class : ClassEnum
            Class enumeration of the object
        object_name : str
            Name of the object containing the property
        property_name : str
            Name of the property to delete
        collection : CollectionEnum | None, optional
            Collection enumeration for the property. If None, uses the default
            collection for the object class, by default None
        parent_class : ClassEnum | None, optional
            Parent class enumeration for the property. If None, defaults to
            ClassEnum.System, by default None
        scenario : str | None, optional
            Name of the scenario to filter by. If specified, only deletes
            property data associated with this scenario, by default None

        Raises
        ------
        NotFoundError
            If the object does not exist
            If the property does not exist for the object
            If the specified scenario does not exist

        See Also
        --------
        delete_object : Delete an entire object and its properties
        add_property : Add a property to an object
        get_object_properties : Get properties for an object

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Generator, "Generator1")
        >>> db.add_property(ClassEnum.Generator, "Generator1", "Max Capacity", 100.0)
        >>> db.delete_property(ClassEnum.Generator, "Generator1", property_name="Max Capacity")
        """
        # Ensure object exists
        if not self.check_object_exists(object_class, object_name):
            msg = f"Object = `{object_name}` does not exist for class `{object_class}`."
            raise NotFoundError(msg)

        # Set defaults
        collection = collection or get_default_collection(object_class)
        parent_class = parent_class or ClassEnum.System

        # Validate property exists for this collection
        valid_properties = self.list_valid_properties(
            collection, child_class_enum=object_class, parent_class_enum=parent_class
        )
        if property_name not in valid_properties:
            msg = (
                f"Property '{property_name}' does not exist for collection: {collection}. "
                f"Run `self.list_valid_properties({collection})` to verify valid properties."
            )
            raise NameError(msg)

        # Get IDs for the property lookup
        property_id = self.get_property_id(
            property_name,
            parent_class_enum=parent_class,
            child_class_enum=object_class,
            collection_enum=collection,
        )

        # For parent object name, default to "System" if not specified
        parent_object_name = "System"  # This matches the pattern used in add_property
        membership_id = self.get_membership_id(parent_object_name, object_name, collection)

        # Build the delete query
        if scenario is not None:
            # Delete only property data associated with the specific scenario
            if not self.check_scenario_exists(scenario):
                msg = f"Scenario '{scenario}' does not exist."
                raise NotFoundError(msg)

            scenario_id = self.get_scenario_id(scenario)

            # Find data_ids that match the property and are tagged with the scenario
            find_data_query = """
            SELECT d.data_id
            FROM t_data d
            JOIN t_tag t ON d.data_id = t.data_id
            WHERE d.membership_id = ? AND d.property_id = ? AND t.object_id = ?
            """
            data_results = self._db.fetchall(find_data_query, (membership_id, property_id, scenario_id))

            if not data_results:
                msg = f"Property '{property_name}' with scenario '{scenario}' "
                msg += f"not found for object '{object_name}'."
                raise NotFoundError(msg)

            # Delete the data records (cascade will handle related tables)
            data_ids = [row[0] for row in data_results]
            placeholders = ",".join("?" * len(data_ids))
            delete_query = f"DELETE FROM t_data WHERE data_id IN ({placeholders})"

            with self._db.transaction():
                self._db.execute(delete_query, tuple(data_ids))
        else:
            # Delete all property data for this object/property combination
            find_data_query = """
            SELECT d.data_id
            FROM t_data d
            WHERE d.membership_id = ? AND d.property_id = ?
            """
            data_results = self._db.fetchall(find_data_query, (membership_id, property_id))

            if not data_results:
                msg = f"Property '{property_name}' not found for object '{object_name}'."
                raise NotFoundError(msg)

            # Delete the data records (cascade will handle related tables)
            delete_query = "DELETE FROM t_data WHERE membership_id = ? AND property_id = ?"

            with self._db.transaction():
                self._db.execute(delete_query, (membership_id, property_id))

    def delete_text(
        self,
        data_id: int,
        /,
        *,
        class_id: int,
    ) -> None:
        """Delete text data from a property data record."""
        raise NotImplementedError

    def get_attribute(
        self,
        object_class: ClassEnum,
        /,
        *,
        object_name: str,
        attribute_name: str,
    ) -> dict:
        """Get attribute details for a specific object."""
        query = """
        SELECT
            t_attribute_data.value
        FROM
            t_attribute_data
        WHERE
            t_attribute_data.attribute_id = ?
        AND t_attribute_data.object_id = ?
        """
        object_id = self.get_object_id(object_class, name=object_name)
        attribute_id = self.get_attribute_id(object_class, name=attribute_name)

        result = self._db.fetchone(query, (attribute_id, object_id))
        assert result
        return result

    def get_attribute_id(self, class_enum: ClassEnum, /, name: str) -> int:
        """Return the ID for a given attribute.

        Retrieves the unique identifier for an attribute with the specified name and class.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration the category belongs to
        name : str
            Name of the attribute

        Returns
        -------
        int
            ID of the category

        Raises
        ------
        AssertionError
            If the category does not exist
        """
        query = """
        SELECT
            attribute_id
        FROM
            t_attribute
        LEFT JOIN
            t_class ON t_class.class_id = t_attribute.class_id
        WHERE
            t_attribute.name = ?
        AND t_class.name = ?
        """
        result = self._db.fetchone(query, (name, class_enum))
        assert result
        return result[0]

    def get_attributes(
        self,
        object_name: str,
        /,
        *,
        object_class: ClassEnum,
        attribute_names: list[str] | None = None,
    ) -> list[dict]:
        """Get all attributes for a specific object."""
        raise NotImplementedError

    def get_category_id(self, class_enum: ClassEnum, /, name: str) -> int:
        """Return the ID for a given category.

        Retrieves the unique identifier for a category with the specified name and class.

        Parameters
        ----------
        category : str
            Name of the category
        class_enum : ClassEnum
            Class enumeration the category belongs to

        Returns
        -------
        int
            ID of the category

        Raises
        ------
        AssertionError
            If the category does not exist

        See Also
        --------
        check_category_exists : Check if a category exists
        add_category : Add a new category

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_category(ClassEnum.Generator, "my_category")
        >>> db.get_category_id(ClasEnum.Generator, "my_category")
        1
        """
        query = """
        SELECT
            category_id
        FROM
            t_category
        LEFT JOIN
            t_class ON t_class.class_id = t_category.class_id
        WHERE
            t_category.name = ?
        AND t_class.name = ?
        """
        result = self._db.fetchone(query, (name, class_enum))
        if not result:
            msg = f"Category = `{name}` not found on the database."
            raise NotFoundError(msg)
        return result[0]

    def get_category_max_id(self, class_enum: ClassEnum) -> int:
        """Return the current maximum rank for a given category class.

        Determines the highest rank value currently assigned to any category
        of the specified class, useful for adding new categories with unique ranks.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration to check

        Returns
        -------
        int
            Maximum rank value for the class's categories

        Raises
        ------
        AssertionError
            If the query fails

        See Also
        --------
        add_category : Add a new category

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_category("Category1", ClassEnum.Generator)
        >>> db.get_category_max_id(ClassEnum.Generator)
        1
        """
        query = """
        SELECT
            max(rank) AS rank
        FROM
            t_category
        LEFT JOIN
            t_class ON t_class.class_id = t_category.class_id
        WHERE
            t_class.name = ?
        """
        result = self._db.fetchone(query, (class_enum,))
        assert result
        return result[0]

    def get_class_id(self, class_enum: ClassEnum) -> int:
        """Return the ID for a given class.

        Retrieves the unique identifier for the specified class enumeration.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration to get the ID for

        Returns
        -------
        int
            ID of the class

        Raises
        ------
        AssertionError
            If the class does not exist

        See Also
        --------
        ClassEnum : Enumeration of available classes

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.get_class_id(ClassEnum.Generator)
        15  # Example ID
        """
        query = f"SELECT class_id FROM {Schema.Class.name} WHERE name = ?"
        result = self._db.fetchone(query, (class_enum,))
        assert result
        return result[0]

    def get_collection_id(
        self,
        collection: CollectionEnum,
        /,
        parent_class_enum: ClassEnum,
        child_class_enum: ClassEnum,
    ) -> int:
        """Return the ID for a given collection.

        Retrieves the unique identifier for a collection based on its name and associated
        parent and child classes.

        Parameters
        ----------
        collection : CollectionEnum
            Collection enumeration to get the ID for
        parent_class_enum : ClassEnum
            Parent class enumeration for the collection
        child_class_enum : ClassEnum
            Child class enumeration for the collection

        Returns
        -------
        int
            ID of the collection

        Raises
        ------
        AssertionError
            If the collection does not exist with the specified parent and child classes

        See Also
        --------
        CollectionEnum : Enumeration of available collections
        get_class_id : Get the ID for a class

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.get_collection_id(CollectionEnum.SystemGenerators, ClassEnum.System, ClassEnum.Generator)
        25  # Example ID
        """
        query = """
        SELECT
            collection_id
        FROM t_collection as collection
        LEFT JOIN t_class as parent_class ON collection.parent_class_id = parent_class.class_id
        LEFT JOIN t_class as child_class ON collection.child_class_id = child_class.class_id
        WHERE
            collection.name = ?
        AND parent_class.name = ?
        AND child_class.name = ?
        """
        result = self._db.fetchone(query, (collection, parent_class_enum, child_class_enum))
        assert result
        return result[0]

    def get_config(self, element: str | None = None) -> dict | list[dict]:
        """Get configuration values from the database."""
        raise NotImplementedError

    def get_custom_columns(self, class_enum: ClassEnum | None = None) -> list[dict]:
        """Get custom columns, optionally filtered by class."""
        raise NotImplementedError

    def get_membership_id(
        self,
        /,
        parent_name: str,
        child_name: str,
        collection: CollectionEnum,
    ) -> int:
        """Return the ID for a given membership.

        Retrieves the unique identifier for a membership between parent and child objects
        in the specified collection.

        Parameters
        ----------
        parent_name : str
            Name of the parent object
        child_name : str
            Name of the child object
        collection : CollectionEnum
            Collection enumeration defining the relationship type

        Returns
        -------
        int
            ID of the membership

        Raises
        ------
        AssertionError
            If the membership does not exist

        See Also
        --------
        add_membership : Add a new membership between objects

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.System, "System")
        >>> db.add_object(ClassEnum.Generator, "Generator1")
        >>> db.add_membership(
        ...     "System", "Generator1", ClassEnum.System, ClassEnum.Generator, CollectionEnum.SystemGenerators
        ... )
        >>> db.get_membership_id("System", "Generator1", CollectionEnum.SystemGenerators)
        1
        """
        query = f"""
        SELECT
            membership_id
        FROM {Schema.Memberships.name} as memberships
        LEFT JOIN t_object as parent_object ON parent_object.object_id = memberships.parent_object_id
        LEFT JOIN t_object as child_object ON child_object.object_id = memberships.child_object_id
        LEFT JOIN t_collection as collection ON collection.collection_id = memberships.collection_id
        WHERE
            parent_object.name = ?
        AND child_object.name = ?
        AND collection.name = ?
        """
        result = self._db.fetchone(query, (parent_name, child_name, collection))
        assert result
        return result[0]

    def list_object_memberships(
        self,
        class_enum: ClassEnum,
        /,
        name: str,
        category: str | None = None,
        collection: CollectionEnum | None = None,
        exclude_system_membership: bool = False,
    ) -> list[dict[str, Any]]:
        """Retrieve all memberships for a given object.

        By default it returns the system membership.

        Parameters
        ----------
        class_enum : ClassEnum
            Class of the object
        name : str
            Name of the object to get its memberships.
        collection : CollectionEnum | None, optional
            Collection to filter memberships.
        exclude_system_membership : bool, optional
            If True, exclude system memberships.

        Returns
        -------
        list[dict]
            A list of dicts representing memberships. Each tuple is structured as:
            (parent_class_id, child_class_id, parent_object_name, child_object_name, collection_id,
            return self.query(query_string=query_string, params=params)
            parent_class_name, collection_name).

        Raises
        ------
        KeyError
            If any of the object_names do not exist.
        """
        object_id = self.get_object_id(class_enum, name=name)
        query_string = """
        SELECT
            mem.membership_id,
            parent_object.name AS parent_name,
            child_object.name AS child_name,
            parent_class.name AS parent_class_name,
            child_class.name AS child_class_name,
            collections.name AS collection_name
        FROM
            t_membership AS mem
        INNER JOIN
            t_object AS parent_object ON mem.parent_object_id = parent_object.object_id
        INNER JOIN
            t_object AS child_object ON mem.child_object_id = child_object.object_id
        LEFT JOIN
            t_class AS parent_class ON mem.parent_class_id = parent_class.class_id
        LEFT JOIN
            t_class AS child_class ON mem.child_class_id = child_class.class_id
        LEFT JOIN
            t_collection AS collections ON mem.collection_id = collections.collection_id
        """

        query_conditions = []
        params: dict[str, int | float | str] = {}

        # We first add conditions for looking the object.
        query_conditions.append(
            "(child_object.object_id = :object_id OR parent_object.object_id = :object_id)"
        )
        params["object_id"] = object_id

        # Query filtering
        if exclude_system_membership:
            query_conditions.append("parent_class.name <> :parent_class_name")
            params["parent_class_name"] = ClassEnum.System.name
        if collection:
            query_conditions.append("collections.name = :collection_name")
            params["collection_name"] = collection.name

        query_string += " WHERE " + " AND ".join(query_conditions)

        return self._db.fetchall_dict(query_string, params=params)

    def get_memberships_system(
        self,
        *object_names: Iterable[str] | str,
        object_class: ClassEnum,
        category: str | None = None,
        collection: CollectionEnum | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve system memberships for the given object(s).

        Parameters
        ----------
        object_names : str or list[str]
            Name or list of names of the objects to get their memberships.
            You can pass multiple string arguments or a single list of strings.
        object_class : ClassEnum
            Class of the objects.
        collection : CollectionEnum | None, optional
            Collection to filter memberships.

        Returns
        -------
        list[tuple]
            A list of tuples representing memberships of the object to the system.

        Raises
        ------
        KeyError
            If any of the object_names do not exist.
        """
        names = normalize_names(*object_names)
        object_ids = tuple(self.get_object_id(object_class, name=name, category=category) for name in names)
        query_string = """
            SELECT
                mem.membership_id,
                mem.child_class_id,
                child_object.name AS name,
                mem.collection_id,
                child_class.name AS class,
                collections.name AS collection_name
            FROM
                t_membership AS mem
            INNER JOIN
                t_object AS parent_object ON mem.parent_object_id = parent_object.object_id
            INNER JOIN
                t_object AS child_object ON mem.child_object_id = child_object.object_id
            LEFT JOIN
                t_class AS parent_class ON mem.parent_class_id = parent_class.class_id
            LEFT JOIN
                t_class AS child_class ON mem.child_class_id = child_class.class_id
            LEFT JOIN
                t_collection AS collections ON mem.collection_id = collections.collection_id
            """
        conditions = []
        if len(object_ids) == 1:
            conditions.append(
                f"(child_object.object_id = {object_ids[0]} OR parent_object.object_id = {object_ids[0]})"
            )
        else:
            conditions.append(
                f"(child_object.object_id in {object_ids} OR parent_object.object_id in {object_ids})"
            )
        parent_class = ClassEnum.System
        if collection:
            conditions.append(
                f"parent_class.name = '{parent_class.value}' and collections.name = '{collection.value}'"
            )
        if conditions:
            query_string += " WHERE " + " AND ".join(conditions)
        return self._db.fetchall_dict(query_string)

    def get_metadata(
        self,
        entity_type: Literal["object", "membership", "data"],
        entity_id: int,
        /,
        *,
        class_name: str | None = None,
        property_name: str | None = None,
    ) -> list[dict]:
        """Retrieve metadata for an entity."""
        raise NotImplementedError

    def get_object_data_ids(
        self,
        class_enum: ClassEnum,
        /,
        name: str,
        property_names: str | Iterable[str] | None = None,
        *,
        parent_class_enum: ClassEnum = ClassEnum.System,
        collection_enum: CollectionEnum | None = None,
        category: str | None = None,
    ):
        """Get all the data_id values for a given object in the database.

        Retrieves all data IDs that match the specified criteria for an object,
        optionally filtered by property names, parent class, collection, and category.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration of the object
        name : str
            Name of the object to retrieve data IDs for
        property_names : str | Iterable[str] | None, optional
            Names of specific properties to filter by, by default None
        parent_class_enum : ClassEnum, optional
            Parent class enumeration, by default ClassEnum.System
        collection_enum : CollectionEnum | None, optional
            Collection enumeration to filter by, by default None
            (if not specified, the default collection for the class is used)
        category : str | None, optional
            Category to filter by, by default None

        Returns
        -------
        list[int]
            List of data_id values that match the criteria

        Raises
        ------
        KeyError
            If the specified category does not exist
        NameError
            If any specified property does not exist for the collection

        See Also
        --------
        check_property_exists : Check if properties exist for a collection
        check_category_exists : Check if a category exists
        list_valid_properties : List valid property names for a collection

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Generator, "Generator1")
        >>> db.add_property(ClassEnum.Generator, "Generator1", "Max Capacity", 100.0)
        >>> db.get_object_data_ids(ClassEnum.Generator, "Generator1")
        [1]
        """
        params = [name]
        filters = "o.name = ?"
        if not collection_enum:
            collection_enum = get_default_collection(class_enum)
        if category:
            if not self.check_category_exists(class_enum, category):
                raise KeyError
            filters += " AND cat.name = ?"
            params.append(category)

        if property_names:
            if not self.check_property_exists(
                collection_enum,
                class_enum,
                property_names,
                parent_class=parent_class_enum,
            ):
                msg = (
                    f"Invalid property {property_names} for {collection_enum=}."
                    " Use `list_valid_properties` to check property requested."
                )
                raise NameError(msg)
            properties = normalize_names(property_names)
            prop_placeholders = ", ".join("?" for _ in properties)
            filters += f" AND p.name IN ({prop_placeholders})"
            params.extend(properties)
        query = f"""
        SELECT
            d.data_id
        FROM t_object o
            JOIN t_class c ON o.class_id = c.class_id
            JOIN t_category cat ON o.category_id = cat.category_id
            JOIN t_membership m ON m.child_object_id = o.object_id
            JOIN t_data d ON d.membership_id = m.membership_id
            JOIN t_property p ON d.property_id = p.property_id
        WHERE
            {filters}
        ORDER BY
            d.data_id
        """
        result = self._db.query(query, tuple(params))
        assert result
        return [row[0] for row in result]

    def get_object_properties(
        self,
        class_enum: ClassEnum,
        /,
        name: str,
        property_names: str | Iterable[str] | None = None,
        *,
        parent_class_enum: ClassEnum | None = None,
        collection_enum: CollectionEnum | None = None,
        category: str | None = None,
    ) -> list[PropertyRecord]:
        """Retrieve properties for a specific object.

        Gets all properties for the specified object, optionally filtered by
        property names, collection, and category.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration of the object
        name : str
            Name of the object to retrieve properties for
        property_names : str | Iterable[str] | None, optional
            Names of specific properties to retrieve. If None, gets all properties, by default None
        parent_class_enum : ClassEnum | None, optional
            Parent class enumeration for filtering properties, by default None
            (defaults to ClassEnum.System if not specified)
        collection_enum : CollectionEnum | None, optional
            Collection enumeration to filter properties by, by default None
            (if not specified, the default collection for the class is used)
        category : str | None, optional
            Category to filter by, by default None

        Returns
        -------
        list[PropertyRecord]
            List of dictionaries containing property information.
            See PropertyRecord TypedDict for full field definitions including:
            name, property, value, unit, category, scenario_name, etc.

        Raises
        ------
        NoPropertiesError
            If the specified object does not have any properties
        NameError
            If the specified property does not exist for the collection
        KeyError
            If the specified category does not exist

        See Also
        --------
        iterate_properties : Iterate through properties with efficient memory handling
        check_property_exists : Check if properties exist for a collection
        list_valid_properties : List valid property names for a collection
        has_properties : Check if an object has any properties

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Generator, "Generator1")
        >>> db.add_property(ClassEnum.Generator, "Generator1", "Max Capacity", 100.0)
        >>> properties = db.get_object_properties(ClassEnum.Generator, "Generator1")
        >>> properties[0]["property"]
        'Max Capacity'
        >>> properties[0]["value"]
        100.0
        """
        parent_class_enum = parent_class_enum or ClassEnum.System

        if not self.has_properties(class_enum, name, collection_enum=collection_enum, category=category):
            msg = f"Object = `{name}` does not have any properties attached to it."
            raise NoPropertiesError(msg)

        return list(
            self.iterate_properties(
                class_enum=class_enum,
                object_names=name,
                property_names=property_names,
                collection=collection_enum,
                parent_class=parent_class_enum,
                category=category,
            )
        )

    def get_object_id(self, class_enum: ClassEnum, /, name: str, *, category: str | None = None) -> int:
        """Return the ID for a given object.

        Retrieves the unique identifier for an object with the specified name and class,
        optionally filtering by category.

        Parameters
        ----------
        object_name : str
            Name of the object
        class_enum : ClassEnum
            Class enumeration of the object
        category : str | None, optional
            Category name to filter by, by default None

        Returns
        -------
        int
            ID of the object

        Raises
        ------
        AssertionError
            If the object does not exist
        KeyError
            If no object matches the criteria

        See Also
        --------
        check_object_exists : Check if an object exists
        add_object : Add a new object to the database
        get_category_id : Get the ID for a category

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Generator, "Generator1" category="Thermal")
        >>> db.get_object_id("Generator1", ClassEnum.Generator)
        1
        """
        query = f"""
        SELECT
            object_id
        FROM
            {Schema.Objects.name} as obj
        LEFT JOIN
            t_class ON t_class.class_id = obj.class_id
        WHERE
            obj.name = ?
        AND
            t_class.name = ?
        """
        params: list[Any] = [name, class_enum]
        if category:
            category_id = self.get_category_id(class_enum, category)
            params.append(category_id)
            query += "AND obj.category_id = ?"
        result = self._db.fetchone(query, tuple(params))
        if not result:
            msg = f"Object = {name} not found on the database."
            raise NotFoundError(msg)
        assert result
        return result[0]

    def get_objects_id(
        self,
        *object_names: Iterable[str] | str,
        class_enum: ClassEnum,
    ) -> list[int]:
        """Get object_ids for a list of names for a given class."""
        names = normalize_names(*object_names)
        class_id = self.get_class_id(class_enum)
        placeholders = ", ".join("?" for _ in names)
        query = f"""
        SELECT
            object_id
        FROM {Schema.Objects.name}
        WHERE
            name in ({placeholders})
        AND
            class_id = {class_id}
        """
        result = self._db.fetchall(query, tuple(names))
        assert result
        return [r[0] for r in result]

    def get_plexos_version(self) -> tuple[int, ...] | None:
        """Return the version information of the PLEXOS model."""
        return self.version

    def get_property_id(
        self,
        property_name: str,
        /,
        *,
        collection_enum: CollectionEnum,
        child_class_enum: ClassEnum,
        parent_class_enum: ClassEnum | None = None,
    ) -> int:
        """Return the ID for a given property.

        Retrieves the unique identifier for a property based on its name and associated
        collection and classes.

        Parameters
        ----------
        property_name : str
            Name of the property
        collection_enum : CollectionEnum
            Collection enumeration the property belongs to
        child_class_enum : ClassEnum
            Child class enumeration for the property
        parent_class_enum : ClassEnum | None, optional
            Parent class enumeration for the property, by default None

        Returns
        -------
        int
            ID of the property

        Raises
        ------
        AssertionError
            If the property does not exist

        See Also
        --------
        get_collection_id : Get the ID for a collection
        list_valid_properties : List valid property names for a collection

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.get_property_id(
        ...     "Max Capacity",
        ...     collection_enum=CollectionEnum.Generators,
        ...     child_class_enum=ClassEnum.Generator,
        ...     parent_class_enum=ClassEnum.System,
        ... )
        42  # Example ID
        """
        parent_class_enum = parent_class_enum or ClassEnum.System
        collection_id = self.get_collection_id(collection_enum, parent_class_enum, child_class_enum)
        query = f"SELECT property_id FROM {Schema.Property.name} WHERE name = ? AND collection_id = ?"
        result = self._db.fetchone(
            query,
            (property_name, collection_id),
        )
        assert result
        return result[0]

    def get_property_unit(
        self,
        property_name: str,
        /,
        collection_enum: CollectionEnum,
        parent_class_enum: ClassEnum,
        child_class_enum: ClassEnum,
    ) -> str:
        """Get the unit for a specific property."""
        raise NotImplementedError

    def get_scenario_id(self, name: str) -> int:
        """Return the ID for a given scenario.

        Retrieves the object id for a scenario based on its name.

        Parameters
        ----------
        name : str
            Name of the scenario

        Returns
        -------
        int
            ID of the property

        Raises
        ------
        AssertionError
            If the property does not exist

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_scenario("TestScenario")
        42  # Example ID
        >>> db.get_scenario_id("TestScenario")
        42
        """
        object_id = self.get_object_id(ClassEnum.Scenario, name=name)
        return object_id

    def get_text(
        self,
        data_id: int,
        /,
        *,
        class_id: int | None = None,
    ) -> list[dict]:
        """Retrieve text data associated with a property data record."""
        raise NotImplementedError

    def get_unit(self, unit_id: int) -> dict:
        """Get details for a specific unit."""
        raise NotImplementedError

    def has_properties(
        self,
        class_enum: ClassEnum,
        /,
        name: str,
        *,
        collection_enum: CollectionEnum | None = None,
        category: str | None = None,
    ) -> bool:
        """Check if the given object has any properties associated with it.

        Determines whether an object has any properties in the database that match
        the specified criteria, optionally filtered by collection and category.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration of the object
        name : str
            Name of the object to check
        collection_enum : CollectionEnum | None, optional
            Collection enumeration to filter by, by default None
            (if not specified, the default collection for the class is used)
        category : str | None, optional
            Category to filter by, by default None

        Returns
        -------
        bool
            True if the object has at least one property matching the criteria,
            False otherwise

        Raises
        ------
        KeyError
            If the specified category does not exist

        See Also
        --------
        get_object_properties : Get properties for an object
        check_category_exists : Check if a category exists
        get_default_collection : Get the default collection for a class

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Generator, "Generator1")
        >>> db.has_properties(ClassEnum.Generator, "Generator1")
        False
        >>> db.add_property(ClassEnum.Generator, "Generator1", "Max Capacity", 100.0)
        >>> db.has_properties(ClassEnum.Generator, "Generator1")
        True
        """
        params = [name]
        filters = "o.name = ?"
        if not collection_enum:
            collection_enum = get_default_collection(class_enum)
        if category:
            if not self.check_category_exists(class_enum, category):
                raise KeyError
            filters += " AND cat.name = ?"
            params.append(category)
        query = f"""
        SELECT
            1
        FROM t_object o
            JOIN t_class c ON o.class_id = c.class_id
            JOIN t_category cat ON o.category_id = cat.category_id
            JOIN t_membership m ON m.child_object_id = o.object_id
            JOIN t_data d ON d.membership_id = m.membership_id
            JOIN t_property p ON d.property_id = p.property_id
        WHERE
            {filters}
        LIMIT 1
        """
        return bool(self._db.query(query, tuple(params)))

    def import_from_csv(self, source_path: str | Path, /, *, tables: list[str] | None = None) -> None:
        """Import data from CSV files into the database."""
        raise NotImplementedError

    def iterate_properties(
        self,
        /,
        *,
        class_enum: ClassEnum | None = None,
        object_names: str | Iterable[str] | None = None,
        property_names: str | Iterable[str] | None = None,
        parent_class: ClassEnum | None = None,
        collection: CollectionEnum | None = None,
        category: str | None = None,
        batch_size: int = 1000,
    ) -> Iterator[PropertyRecord]:
        """Iterate through properties with chunked processing to handle large datasets efficiently.

        This method efficiently retrieves properties for multiple objects of the specified class,
        processing data in chunks to minimize memory usage. Results are yielded one at a time
        as dictionaries.

        Parameters
        ----------
        class_enum : ClassEnum | None, optional
            Class enumeration of the objects to retrieve properties for
        object_names : str | Iterable[str] | None, optional
            Names of specific objects to retrieve properties for. If None, gets properties
            for all objects of the specified class
        property_names : str | Iterable[str] | None, optional
            Names of specific properties to retrieve. If None, gets all properties
        parent_class : ClassEnum | None, optional
            Parent class enumeration for filtering properties, defaults to ClassEnum.System
        collection : CollectionEnum | None, optional
            Collection enumeration to filter properties by
        batch_size : int, optional
            Number of records to process in each database query chunk, by default 1000

        Yields
        ------
        PropertyRecord
            Dictionary containing property information with keys such as:
            name, property, value, unit, category, scenario_name, etc.
            See PropertyRecord TypedDict for full field definitions.

        Raises
        ------
        NameError
            If a specified property does not exist for the collection
        NotFoundError
            If specified objects or category do not exist
        KeyError
            If specified category does not exist
        """
        conditions: list[str] = []

        if class_enum and self.check_class_exists(class_enum):
            conditions.append(f"child_class.name = '{class_enum}'")

        if parent_class and self.check_class_exists(parent_class):
            conditions.append(f"parent_class.name = '{parent_class}'")

        if (
            collection
            and parent_class
            and class_enum
            and self.check_collection_exists(collection, parent_class=parent_class, child_class=class_enum)
        ):
            collection_id = self.get_collection_id(collection, parent_class, class_enum)
            conditions.append(f"membership.collection_id = {collection_id}")

        if object_names:
            names = (
                self._validate_and_filter_objects(object_names, class_enum)
                if class_enum
                else normalize_names(object_names)
            )
            joined = ", ".join(f"'{n}'" for n in names)
            conditions.append(f"object.name IN ({joined})")

        if property_names:
            check_collection = collection or (get_default_collection(class_enum) if class_enum else None)
            props = (
                self._validate_properties(property_names, check_collection, class_enum)
                if check_collection and class_enum
                else normalize_names(property_names)
            )
            joined = ", ".join(f"'{p}'" for p in props)
            conditions.append(f"property.name IN ({joined})")

        if category and class_enum and not self.check_category_exists(class_enum, category):
            msg = f"Category '{category}' does not exist for class {class_enum}."
            raise NotFoundError(msg)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        query = Template(PROPERTY_QUERY).safe_substitute(where_clause=where_clause)
        yield from cast(Iterator[PropertyRecord], self._db.iter_dicts(query, batch_size=batch_size))

    def list_attributes(self, class_enum: ClassEnum) -> list[str]:
        """Get all attributes for a specific class.

        Retrieves the names of attributes associated with the specified class.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration to list categories for

        Returns
        -------
        list[str]
            List of attributes names

        Raises
        ------
        AssertionError
            If the query fails
        """
        query = f"""
        SELECT
            t_attribute.name
        FROM
            {Schema.Attributes.name}
        LEFT JOIN
            t_class ON t_class.class_id = t_attribute.class_id
        WHERE
            t_class.name = ?
        """
        result = self._db.fetchall_dict(query, (class_enum,))
        assert result
        return [row["name"] for row in result]

    def list_categories(self, class_enum: ClassEnum) -> list[str]:
        """Get all categories for a specific class.

        Retrieves the names of all categories associated with the specified class.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration to list categories for

        Returns
        -------
        list[str]
            List of category names

        Raises
        ------
        AssertionError
            If the query fails

        See Also
        --------
        add_category : Add a new category
        check_category_exists : Check if a category exists

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_category("Thermal", ClassEnum.Generator)
        >>> db.add_category("Renewable", ClassEnum.Generator)
        >>> db.list_categories(ClassEnum.Generator)
        ['Thermal', 'Renewable']
        """
        query = """
        SELECT
            t_category.name
        FROM
            t_category
        LEFT JOIN
            t_class ON t_class.class_id = t_category.class_id
        WHERE
            t_class.name = ?
        """
        result = self._db.fetchall_dict(query, (class_enum,))
        assert result
        return [row["name"] for row in result]

    def list_child_objects(
        self,
        object_name: str,
        /,
        *,
        parent_class: ClassEnum,
        child_class: ClassEnum | None = None,
        collection: CollectionEnum | None = None,
    ) -> list[dict]:
        """List all child objects for a given parent object.

        Retrieves all child objects that have a membership relationship with the specified
        parent object, optionally filtered by child class and collection.

        Parameters
        ----------
        object_name : str
            Name of the parent object
        parent_class : ClassEnum
            Class enumeration of the parent object
        child_class : ClassEnum | None, optional
            Class enumeration to filter child objects by, by default None
        collection : CollectionEnum | None, optional
            Collection enumeration to filter relationships by, by default None

        Returns
        -------
        list[dict]
            List of dictionaries containing child object information with keys:
            - object_id: ID of the child object
            - name: Name of the child object
            - class_name: Class name of the child object
            - collection_name: Name of the collection/relationship type
            - membership_id: ID of the membership relationship

        Raises
        ------
        AssertionError
            If the parent object does not exist

        See Also
        --------
        list_parent_objects : List parent objects for a given child object
        get_object_id : Get the ID for an object
        get_class_id : Get the ID for a class
        get_collection_id : Get the ID for a collection
        add_membership : Add a membership between two objects

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Region, "Region1")
        >>> db.add_object(ClassEnum.Node, "Node1")
        >>> db.add_object(ClassEnum.Node, "Node2")
        >>> db.add_membership(
        ...     parent_class_enum=ClassEnum.Region,
        ...     child_class_enum=ClassEnum.Node,
        ...     parent_object_name="Region1",
        ...     child_object_name="Node1",
        ...     collection_enum=CollectionEnum.ReferenceNode,
        ... )
        >>> db.add_membership(
        ...     parent_class_enum=ClassEnum.Region,
        ...     child_class_enum=ClassEnum.Node,
        ...     parent_object_name="Region1",
        ...     child_object_name="Node2",
        ...     collection_enum=CollectionEnum.ReferenceNode,
        ... )
        >>> children = db.list_child_objects(
        ...     "Region1",
        ...     parent_class=ClassEnum.Region,
        ...     child_class=ClassEnum.Node
        ...     collection=CollectionEnum.ReferenceNode
        ... )
        >>> len(children)
        2
        >>> children[0]["name"]
        'Node1'
        >>> children[0]["class_name"]
        'Node'
        """
        try:
            parent_object_id = self.get_object_id(parent_class, object_name)
        except AssertionError:
            return []

        query = """
        SELECT
            child_obj.object_id,
            child_obj.name,
            child_class.name AS class_name,
            coll.name AS collection_name,
            mem.membership_id
        FROM t_membership mem
        JOIN t_object parent_obj ON mem.parent_object_id = parent_obj.object_id
        JOIN t_object child_obj ON mem.child_object_id = child_obj.object_id
        JOIN t_class parent_class ON mem.parent_class_id = parent_class.class_id
        JOIN t_class child_class ON mem.child_class_id = child_class.class_id
        JOIN t_collection coll ON mem.collection_id = coll.collection_id
        WHERE parent_obj.object_id = ?
        """

        params: list[Any] = [parent_object_id]

        if child_class is not None:
            query += " AND child_class.name = ?"
            params.append(child_class.name)

        if collection is not None:
            query += " AND coll.name = ?"
            params.append(collection.name)
        query += " ORDER BY child_obj.name"

        result = self._db.fetchall_dict(query, tuple(params))
        return result

    def list_classes(self) -> list[str]:
        """Return all classes names in the database.

        Returns
        -------
        list[str]
            List of valid classes names

        Raises
        ------
        AssertionError
            If the query fails

        See Also
        --------
        query : Query the SQL database

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.list_classes()
        ["System", "Generator", ...]
        """
        query_string = f"SELECT name from {Schema.Class.name}"
        result = self.query(query_string)
        assert result
        return [d[0] for d in result]

    def list_collections(
        self,
        /,
        *,
        parent_class: ClassEnum | None = None,
        child_class: ClassEnum | None = None,
    ) -> list[dict]:
        """List all available collections in the database.

        Parameters
        ----------
        parent_class : ClassEnum | None, optional
            Filter by parent class, by default None
        child_class : ClassEnum | None, optional
            Filter by child class, by default None

        Returns
        -------
        list[dict]
            List of collections with their details including id, name,
            parent_class, child_class, description, etc.
        """
        params = []
        parent_class_id = self.get_class_id(parent_class) if parent_class else None
        child_class_id = self.get_class_id(child_class) if child_class else None
        where_clause = ""

        if parent_class_id or child_class_id:
            where_clause = " WHERE "

        query_string = """
            SELECT t_collection.collection_id, t_collection.name AS collection_name,
            parent_class.name AS parent_class_name, child_class.name AS child_class_name
            FROM t_collection
            LEFT JOIN t_class AS parent_class ON parent_class.class_id = t_collection.parent_class_id
            LEFT JOIN t_class AS child_class ON t_collection.child_class_id = child_class.class_id
        """
        if parent_class_id:
            params.append(parent_class_id)
            where_clause += "parent_class.class_id = ?"
        if child_class_id:
            params.append(child_class_id)
            if parent_class_id:
                where_clause += " AND "
            where_clause += " child_class.class_id = ?"

        result = self._db.iter_dicts(query_string + where_clause, tuple(params))
        return list(result)

    def list_objects_by_class(self, class_enum: ClassEnum, /, *, category: str | None = None) -> list[str]:
        """Return all objects of a specific class.

        Retrieves names of all objects belonging to the specified class,
        optionally filtered by category.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration to filter oRaises
        category : str | None, optional
            Category name to filter by, by default None

        Raises
        ------
        AssertionError
            If the query fails

        See Also
        --------
        get_class_id : Get the ID for a class
        add_object : Add an object to the database
        query : Query the SQL database bjects by

        Returns
        -------
        list[dict]
            List of object names

        Raises
        ------
        AssertionError
            If the query fails

        See Also
        --------
        get_class_id : Get the ID for a class
        add_object : Add an object to the database
        query : Query the SQL database

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object("Generator1", ClassEnum.Generator)
        >>> db.add_object("Generator2", ClassEnum.Generator)
        >>> db.list_objects_by_class(ClassEnum.Generator)
        ['Generator1', 'Generator2']
        """
        class_id = self.get_class_id(class_enum)
        query = f"SELECT name from {Schema.Objects.name} WHERE class_id = ?"
        result = self._db.query(query, (class_id,))
        return [d[0] for d in result]

    def list_parent_objects(
        self,
        object_name: str,
        /,
        *,
        child_class: ClassEnum,
        parent_class: ClassEnum | None = None,
        collection: CollectionEnum | None = None,
    ) -> list[dict]:
        """List all parent objects for a given child object.

        Retrieves all parent objects that have a membership relationship with the specified
        child object, optionally filtered by parent class and collection.

        Parameters
        ----------
        object_name : str
            Name of the child object
        child_class : ClassEnum
            Class enumeration of the child object
        parent_class : ClassEnum | None, optional
            Class enumeration to filter parent objects by, by default None
        collection : CollectionEnum | None, optional
            Collection enumeration to filter relationships by, by default None

        Returns
        -------
        list[dict]
            List of dictionaries containing parent object information with keys:
            - object_id: ID of the parent object
            - name: Name of the parent object
            - class_name: Class name of the parent object
            - collection_name: Name of the collection/relationship type
            - membership_id: ID of the membership relationship

        Raises
        ------
        AssertionError
            If the child object does not exist

        See Also
        --------
        list_child_objects : List child objects for a given parent object
        get_object_id : Get the ID for an object
        get_class_id : Get the ID for a class
        get_collection_id : Get the ID for a collection
        add_membership : Add a membership between two objects

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Region, "Region1")
        >>> db.add_object(ClassEnum.Region, "Region2")
        >>> db.add_object(ClassEnum.Node, "Node1")
        >>> db.add_membership(
        ...     parent_class_enum=ClassEnum.Region,
        ...     child_class_enum=ClassEnum.Node,
        ...     parent_object_name="Region1",
        ...     child_object_name="Node1",
        ...     collection_enum=CollectionEnum.ReferenceNode,
        ... )
        >>> db.add_membership(
        ...     parent_class_enum=ClassEnum.Region,
        ...     child_class_enum=ClassEnum.Node,
        ...     parent_object_name="Region2",
        ...     child_object_name="Node1",
        ...     collection_enum=CollectionEnum.ReferenceNode,
        ... )
        >>> parents = db.list_parent_objects(
        ...     "Node1",
        ...     child_class=ClassEnum.Node,
        ...     parent_class=ClassEnum.Region,
        ...     collection=CollectionEnum.ReferenceNode,
        ... )
        >>> parents[0]["name"]
        'Region1'
        """
        try:
            child_object_id = self.get_object_id(child_class, object_name)
        except AssertionError:
            return []

        query = """
        SELECT
            parent_obj.object_id,
            parent_obj.name,
            parent_class.name AS class_name,
            coll.name AS collection_name,
            mem.membership_id
        FROM t_membership mem
        JOIN t_object parent_obj ON mem.parent_object_id = parent_obj.object_id
        JOIN t_object child_obj ON mem.child_object_id = child_obj.object_id
        JOIN t_class parent_class ON mem.parent_class_id = parent_class.class_id
        JOIN t_class child_class ON mem.child_class_id = child_class.class_id
        JOIN t_collection coll ON mem.collection_id = coll.collection_id
        WHERE child_obj.object_id = ?
        """

        params: list[Any] = [child_object_id]

        if parent_class is not None:
            query += " AND parent_class.name = ?"
            params.append(parent_class.name)

        if collection is not None:
            query += " AND coll.name = ?"
            params.append(collection.name)

        query += " ORDER BY parent_obj.name"

        result = self._db.fetchall_dict(query, tuple(params))
        return result

    def list_reports(self) -> list[dict]:
        """List all defined reports in the database."""
        raise NotImplementedError

    def list_scenarios(self) -> list[str]:
        """Return all scenarios in the database.

        Returns
        -------
        list[str]
            List of valid scenario names

        Raises
        ------
        AssertionError
            If the query fails

        See Also
        --------
        query : Query the SQL database

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Generator, "Generator1")
        >>> db.add_property(
        ...     ClassEnum.Generator,
        ...     "Generator1",
        ...     "Max Capacity",
        ...     100.0,
        ...     scenario="Scenario",
        ... )
        >>> db.list_scenarios()
        ["Scenario"]
        """
        query_string = """
        SELECT
            t_object.name
        FROM t_object
        LEFT JOIN t_class on t_class.class_id = t_object.class_id
        WHERE
            t_class.name = ?
        """
        result = self.query(query_string, (ClassEnum.Scenario,))
        assert result
        return [d[0] for d in result]

    def list_models(self) -> list[str]:
        """Return all models in the database.

        Returns
        -------
        list[str]
            List of valid models names

        Raises
        ------
        AssertionError
            If the query fails

        See Also
        --------
        query : Query the SQL database

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Model, "model_01")
        >>> db.list_models()
        ["model_01"]
        """
        query_string = """
        SELECT
            t_object.name
        FROM t_object
        LEFT JOIN t_class on t_class.class_id = t_object.class_id
        WHERE
            t_class.name = ?
        """
        result = self.query(query_string, (ClassEnum.Model,))
        assert result
        return [d[0] for d in result]

    def list_scenarios_by_model(self, model_name: str) -> list[str]:
        """
        List all scenarios associated with a given model.

        Parameters
        ----------
        model_name : str
            Name of the model to list scenarios for.

        Returns
        -------
        list[str]
            List of scenario names associated with the model. Returns an empty list if none found.

        Examples
        --------
        >>> db.add_object(ClassEnum.Model, "Model1")
        >>> db.add_object(ClassEnum.Scenario, "Scenario1")
        >>> db.add_membership(
        ...     parent_class_enum=ClassEnum.Model,
        ...     child_class_enum=ClassEnum.Scenario,
        ...     parent_object_name="Model1",
        ...     child_object_name="Scenario1",
        ...     collection_enum=CollectionEnum.Scenarios,
        ... )
        >>> db.list_scenarios_by_model("Model1")
        ['Scenario1']
        """
        parent_object_id = self.get_object_id(ClassEnum.Model, model_name)

        query = """
        SELECT t_object.name
        FROM t_membership as membership
        LEFT JOIN t_object on t_object.object_id = membership.child_object_id
        LEFT JOIN t_class on t_object.class_id = t_class.class_id
        WHERE membership.parent_object_id = ? and t_class.name = ?
        """
        result = self.query(query, (parent_object_id, ClassEnum.Scenario))
        return [row[0] for row in result] if result else []

    def list_units(self) -> list[dict[int, str]]:
        """List all available units in the database."""
        query_string = "SELECT unit_id, value from t_unit"
        result = self.query(query_string)
        assert result
        return [{d[0]: d[1]} for d in result]

    def list_valid_properties(
        self,
        collection_enum: CollectionEnum,
        /,
        parent_class_enum: ClassEnum,
        child_class_enum: ClassEnum,
    ) -> list[str]:
        """Return list of valid property names for a collection.

        Retrieves all valid property names that can be used with a specific collection,
        optionally filtered by parent and child classes.

        Parameters
        ----------
        collection_enum : CollectionEnum
            Collection enumeration to list properties for
        parent_class_enum : ClassEnum | None, optional
            Parent class enumeration to filter by, by default None
        child_class_enum : ClassEnum | None, optional
            Child class enumeration to filter by, by default None

        Returns
        -------
        list[str]
            List of valid property names

        Raises
        ------
        AssertionError
            If the query fails

        See Also
        --------
        get_collection_id : Get the ID for a collection
        check_property_exists : Check if properties exist for a collection

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.list_valid_properties(
        ...     CollectionEnum.Generators,
        ...     parent_class_enum=ClassEnum.System,
        ...     child_class_enum=ClassEnum.Generator,
        ... )
        ['Max Capacity', 'Min Stable Level', 'Heat Rate']  # Example properties
        """
        collection_id = self.get_collection_id(
            collection_enum, parent_class_enum=parent_class_enum, child_class_enum=child_class_enum
        )
        query = "SELECT name from t_property where collection_id = ?"
        result = self.query(query, (collection_id,))
        assert result
        return [d[0] for d in result]

    def list_valid_properties_report(
        self,
        collection_enum: CollectionEnum,
        /,
        parent_class_enum: ClassEnum,
        child_class_enum: ClassEnum,
    ) -> list[str]:
        """Return list of valid property names for reports.

        Retrieves all valid property names that can be used with a specific collection filtered by parent and
        child classes.

        Parameters
        ----------
        collection_enum : CollectionEnum
            Collection enumeration to list properties for
        parent_class_enum : ClassEnum | None, optional
            Parent class enumeration to filter by, by default None
        child_class_enum : ClassEnum | None, optional
            Child class enumeration to filter by, by default None

        Returns
        -------
        list[str]
            List of valid property report names

        Raises
        ------
        AssertionError
            If the query fails

        See Also
        --------
        get_collection_id : Get the ID for a collection
        """
        collection_id = self.get_collection_id(
            collection_enum, parent_class_enum=parent_class_enum, child_class_enum=child_class_enum
        )
        query = "SELECT name from t_property_report where collection_id = ?"
        result = self.query(query, (collection_id,))
        assert result
        return [d[0] for d in result]

    def query(self, query_string: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> list[Any]:
        """Execute a read-only query and return all results.

        Executes a SQL SELECT query against the database and returns the results.

        Parameters
        ----------
        query_string : str
            SQL query to execute (SELECT statements only)
        params : tuple[Any, ...] | dict[str, Any] | None, optional
            Parameters to bind to the query, by default None

        Returns
        -------
        list
            Query results (tuples or named tuples based on initialization)

        Raises
        ------
        sqlite3.Error
            If a database error occurs

        See Also
        --------
        _db.query : Underlying database query method

        Notes
        -----
        This method should ONLY be used for SELECT statements. For INSERT/UPDATE/DELETE,
        use execute() instead.

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object("Generator1", ClassEnum.Generator)
        >>> db.query("SELECT name FROM t_object WHERE class_id = ?", (15,))  # 15 = Generator class_id
        [('Generator1',)]
        """
        return self._db.query(query=query_string, params=params)

    def set_config(self, element: str, value: str) -> None:
        """Set a configuration value in the database."""
        raise NotImplementedError

    def set_date_range(
        self,
        data_id: int,
        /,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> None:
        """Set the date range for a property data record."""
        raise NotImplementedError

    def to_csv(self, target_path: str | Path, /, *, tables: list[str] | None = None) -> None:
        """Export selected tables or the entire database to CSV files."""
        raise NotImplementedError

    def to_xml(self, target_path: str | Path) -> bool:
        """Convert SQLite to XML format.

        This method takes all the tables of the SQLite database and creates the
        appropriate tags based on the column name, exporting the complete database
        as a PLEXOS-compatible XML file.

        Parameters
        ----------
        target_path: str | Path
            Path to serialize the database

        Returns
        -------
        bool
            True if the creation succeeded, False if it failed

        See Also
        --------
        from_xml : Create a database from XML file
        XMLHandler : Class that handles XML parsing and generation

        Notes
        -----
        The exported XML file follows the PLEXOS MasterDataSet format and can be
        imported directly into PLEXOS.

        Examples
        --------
        >>> # Initialize a database with some data
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> # Add some objects and properties
        >>> db.add_object(ClassEnum.Generator, "Generator1", description="Example generator")
        >>> db.add_property(ClassEnum.Generator, "Generator1", "Max Capacity", 100.0)
        >>> # Add another object with a scenario
        >>> db.add_object(ClassEnum.Generator, "Generator2")
        >>> db.add_property(ClassEnum.Generator, "Generator2", "Max Capacity", 200.0, scenario="High Demand")
        >>> # Export the database to XML
        >>> xml_path = Path("output_model.xml")
        >>> result = db.to_xml(xml_path)
        >>> result
        True
        >>> xml_path.exists()
        True
        >>> # Create a new database from the generated XML
        >>> new_db = PlexosDB.from_xml(xml_path)
        >>> generators = new_db.list_objects_by_class(ClassEnum.Generator)
        >>> sorted(generators)
        ['Generator1', 'Generator2']
        """
        xml_handler = XMLHandler(initialize=True)

        # We remove the row factory for simpler digestion to XML as list of tuples instead of having to
        # process them individually.
        previous_row_factory = self._db.connection.row_factory
        self._db.connection.row_factory = None

        tables = [
            table[0] for table in self._db.iter_query("SELECT name from sqlite_master WHERE type='table'")
        ]
        for table_name in tables:
            rows = self.query(f"SELECT * FROM {table_name}")
            if not rows:
                continue
            column_types_tuples = self.query(f"SELECT name, type FROM pragma_table_info('{table_name}')")
            column_types: dict[str, str] = {key: value for key, value in column_types_tuples}
            logger.trace("Adding {} to {}", table_name, target_path)
            xml_handler.create_table_element(rows, column_types, table_name)

        xml_handler.to_xml(target_path)

        # Reset row factory
        self._db.connection.row_factory = previous_row_factory
        return True

    def update_attribute(
        self,
        attribute_name: str,
        new_value: str | float | int,
        /,
        *,
        object_name: str,
        object_class: ClassEnum,
    ) -> None:
        """Update an attribute value for an object."""
        raise NotImplementedError

    def update_category(self, category: str, new_name: str, /, *, class_name: ClassEnum) -> None:
        """Update a category name."""
        raise NotImplementedError

    def update_object(
        self,
        class_enum: ClassEnum,
        object_name: str,
        *,
        new_name: str,  # Required parameter now
        new_category: str | None = None,
        new_description: str | None = None,
    ) -> bool:
        """Update an object's attributes.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration of the object
        object_name : str
            Current name of the object to update
        new_name : str
            New name for the object
        new_category : str | None, optional
            New category for the object, by default None
        new_description : str | None, optional
            New description for the object, by default None

        Returns
        -------
        bool
            True if the update was successful

        Raises
        ------
        AssertionError
            If the query fails

        See Also
        --------
        get_object_id : Get the ID for an object
        get_category_id : Get the ID for a category
        add_object : Add a new object to the database

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object(ClassEnum.Generator, "ThermalGen1", category="Thermal")
        >>> db.add_object(ClassEnum.Generator, "SolarGen1", category="Solar")
        >>> # Update just the name
        >>> db.update_object(ClassEnum.Generator, "ThermalGen1", new_name="SolarGen")
        True
        >>> # Update name, category, and description
        >>> db.update_object(
        ...     ClassEnum.Generator,
        ...     "SolarGen",
        ...     new_name="SolarGen2",
        ...     new_category="Solar",
        ...     new_description="Updated from thermal to solar generator",
        ... )
        True
        """
        object_id = self.get_object_id(class_enum, object_name)

        set_clauses = ["name = ?"]
        params: list[Any] = [new_name]

        if new_category is not None:
            category_id = self.get_category_id(class_enum, new_category)
            set_clauses.append("category_id = ?")
            params.append(category_id)

        if new_description is not None:
            set_clauses.append("description = ?")
            params.append(new_description)

        query = f"UPDATE {Schema.Objects.name} SET {', '.join(set_clauses)} WHERE object_id = ?"
        params.append(object_id)

        result = self._db.execute(query, tuple(params))
        assert result
        return True

    def update_properties(self, updates: list[dict]) -> None:
        """Update multiple properties in a single transaction."""
        raise NotImplementedError

    def update_property(
        self,
        object_name: str,
        property_name: str,
        new_value: str | None,
        /,
        *,
        object_class: ClassEnum,
        scenario: str | None = None,
        band: str | None = None,
        collection: CollectionEnum | None = None,
        parent_class: ClassEnum | None = None,
    ) -> None:
        """Update a property value for a given object."""
        raise NotImplementedError

    def update_scenario(
        self,
        scenario_name: str,
        /,
        *,
        new_name: str | None = None,
        new_category: str | None = None,
        new_description: str | None = None,
    ) -> None:
        """Update a scenario's properties."""
        raise NotImplementedError

    def update_text(
        self,
        data_id: int,
        text_value: str,
        /,
        *,
        class_id: int,
    ) -> None:
        """Update text data for a property data record."""
        raise NotImplementedError

    def validate_database(self, /, *, fix_issues: bool = False) -> dict[str, list[str]]:
        """Validate database integrity and consistency."""
        raise NotImplementedError

    def _validate_and_filter_objects(
        self, object_names: str | Iterable[str], class_enum: ClassEnum
    ) -> list[str]:
        """Validate objects exist and return filtered list of valid names."""
        names = normalize_names(object_names)
        valid_names = [n for n in names if self.check_object_exists(class_enum, n)]
        if not valid_names:
            msg = (
                f"None of the objects {names} exist in class {class_enum}. "
                "Use `list_objects_by_class()` to see available objects."
            )
            raise NotFoundError(msg)
        return valid_names

    def _validate_properties(
        self, property_names: str | Iterable[str], collection: CollectionEnum, class_enum: ClassEnum
    ) -> list[str]:
        """Validate properties exist for collection and return normalized list."""
        props = normalize_names(property_names)
        if not self.check_property_exists(collection, class_enum, props):
            msg = (
                f"Invalid property {props} for collection={collection}. "
                "Use `list_valid_properties()` to check valid properties."
            )
            raise NameError(msg)
        return props
