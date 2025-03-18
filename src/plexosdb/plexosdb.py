"""Main API for interacting with the Plexos database schema."""

import sqlite3
import sys
import uuid
from collections.abc import Iterable, Iterator
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal

from loguru import logger

from .db_manager import SQLiteManager
from .enums import ClassEnum, CollectionEnum, Schema, get_default_collection, str2enum
from .exceptions import PropertyNameError
from .utils import get_sql_query, normalize_names
from .xml_handler import XMLHandler

if sys.version_info >= (3, 12):
    from itertools import batched
else:
    from .utils import batched

SQLITE_BACKEND_KWARGS = {"create_collations", "initialize", "in_memory", "use_named_tuples"}
PLEXOS_DEFAULT_SCHEMA = fpath = files("plexosdb").joinpath("schema.sql").read_text(encoding="utf-8-sig")


class PlexosDB:
    """High-level API for PlexosDB schema manipulation."""

    def __init__(
        self,
        xml_fpath: Path | str | None = None,
        db_path: Path | str | None = None,
        plexos_schema: str | None = None,
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
        self._db = SQLiteManager(db_path=db_path, **sqlite_kwargs)

        # Initialize version attribute
        self._version = self._initialize_version()

    @property
    def version(self) -> str:
        """Get the PLEXOS version of the loaded model."""
        if not self._version:
            self._version = self._initialize_version()
        return self._version

    def _initialize_version(self) -> str:
        """Initialize the PLEXOS version from the database."""
        # Attempt to get version from database configuration
        try:
            result = self.query("SELECT value FROM t_config WHERE element = 'Version'")
        except sqlite3.OperationalError:
            return ""
        if result and result[0][0]:
            return result[0][0]
        return ""

    @classmethod
    def from_xml(cls, xml_path: str | Path, **kwargs) -> "PlexosDB":
        """Create a PlexosDB instance from an XML file.

        This factory method creates a new PlexosDB instance and populates it with data
        from the provided XML file. It creates the database schema and processes all
        valid XML elements into their corresponding database tables.

        Parameters
        ----------
        xml_path : str | Path
            Path to the XML file to import data from
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

        instance = cls(**kwargs)
        instance.create_schema()
        xml_handler = XMLHandler.parse(fpath=xml_path)
        xml_tags = set([e.tag for e in xml_handler.root])  # Extract set of valid tags from xml
        for tag in xml_tags:
            # Only parse valid schemas that we maintain.
            # NOTE: If there are some missing tables, we need to add them to the Enums.
            schema = str2enum(tag)
            if not schema:
                continue

            record_dict = xml_handler.get_records(schema)
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

        return instance

    def add_attribute(
        self,
        /,
        *,
        object_name: str,
        object_class: ClassEnum,
        attribute_class: ClassEnum,
        attribute_name: str,
        attribute_value: str | float | int,
    ) -> int:
        """Add attribute to a given object."""
        raise NotImplementedError

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

    def add_category(self, category_name: str, /, class_name: ClassEnum) -> int:
        """Add a new category for a given class.

        Creates a new category for the specified class. If the category already exists,
        it returns the ID of the existing category.

        Parameters
        ----------
        category_name : str
            Name of the category to be added
        class_name : ClassEnum
            Class enumeration for the category, e.g., for generators use ClassEnum.Generator

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
        if self.check_category_exists(category_name, class_enum=class_name):
            logger.debug("Category {} for {} already exist. Returning id.", category_name, class_name)
            return self.get_category_id(category_name, class_enum=class_name)
        class_id = self.get_class_id(class_name)
        rank = self.get_category_max_id(class_name) or 1
        params = (class_id, category_name, rank)
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
        parent_object_name: str,
        child_object_name: str,
        /,
        parent_class_enum: ClassEnum,
        child_class_enum: ClassEnum,
        collection_enum: CollectionEnum,
    ) -> int:
        """Add a membership between two objects for a given collection.

        Creates a relationship between parent and child objects within the specified collection.

        Parameters
        ----------
        parent_object_name : str
            Name of the parent object
        child_object_name : str
            Name of the child object
        parent_class_enum : ClassEnum
            Class enumeration of the parent object
        child_class_enum : ClassEnum
            Class enumeration of the child object
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
        >>> db.add_object("Parent", ClassEnum.Region)
        >>> db.add_object("Child", ClassEnum.Node)
        >>> db.add_membership("Parent", "Child", ClassEnum.Region, ClassEnum.Node, CollectionEnum.RegionNode)
        1
        """
        parent_class_id = self.get_class_id(parent_class_enum)
        child_class_id = self.get_class_id(child_class_enum)
        parent_object_id = self.get_object_id(parent_object_name, parent_class_enum)
        child_object_id = self.get_object_id(child_object_name, child_class_enum)
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
        assert query_status
        return self._db.last_insert_rowid()

    def add_memberships_from_records(
        self,
        records: list[dict],
        /,
        *,
        parent_class: ClassEnum,
        child_class: ClassEnum,
        collection: CollectionEnum,
        create_missing_objects: bool = False,
        chunksize: int = 10_000,
    ) -> None:
        """Bulk insert multiple memberships from a list of records."""
        raise NotImplementedError

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
        name: str,
        class_enum: ClassEnum,
        /,
        *,
        collection_enum: CollectionEnum | None = None,
        category: str = "-",
        description: str | None = None,
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
        collection_enum : CollectionEnum | None, optional
            Collection for the system membership. If None, a default collection is determined
            based on the class, by default None
        category : str, optional
            Category of the object, by default "-"
        description : str | None, optional
            Description of the object, by default None

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
        if not self.check_category_exists(category, class_enum=class_enum):
            category_id = self.add_category(category, class_name=class_enum)

        category_id = category_id or self.get_category_id(category, class_enum=class_enum)
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
        _ = self.add_membership("System", name, ClassEnum.System, class_enum, collection_enum)
        return object_id

    def add_properties_from_records(
        self,
        records: list[dict],
        /,
        *,
        parent_class: ClassEnum,
        parent_object_name: str = "System",
        collection: CollectionEnum,
        scenario: str,
        chunksize: int = 10_000,
    ) -> None:
        """Bulk insert multiple properties from a list of records."""
        raise NotImplementedError

    def add_property(
        self,
        object_name: str,
        property_name: str,
        property_value: str | int | float,
        /,
        object_class_enum: ClassEnum,
        *,
        collection_enum: CollectionEnum | None = None,
        parent_class_enum: ClassEnum | None = None,
        parent_object_name: str | None = None,
        scenario: str | None = None,
        band: str | int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        text: dict[ClassEnum, Any] | None = None,
    ) -> int:
        """Add a property for a given object in the database.

        Adds a property with the specified value to an object, optionally associating
        it with a scenario, band, date range, and text data.

        Parameters
        ----------
        object_name : str
            Name of the object to add the property to
        property_name : str
            Name of the property to add
        property_value : str | int | float
            Value to assign to the property
        object_class_enum : ClassEnum
            Class enumeration of the object
        collection_enum : CollectionEnum | None, optional
            Collection enumeration for the property. If None, a default is determined
            based on the object class, by default None
        parent_class_enum : ClassEnum | None, optional
            Class enumeration of the parent object. If None, defaults to ClassEnum.System,
            by default None
        parent_object_name : str | None, optional
            Name of the parent object. If None, defaults to "System", by default None
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

        Returns
        -------
        int
            ID of the created property data record

        Raises
        ------
        PropertyNameError
            If the property name does not exist for the specified collection

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
        >>> db.add_object("Generator1", ClassEnum.Generator)
        >>> db.add_property("Generator1", "Max Capacity", 100.0, ClassEnum.Generator)
        1
        """
        # Ensure object exist
        _ = self.get_object_id(object_name, class_enum=object_class_enum)

        if not collection_enum:
            collection_enum = get_default_collection(object_class_enum)

        parent_class_enum = parent_class_enum or ClassEnum.System

        valid_properties = self.list_valid_properties(
            collection_enum, child_class_enum=object_class_enum, parent_class_enum=parent_class_enum
        )
        if property_name not in valid_properties:
            msg = (
                f"Property {property_name} does not exist for collection: {collection_enum}. "
                f"Run `self.list_valid_properties({collection_enum}) to verify valid properties."
            )
            raise PropertyNameError(msg)

        property_id = self.get_property_id(
            property_name,
            parent_class_enum=parent_class_enum,
            child_class_enum=object_class_enum,
            collection_enum=collection_enum,
        )
        membership_id = self.get_membership_id(parent_object_name or "System", object_name, collection_enum)

        query = f"INSERT INTO {Schema.Data.name}(membership_id, property_id, value) values (?, ?, ?)"
        result = self._db.execute(query, (membership_id, property_id, property_value))
        assert result
        data_id = self._db.last_insert_rowid()

        if scenario is not None:
            if not self.check_scenario_exists(scenario):
                scenario_id = self.add_object(
                    scenario, ClassEnum.Scenario, collection_enum=CollectionEnum.Scenario
                )
            scenario_query = "INSERT INTO t_tag(object_id,data_id) VALUES (?,?)"
            result = self._db.execute(scenario_query, (scenario_id, data_id))

        # Text could contain multiple keys, if so we add all of them with a execute many.
        if text is not None:
            text_params: list[tuple[Any, ...]] = []
            for key, value in text.items():
                text_class_id = self.get_class_id(key)
                text_params.append((text_class_id, data_id, value))
            self._db.executemany("INSERT into t_text(class_id,data_id,value) VALUES(?,?,?)", text_params)
        return data_id

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
        """Add a report configuration."""
        raise NotImplementedError

    def add_text(
        self,
        data_id: int,
        text_value: str,
        /,
        *,
        class_id: int,
        action_id: int | None = None,
    ) -> None:
        """Add text data to a property data record."""
        raise NotImplementedError

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

    def check_category_exists(self, category_name: str, class_enum: ClassEnum) -> bool:
        """Check if a category exists for a specific class.

        Determines whether a category with the given name exists for the specified class.

        Parameters
        ----------
        category_name : str
            Name of the category to check
        class_enum : ClassEnum
            Class enumeration to check the category for

        Returns
        -------
        bool
            True if the category exists, False otherwise

        See Also
        --------
        get_class_id : Get the ID for a class
        add_category : Add a new category

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_category("my_category", ClassEnum.Generator)
        >>> db.check_category_exists("my_category", ClassEnum.Generator)
        True
        >>> db.check_category_exists("nonexistent", ClassEnum.Generator)
        False
        """
        query = f"SELECT 1 FROM {Schema.Categories.name} WHERE name = ? AND class_id = ?"
        class_id = self.get_class_id(class_enum)
        return bool(self._db.query(query, (category_name, class_id)))

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
        """Check if a membership exists between two objects."""
        raise NotImplementedError

    def check_object_exists(self, object_name: str, class_enum: ClassEnum) -> bool:
        """Check if an object exists in the database.

        Determines whether an object with the given name and class exists.

        Parameters
        ----------
        object_name : str
            Name of the object to check
        class_enum : ClassEnum
            Class enumeration of the object

        Returns
        -------
        bool
            True if the object exists, False otherwise

        See Also
        --------
        get_class_id : Get the ID for a class
        get_object_id : Get the ID for an object
        add_object : Add an object to the database

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object("TestObject", ClassEnum.Generator)
        >>> db.check_object_exists("TestObject", ClassEnum.Generator)
        True
        >>> db.check_object_exists("NonExistent", ClassEnum.Generator)
        False
        """
        query = f"SELECT 1 FROM {Schema.Objects.name} WHERE name = ? AND class_id = ?"
        class_id = self.get_class_id(class_enum)
        return bool(self._db.query(query, (object_name, class_id)))

    def check_property_exists(
        self,
        property_names: str | Iterable[str],
        /,
        collection_enum: CollectionEnum,
        *,
        object_class: ClassEnum,
        parent_class: ClassEnum | None = None,
    ) -> bool:
        """Check if properties exist for a specific collection and class.

        Verifies that all specified property names are valid for the given collection and class.

        Parameters
        ----------
        property_names : str | Iterable[str]
            Property name or names to check
        collection_enum : CollectionEnum
            Collection enumeration the properties should belong to
        object_class : ClassEnum
            Class enumeration of the object
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
        >>> db.check_property_exists(
        ...     "Max Capacity", CollectionEnum.GeneratorProperties, object_class=ClassEnum.Generator
        ... )
        True
        >>> db.check_property_exists(
        ...     ["Invalid Property"], CollectionEnum.GeneratorProperties, object_class=ClassEnum.Generator
        ... )
        False
        """
        if property_names is None:
            return []
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

    def check_scenario_exists(self, scenario_name) -> bool:
        """Check if a scenario exists in the database.

        Determines whether a scenario with the given name exists.

        Parameters
        ----------
        scenario_name : str
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
        return bool(self._db.query(query, (scenario_name, class_id)))

    def copy_object(
        self,
        original_object_name: str,
        new_object_name: str,
        object_class: ClassEnum,
        copy_properties: bool = True,
    ) -> int:
        """Copy an object and its properties, tags, and texts."""
        raise NotImplementedError

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

    def delete_category(self, category_name: str, /, *, class_name: ClassEnum) -> None:
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

    def delete_object(self, object_name: str, /, *, class_enum: ClassEnum, cascade: bool = True) -> None:
        """Delete an object from the database."""
        raise NotImplementedError

    def delete_property(
        self,
        property_name: str,
        /,
        *,
        object_name: str,
        object_class: ClassEnum,
        collection: CollectionEnum,
        parent_class: ClassEnum | None = None,
        scenario: str | None = None,
    ) -> None:
        """Delete a property from an object."""
        raise NotImplementedError

    def delete_text(
        self,
        data_id: int,
        /,
        *,
        class_id: int,
    ) -> None:
        """Delete text data from a property data record."""
        raise NotImplementedError

    def export_to_csv(self, target_path: str | Path, /, *, tables: list[str] | None = None) -> None:
        """Export selected tables or the entire database to CSV files."""
        raise NotImplementedError

    def export_to_xml(self, target_path: str | Path) -> None:
        """Export the current database content to an XML file."""
        raise NotImplementedError

    def get_attribute(
        self,
        attribute_name: str,
        /,
        *,
        object_name: str,
        object_class: ClassEnum,
    ) -> dict:
        """Get attribute details for a specific object."""
        raise NotImplementedError

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

    def get_category_id(self, category_name: str, /, class_enum: ClassEnum) -> int:
        """Return the ID for a given category.

        Retrieves the unique identifier for a category with the specified name and class.

        Parameters
        ----------
        category_name : str
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
        >>> db.add_category("my_category", ClassEnum.Generator)
        >>> db.get_category_id("my_category", ClassEnum.Generator)
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
        result = self._db.query(query, (category_name, class_enum))
        assert result
        return result[0]["category_id"]

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
        result = self._db.query(query, (class_enum,))
        assert result
        return result[0]["rank"]

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
        result = self._db.query(query, (class_enum,))
        assert result
        return result[0]["class_id"]

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
        result = self._db.query(query, (collection, parent_class_enum, child_class_enum))
        assert result
        return result[0]["collection_id"]

    def get_config(self, element: str | None = None) -> dict | list[dict]:
        """Get configuration values from the database."""
        raise NotImplementedError

    def get_custom_columns(self, class_enum: ClassEnum | None = None) -> list[dict]:
        """Get custom columns, optionally filtered by class."""
        raise NotImplementedError

    def get_data_ids(
        self,
        object_name: str,
        class_enum: ClassEnum,
        parent_class_enum: ClassEnum = ClassEnum.System,
        property_names: str | Iterable[str] | None = None,
        collection_enum: CollectionEnum | None = None,
        category: str | None = None,
    ):
        """Get all the `data_id` for a given object."""
        params = [object_name]
        filters = "o.name = ?"
        if not collection_enum:
            collection_enum = get_default_collection(class_enum)
        if category:
            if not self.check_category_exists(category, class_enum):
                raise KeyError
            filters += " AND cat.name = ?"
            params.append(category)

        if property_names:
            if not self.check_property_exists(
                property_names,
                collection_enum=collection_enum,
                object_class=class_enum,
                parent_class=parent_class_enum,
            ):
                msg = (
                    f"Invalid property {property_names} for {collection_enum=}."
                    " Use `list_valid_properties` to check property requested."
                )
                raise PropertyNameError(msg)
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
        return [row["data_id"] for row in result]

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
        >>> db.add_object("System", ClassEnum.System)
        >>> db.add_object("Generator1", ClassEnum.Generator)
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
        result = self._db.query(query, (parent_name, child_name, collection))
        assert result
        return result[0]["membership_id"]

    def get_memberships(
        self,
        *object_names: str | list[str],
        object_class: ClassEnum,
        parent_class: ClassEnum | None = None,
        collection: CollectionEnum | None = None,
        include_system_membership: bool = False,
    ) -> list[tuple]:
        """Retrieve all memberships for the given object(s)."""
        raise NotImplementedError

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

    def get_object_properties(
        self,
        object_name: str,
        class_enum: ClassEnum,
        property_names: str | Iterable[str] | None = None,
        parent_class_enum: ClassEnum | None = None,
        collection_enum: CollectionEnum | None = None,
        category: str | None = None,
        chunk_size: int = 1000,
    ) -> list[dict]:
        """Retrieve properties for a specific object with efficient memory handling.

        Gets properties for the specified object, with support for chunked processing
        to prevent memory issues when dealing with large datasets.

        Parameters
        ----------
        object_name : str
            Name of the object to retrieve properties for
        class_enum : ClassEnum
            Class enumeration of the object
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
        chunk_size : int, optional
            Number of data IDs to process in each chunk, by default 1000

        Returns
        -------
        list[dict]
            List of dictionaries containing property information with keys:
            - name: Object name
            - property: Property name
            - value: Property value
            - unit: Unit of measurement
            - texts: Associated text data
            - tags: Associated tags
            - bands: Associated bands
            - scenario: Associated scenario name
            - scenario_category: Category of the associated scenario

        Raises
        ------
        PropertyNameError
            If the specified property does not exist for the collection
        KeyError
            If the specified category does not exist

        See Also
        --------
        get_data_ids : Get data IDs for an object
        check_property_exists : Check if properties exist for a collection
        list_valid_properties : List valid property names for a collection

        Examples
        --------
        >>> db = PlexosDB()
        >>> db.create_schema()
        >>> db.add_object("Generator1", ClassEnum.Generator)
        >>> db.add_property("Generator1", "Max Capacity", 100.0, ClassEnum.Generator)
        >>> properties = db.get_object_properties("Generator1", ClassEnum.Generator)
        >>> properties[0]["property"]
        'Max Capacity'
        >>> properties[0]["value"]
        100.0
        """
        parent_class_enum = parent_class_enum or ClassEnum.System
        query = """
        SELECT
            d.data_id,
            o.name AS name,
            p.name AS property,
            d.value AS property_value,
            u.value AS unit
        FROM t_data d
        JOIN t_property p ON d.property_id = p.property_id
        JOIN t_membership m ON d.membership_id = m.membership_id
        JOIN t_object o ON m.child_object_id = o.object_id
        LEFT JOIN t_unit u  ON p.unit_id = u.unit_id
        WHERE
            d.data_id IN ({placeholders});
        """

        data_ids = self.get_data_ids(
            object_name, class_enum, parent_class_enum, property_names, collection_enum, category
        )
        assert data_ids
        all_results: list[dict[str, Any]] = []
        for chunk_data_ids in batched(data_ids, chunk_size):
            placeholders = ",".join(["?"] * len(chunk_data_ids))
            base_query = query.format(placeholders=placeholders)

            base_data = {
                row[0]: {
                    "name": row[1],
                    "property": row[2],
                    "value": row[3],
                    "unit": row[4],
                    "texts": "",
                    "tags": "",
                    "bands": "",
                    "scenario": None,
                    "scenario_category": None,
                }
                for row in self.query(base_query, chunk_data_ids)
            }

            # Get text values for this chunk
            text_query = f"""
                SELECT
                    txt.data_id,
                    GROUP_CONCAT(txt.value, '; ') as text_values
                FROM t_text txt
                WHERE txt.data_id IN ({placeholders})
                GROUP BY txt.data_id
            """
            for row in self.query(text_query, chunk_data_ids):
                if row[0] in base_data:
                    base_data[row[0]]["texts"] = row[1] or ""

            # Get tag values for this chunk
            tag_query = f"""
                SELECT
                    tag.data_id,
                    GROUP_CONCAT(pt.name, '; ') as tag_values
                FROM t_tag tag
                JOIN t_property_tag pt ON tag.action_id = pt.tag_id
                WHERE tag.data_id IN ({placeholders})
                GROUP BY tag.data_id
            """
            for row in self.query(tag_query, chunk_data_ids):
                if row[0] in base_data:
                    base_data[row[0]]["tags"] = row[1] or ""

            # Get band values for this chunk
            band_query = f"""
                SELECT
                    band.data_id,
                    GROUP_CONCAT(band.band_id, '; ') as band_values
                FROM t_band band
                WHERE band.data_id IN ({placeholders})
                GROUP BY band.data_id
            """
            for row in self.query(band_query, chunk_data_ids):
                if row[0] in base_data:
                    base_data[row[0]]["bands"] = row[1] or ""

            # Get scenario info for this chunk
            scenario_query = f"""
                SELECT
                    t.data_id,
                    obj.name AS scenario_name,
                    cat.name AS scenario_category
                FROM t_tag t
                JOIN t_membership mem ON t.object_id = mem.child_object_id
                JOIN t_object obj ON mem.child_object_id = obj.object_id
                JOIN t_class cls ON mem.child_class_id = cls.class_id
                LEFT JOIN t_category cat ON obj.category_id = cat.category_id
                WHERE cls.name = 'Scenario' AND t.data_id IN ({placeholders})
            """
            for row in self.query(scenario_query, chunk_data_ids):
                if row[0] in base_data:
                    base_data[row[0]]["scenario"] = row[1]
                    base_data[row[0]]["scenario_category"] = row[2]

            # Add results from this chunk to the final results
            all_results.extend(base_data.values())

        return all_results

    def get_object_legacy(
        self,
        object_name: str,
        /,
        class_enum: ClassEnum,
        *,
        property_names: str | Iterable[str] | None = None,
        collection_enum: CollectionEnum | None = None,
        parent_class: ClassEnum | None = None,
        scenario: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get all details about a specific object including its properties."""
        filters = " AND o.name = ?"
        params = [object_name]
        parent_class = parent_class or ClassEnum.System
        if not collection_enum:
            collection_enum = get_default_collection(class_enum)

        if category:
            if not self.check_category_exists(category, class_enum):
                raise KeyError
            filters += " AND cat.name = ?"
            params.append(category)

        if property_names:
            if not self.check_property_exists(
                property_names,
                collection_enum=collection_enum,
                object_class=class_enum,
                parent_class=parent_class,
            ):
                msg = (
                    f"Invalid property {property_names} for {collection_enum=}."
                    " Use `list_valid_properties` to check property requested."
                )
                raise PropertyNameError(msg)
            properties = normalize_names(property_names)
            prop_placeholders = ", ".join("?" for _ in properties)
            filters += f" AND p.name IN ({prop_placeholders})"
            params.extend(properties)

        query_template = get_sql_query("property_query.sql")
        query = query_template.format(class_name=class_enum.name, extra_filters=filters)
        result = self._db.query(query, tuple(params))
        assert result
        return [{key: row[key] for key in row.keys()} for row in result]

    def get_object_id(
        self, object_name: str, /, class_enum: ClassEnum, *, category_name: str | None = None
    ) -> int:
        """Return the ID for a given object.

        Retrieves the unique identifier for an object with the specified name and class,
        optionally filtering by category.

        Parameters
        ----------
        object_name : str
            Name of the object
        class_enum : ClassEnum
            Class enumeration of the object
        category_name : str | None, optional
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
        >>> db.add_object("Generator1", ClassEnum.Generator, category="Thermal")
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
        params: list[Any] = [object_name, class_enum]
        if category_name:
            category_id = self.get_category_id(category_name, class_enum)
            params.append(category_id)
            query += "AND obj.category_id = ?"
        result = self._db.query(query, tuple(params))
        assert result
        return result[0]["object_id"]

    def get_objects(
        self,
        /,
        *,
        class_enum: ClassEnum | None = None,
        category: str | None = None,
        include_properties: bool = False,
        property_names: str | Iterable[str] | None = None,
        collection: CollectionEnum | None = None,
        parent_class: ClassEnum | None = None,
        scenario: str | None = None,
    ) -> list[dict]:
        """Get all objects, optionally filtered by class and category."""
        raise NotImplementedError

    def get_plexos_version(self) -> str:
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
        ...     collection_enum=CollectionEnum.GeneratorProperties,
        ...     child_class_enum=ClassEnum.Generator,
        ...     parent_class_enum=ClassEnum.System,
        ... )
        42  # Example ID
        """
        parent_class_enum = parent_class_enum or ClassEnum.System
        collection_id = self.get_collection_id(collection_enum, parent_class_enum, child_class_enum)
        query = f"SELECT property_id FROM {Schema.Property.name} WHERE name = ? AND collection_id = ?"
        result = self._db.query(
            query,
            (property_name, collection_id),
        )
        assert result
        return result[0]["property_id"]

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

    def get_scenario_id(self, scenario_name: str) -> int:
        """Return scenario id for a given scenario name."""
        raise NotImplementedError

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

    def import_from_csv(self, source_path: str | Path, /, *, tables: list[str] | None = None) -> None:
        """Import data from CSV files into the database."""
        raise NotImplementedError

    def iterate_properties(
        self,
        class_enum: ClassEnum,
        /,
        *,
        object_names: str | Iterable[str] | None = None,
        property_names: str | Iterable[str] | None = None,
        parent_class: ClassEnum | None = None,
        collection: CollectionEnum | None = None,
        chunk_size: int = 1000,
    ) -> Iterator[dict]:
        """Iterate through properties with chunked processing to handle large datasets efficiently."""
        raise NotImplementedError

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
        result = self._db.query(query, (class_enum,))
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
        """List all child objects for a given parent object."""
        raise NotImplementedError

    def list_classes(self) -> list[dict]:
        """List all available classes in the database."""
        raise NotImplementedError

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
        raise NotImplementedError

    def list_models(self) -> list[dict]:
        """Return all models in the database."""
        raise NotImplementedError

    def list_objects_by_class(self, class_enum: ClassEnum, /, *, category: str | None = None) -> list[dict]:
        """Return all objects of a specific class.

        Retrieves names of all objects belonging to the specified class,
        optionally filtered by category.

        Parameters
        ----------
        class_enum : ClassEnum
            Class enumeration to filter objects by
        category : str | None, optional
            Category name to filter by, by default None

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
        assert result
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
        """List all parent objects for a given child object."""
        raise NotImplementedError

    def list_reports(self) -> list[dict]:
        """List all defined reports in the database."""
        raise NotImplementedError

    def list_scenarios(self) -> list[dict]:
        """Return all scenarios in the database."""
        raise NotImplementedError

    def list_units(self) -> list[dict]:
        """List all available units in the database."""
        raise NotImplementedError

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
        ...     CollectionEnum.GeneratorProperties,
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

    def query(self, query_string: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> list:
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

    def update_category(self, category_name: str, new_name: str, /, *, class_name: ClassEnum) -> None:
        """Update a category name."""
        raise NotImplementedError

    def update_object(
        self,
        object_name: str,
        /,
        *,
        class_enum: ClassEnum,
        new_name: str | None = None,
        new_category: str | None = None,
        new_description: str | None = None,
    ) -> None:
        """Update an object's attributes."""
        raise NotImplementedError

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
