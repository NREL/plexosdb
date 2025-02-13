"""Simple interface with the Plexos database."""

import sqlite3
import uuid
import xml.etree.ElementTree as ET  # noqa: N817
from collections.abc import Iterable, Sequence
from importlib.resources import files
from pathlib import Path
from typing import Any

from loguru import logger

from plexosdb.exceptions import PropertyNameError

from .enums import ClassEnum, CollectionEnum, Schema, str2enum
from .utils import batched, no_space
from .xml_handler import XMLHandler

SYSTEM_CLASS_NAME = "System"


class PlexosSQLite:
    """Class that wraps the connection to the SQL database.

    Since we always start from a file XML, the default behaviour is to create an in-memory representation
    of the database. The usage is not to persist it into disk since the output file is always a XML, but it is
    possible by using the method `backup`.
    """

    DB_FILENAME = "plexos.db"
    _conn: sqlite3.Connection

    def __init__(
        self,
        xml_fname: str | None = None,
        xml_handler: XMLHandler | None = None,
        create_collations: bool = True,
    ) -> None:
        self._conn = sqlite3.connect(":memory:")
        self._sqlite_config()
        self._QUERY_CACHE: dict[tuple, int] = {}

        if create_collations:
            self._create_collations()
        # Always create table schema (even if no XML is loaded)
        self._create_table_schema()

        # Only populate if user provided an XML filename or handler.
        if xml_fname or xml_handler:
            self._populate_database(xml_fname=xml_fname, xml_handler=xml_handler)

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
        """Add attribute to a given object.

        Attributes are different from properties. They live on a separate table
        and are mostly used for model configuration.

        Parameters
        ----------
        object_name : str
            Name to be added to the object
        class_id : ClassEnum
            ClassEnum from the object to be added. E.g., for generators class_id=ClassEnum.Generators
        collection_id : CollectionEnum
            Collection for system membership. E.g., for generators class_enum=CollectionEnum.SystemGenerators
        object_class
            ClassEnum from the object to be added. E.g., for generators class_id=ClassEnum.Generators
        attribute_class
            Class of the attribute to be added
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
        object_id = self.get_object_id(object_name, class_name=object_class)
        attribute_id = self._get_id(Schema.Attributes, attribute_name, class_name=attribute_class)
        params = (object_id, attribute_id, attribute_value)
        placeholders = ", ".join("?" * len(params))
        query = (
            f"INSERT INTO {Schema.AttributeData.name}(object_id, attribute_id, value) VALUES({placeholders})"
        )
        attribute_id = self._execute_insert(query, params)
        return attribute_id

    def add_category(self, category_name, /, *, class_name: ClassEnum) -> int:
        """Add a new category for a given class.

        Parameters
        ----------
        category_name
            Name to be added to the object
        class_name
            ClassEnum from the category to be added. E.g., for generators class_name=ClassEnum.Generators

        Returns
        -------
        int
            category_id

        Raises
        ------
        KeyError
            When the property is not a valid string for the collection.
        TypeError
            If the database could not return the category_id from the connection.
        """
        class_id = self._get_id(Schema.Class, class_name.name)
        rank_query = f"""
        SELECT
            max(rank)
        FROM
            {Schema.Categories.name}
        WHERE
            class_id = :class_id
        """
        existing_rank = (
            self.query(
                rank_query,
                {"class_id": class_id},
            )[0][0]
            or 0
        )

        params = (class_id, existing_rank + 1, category_name)

        with self._conn as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO {Schema.Categories.name}(class_id, rank, name) VALUES(?, ?, ?)", params
            )
            category_id = cursor.lastrowid

        if category_id is None:
            raise TypeError("Could not fetch the last row of the insert. Check query format.")

        return category_id

    def add_membership(
        self,
        parent_object_name,
        child_object_name,
        /,
        *,
        parent_class: ClassEnum,
        child_class: ClassEnum,
        collection: CollectionEnum,
        child_category: str | None = None,
    ) -> int:
        """Add a memberships between two objects for a given collection.

        Parameter
        ---------
        parent_object_name
            Name to parent
        child_object_name
            Name of the child
        parent_class
            Class of to parent
        child_class
            Class of the child
        collection
            Collection for membership to be added.
        """
        # Check if classes are found first
        parent_class_id = self._get_id(Schema.Class, parent_class.name)
        child_class_id = self._get_id(Schema.Class, child_class.name)

        # Check for child objects
        parent_object_id = self._get_id(Schema.Objects, parent_object_name, class_name=parent_class)
        child_object_id = self._get_id(
            Schema.Objects, child_object_name, class_name=child_class, category_name=child_category
        )
        collection_id = self._get_id(
            Schema.Collection,
            collection.name,
            parent_class_name=parent_class,
            child_class_name=child_class,
        )

        membership_id = self._add_membership(
            parent_object_id, child_object_id, parent_class_id, child_class_id, collection_id
        )

        return membership_id

    def _add_membership(
        self,
        parent_object: int,
        child_object: int,
        parent_class: int,
        child_class: int,
        collection: int,
    ) -> int:
        membership_query = """
        INSERT into
            t_membership(parent_class_id, parent_object_id, child_class_id, child_object_id, collection_id)
        values (?,?,?,?,?)
        """

        with self._conn as conn:
            cursor = conn.cursor()
            cursor.execute(
                membership_query,
                (parent_class, parent_object, child_class, child_object, collection),
            )
            membership_id = cursor.lastrowid
        assert membership_id is not None

        return membership_id

    def add_property(
        self,
        object_name: str,
        property_name: str,
        property_value: str | int | float,
        /,
        *,
        object_class: ClassEnum,
        collection: CollectionEnum,
        parent_class: ClassEnum | None = None,
        parent_object_name: str | None = None,
        scenario: str | None = None,
        text: dict | None = None,
    ):
        """Add property for a given object in the database.

        Parameters
        ----------
        object_name
            Name to be added to the object
        property_name
            Valid plexos property to be added for the given collection
        property_value
            Value to assign to the property
        object_class
            ClassEnum from the object to be added. E.g., for generators class_id=ClassEnum.Generators
        parent_object_name
            Name of the parent object. User for creating the membership.
        collection
            Collection for membership
        scenario
            Scenario tag to add to the property
        text
            Additional text to add to the property. E.g., memo data or Data File.

        Returns
        -------
        int
            data_id of the added property.

        Raises
        ------
        KeyError
            When the property is not a valid string for the collection.
        """
        parent_class = parent_class or ClassEnum.System
        object_id = self.get_object_id(object_name, class_name=object_class)
        valid_properties = self.get_valid_properties(
            collection, child_class=object_class, parent_class=parent_class
        )
        if property_name not in valid_properties:
            msg = (
                f"Property {property_name} does not exist for collection: {collection}. "
                f"Run `self.get_valid_properties({collection}) to verify valid properties."
            )
            raise KeyError(msg)
        property_id = self.get_property_id(
            property_name, collection=collection, child_class=object_class, parent_class=parent_class
        )
        assert object_id is not None

        # Add system membership
        parent_object_name = parent_object_name or SYSTEM_CLASS_NAME  # Default to system class

        membership_id = self.get_membership_id(
            child_name=object_name,
            parent_name=parent_object_name,
            child_class=object_class,
            parent_class=parent_class,
            collection=collection,
        )

        sqlite_data = (membership_id, property_id, property_value)
        with self._conn as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT into t_data(membership_id, property_id, value) values (?,?,?)", sqlite_data
            )
            data_id = cursor.lastrowid
        assert data_id is not None

        # Enable proeprty if disabled.
        with self._conn as conn:
            conn.execute("UPDATE t_property set is_dynamic=1 where property_id = ?", (property_id,))
            conn.execute("UPDATE t_property set is_enabled=1 where property_id = ?", (property_id,))

        # Add scenario tag if passed
        if scenario:
            scenario_id = self.get_scenario_id(scenario)
            if scenario_id is None:
                scenario_id = self.add_object(scenario, ClassEnum.Scenario, CollectionEnum.Scenario)
            self.execute_query("INSERT into t_tag(object_id,data_id) values (?,?)", (scenario_id, data_id))

        # Add text if passed
        if text:
            text_sqlite = []
            for key, value in text.items():
                text_class_id = self.query("select class_id from t_class where name = ?", (key,))[0][0]
                assert text_class_id
                text_sqlite.append((text_class_id, data_id, value))
            with self._conn as conn:
                conn.executemany("INSERT into t_text(class_id,data_id,value) VALUES(?,?,?)", text_sqlite)

        return data_id

    def add_property_from_records(
        self,
        records: list[dict],
        /,
        *,
        parent_class: ClassEnum,
        parent_object_name: str = SYSTEM_CLASS_NAME,
        collection: CollectionEnum,
        scenario: str,
        chunksize: int = 10_000,
    ) -> None:
        """Bulk insert multiple properties from a list of records."""
        parent_object_id = self.get_object_id(parent_object_name, class_name=parent_class)
        collection_id = self.get_collection_id(collection, parent_class=parent_class)
        collection_properties = self.query(
            f"select name, property_id from t_property where collection_id={collection_id}"
        )
        property_ids = {key: value for key, value in collection_properties}
        component_names = tuple(d["name"] for d in records)
        component_memberships_query = f"""
        SELECT
          t_object.name as name,
          membership_id
        FROM
          t_membership
        INNER JOIN
            t_object on t_membership.child_object_id = t_object.object_id
        WHERE
          t_membership.parent_object_id = {parent_object_id} AND
          t_object.name in ({", ".join(["?" for _ in range(len(component_names))])})
        """
        component_memberships = self.query(component_memberships_query, params=component_names)
        component_memberships_dict: dict = {key: value for key, value in component_memberships}

        if not component_memberships:
            raise KeyError(
                "Object do not exists on the database yet. "
                "Make sure you use `add_object` before adding properties."
            )

        sqlite_data = self._properties_to_sql_ingest(records, component_memberships_dict, property_ids)
        # Make properties dynamic on plexos
        with self._conn as conn:
            filter_property_ids = [d[1] for d in sqlite_data]
            for property_id in filter_property_ids:
                conn.execute("UPDATE t_property set is_dynamic=1 where property_id = ?", (property_id,))
                conn.execute("UPDATE t_property set is_enabled=1 where property_id = ?", (property_id,))

        with self._conn as conn:
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT into t_data(membership_id, property_id, value) values (?,?,?)", sqlite_data
            )

        scenario_id = self.get_scenario_id(scenario_name=scenario)
        for batch in batched(sqlite_data, chunksize):
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
            self.execute_query(scenario_query, params=params)

            data_ids_query = f"""
                SELECT
                  t_data.data_id,
                  t_data.property_id,
                  t_property.name as property_name,
                  t_data.value,
                  t_membership.membership_id,
                  t_membership.child_object_id
                FROM
                  t_data
                inner join t_property on t_data.property_id = t_property.property_id
                inner join t_membership on t_data.membership_id = t_membership.membership_id
                WHERE (t_membership.membership_id, t_data.property_id, value) in ({place_holders});
                """
            _ = self.query(data_ids_query, params=params)
        return

    def add_object(
        self,
        object_name: str,
        class_name: ClassEnum,
        collection_name: CollectionEnum,
        /,
        *,
        category_name: str = "-",
        description: str | None = None,
    ):
        """Add object to the database and append a system membership.

        The base type on the plexos database are objects. Each object can have
        multiple memberships and belong to predetermined class.

        Parameters
        ----------
        object_name
            Name to be added to the object
        class_id
            ClassEnum from the object to be added. E.g., for generators class_id=ClassEnum.Generators
        collection_name
            Collection for system membership. E.g., for generators class_enum=CollectionEnum.SystemGenerators
        parent_class_name
            Name of the parent class if different from System.

        Notes
        -----
        By default, we add all objects to the system membership.

        Raises
        ------
        sqlite.IntegrityError
            if an object is inserted without a unique name/class pair

        Returns
        -------
        int
            object_id
        """
        category_check = self.check_id_exists(Schema.Categories, category_name, class_name=class_name)
        if not category_check:
            category_id = self.add_category(category_name, class_name=class_name)
        else:
            category_id = self.get_category_id(category_name, class_name=class_name)

        class_id = self._get_id(Schema.Class, class_name.name)

        params = (object_name, class_id, category_id, str(uuid.uuid4()), description)
        placeholders = ", ".join("?" * len(params))
        with self._conn as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO {Schema.Objects.name}(name, class_id, category_id, GUID, description) "
                f"VALUES({placeholders})",
                params,
            )
            object_id = cursor.lastrowid

        if object_id is None:
            raise TypeError("Could not fetch the last row of the insert. Check query format.")

        # Add system membership
        self.add_membership(
            SYSTEM_CLASS_NAME,
            object_name,
            parent_class=ClassEnum.System,
            child_class=class_name,
            collection=collection_name,
            child_category=category_name,
        )
        return object_id

    def add_report(  # noqa: D102
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
        object_id = self.get_object_id(object_name, class_name=ClassEnum.Report)
        collection_id = self.get_collection_id(collection, parent_class=parent_class, child_class=child_class)
        query_string = "SELECT name from t_property_report where collection_id = ?"
        valid_properties = [d[0] for d in self.query(query_string, (collection_id,))]
        if property not in valid_properties:
            msg = (
                f"Property {property} does not exist for collection: {collection}. "
                "Check valid properties for the report type."
            )
            raise KeyError(msg)
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
        self.execute_query(
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

    def copy_object(
        self,
        original_object_name: str,
        new_object_name: str,
        object_type: ClassEnum,
        copy_properties: bool = True,
    ) -> int:
        """Copy an object and its properties, tags, and texts."""
        object_id = self.get_object_id(original_object_name, object_type)
        category_id = self.query("SELECT category_id from t_object WHERE object_id = ?", (object_id,))
        category_name = self.query("SELECT name from t_category WHERE category_id = ?", (category_id[0][0],))
        new_object_id = self.add_object(
            new_object_name, object_type, CollectionEnum.Generators, category_name=category_name[0][0]
        )
        if new_object_id is None:
            raise ValueError(f"Failed to add new object '{new_object_name}'.")

        # Copy memberships and obtain a mapping.
        membership_mapping = self._copy_object_memberships(original_object_name, new_object_name, object_type)

        if copy_properties:
            self._copy_properties_sql(membership_mapping)
            self._copy_tags(membership_mapping)
            self._copy_text(membership_mapping)
        return new_object_id

    def get_category_id(self, category_name: str, class_name: ClassEnum) -> int:
        """Return the ID for a given category.

        Parameters
        ----------
        category_name
            Name of the category to retrieve
        class_name
            ClassEnum of the class of the category. Used to filter memberships by `class_id`

        Returns
        -------
        int
            The ID corresponding to the category, or None if not found.

        Raises
        ------
        KeyError
            If ID does not exists on the database.
        ValueError
            If multiple IDs are returned for the given category.
        """
        return self._get_id(Schema.Categories, category_name, class_name=class_name)

    def get_collection_id(
        self,
        collection: CollectionEnum,
        parent_class: ClassEnum | None = None,
        child_class: ClassEnum | None = None,
    ) -> int:
        """Return the ID for a given collection.

        Parameters
        ----------
        collection : CollectionEnum
            The enum collection from which to retrieve the ID.
        parent_class, Optional
            ClassEnum of the parent object. Used to filter memberships by `parent_class_id`
        child_class, Optional
            ClassEnum of the child object. Used to filter memberships by `child_class_id`

        Returns
        -------
        int
            The ID corresponding to the object, or None if not found.

        Raises
        ------
        KeyError
            If ID does not exists on the database.
        ValueError
            If multiple IDs are returned for the given parent/child class provided.
        """
        return self._get_id(
            Schema.Collection, collection.name, parent_class_name=parent_class, child_class_name=child_class
        )

    def get_category_max_id(self, class_enum: ClassEnum) -> int:
        """Return the current max rank for a given category."""
        class_id = self._get_id(Schema.Class, class_enum.name)
        query = """
        SELECT
            max(rank)
        FROM
            t_category
        LEFT JOIN
            t_class ON t_class.class_id = t_category.class_id
        WHERE
            t_class.class_id = :class_id
        """
        return self.query(query, params={"class_id": class_id})[0][0]

    def get_class_id(self, class_enum: ClassEnum) -> int:
        """Return the ID for a given class.

        Parameters
        ----------
        class_name : ClassEnum
            The enum collection from which to retrieve the ID.

        Returns
        -------
        int
            The ID corresponding to the object, or None if not found.

        Raises
        ------
        KeyError
            If ID does not exists on the database.
        ValueError
            If multiple IDs are returned for the given class.
        """
        return self._get_id(Schema.Class, class_enum.name)

    def get_property_id(
        self,
        property_name: str,
        collection: CollectionEnum,
        child_class: ClassEnum,
        parent_class: ClassEnum | None = None,
    ) -> int:
        """Return the ID for a given collection.

        Parameters
        ----------
        property_name: str
            Name of the property to retrieve
        collection : CollectionEnum
            The enum collection from which to retrieve the ID. Used for filter by `collection_id`.
        parent_class
            ClassEnum of the parent object. Used to filter memberships by `parent_class_id`
        child_class
            ClassEnum of the child object. Used to filter memberships by `child_class_id`

        Returns
        -------
        int
            The ID corresponding to the object, or None if not found.

        Raises
        ------
        KeyError
            If name of the property does not exist for the collection id.
        ValueError
            If multiple IDs are returned for the given parent/child class provided.
        """
        valid_properties = self.get_valid_properties(
            collection, parent_class=parent_class, child_class=child_class
        )
        if property_name not in valid_properties:
            msg = (
                f"Property {property_name} does not exist for collection: {collection}. "
                f"Run `self.get_valid_properties({collection}) to verify valid properties."
            )
            raise KeyError(msg)

        collection_id = self.get_collection_id(collection, parent_class=parent_class, child_class=child_class)

        query_id = """
        SELECT
            property_id
        FROM `t_property`
        WHERE
            name = :property_name
        AND
            collection_id = :collection_id
        """
        params = {"property_name": property_name, "collection_id": collection_id}
        result = self.query(query_id, params)

        return result[0][0]  # Get first element of tuple

    def get_objects_properties(
        self,
        class_name: ClassEnum,
        collection: CollectionEnum,
        /,
        objects: str | Iterable[str] | None = None,
        *,
        properties: str | Iterable[str] | None = None,
        parent_class: ClassEnum | None = None,
        scenario: str | None = None,
        variable_tag: str | None = None,
    ) -> list[tuple[Any, ...]]:
        """Retrieve selected properties for the specified object.

        If no collection is provided, all properties are returned. If a collection is passed then
        the collection id is computed using the provided parent_class and the child class (class_name)
        so that only properties for that membership are returned.
        """
        extra_filters = ""
        params: list[Any] = self._normalize_names(objects, "objects")

        # Check if the passed properties exist on the collection, parent_class, child_class.
        properties = self._check_properties_names(
            properties,
            parent_class=parent_class or ClassEnum.System,
            child_class=class_name,
            collection=collection,
        )

        if params:
            placeholders = ", ".join("?" * len(params))
            extra_filters += f" AND o.name in ({placeholders})"

        if collection is not None:
            if parent_class is None:
                raise ValueError("When filtering by collection, parent_class must be provided.")
            coll_id = self.get_collection_id(collection, parent_class, child_class=class_name)
            extra_filters += " AND p.collection_id = ?"
            params.append(coll_id)

        if properties:
            prop_placeholders = ", ".join("?" for _ in properties)
            extra_filters += f" AND p.name IN ({prop_placeholders})"
            params.extend(properties)

        if scenario:
            extra_filters += " AND scenario.scenario_name = ?"
            params.append(scenario)

        if variable_tag:
            extra_filters += " AND p.tag = ?"
            params.append(variable_tag)

        # Load the SQL query from the file and substitute the placeholders.
        query_template = self._get_sql_query("property_query.sql")
        query = query_template.format(class_name=class_name.name, extra_filters=extra_filters)
        results = self.query(query, params)
        return results

    def get_object_id(self, object_name: str, class_name: ClassEnum, category_name: str | None = None) -> int:
        """Return the ID for a given object.

        Parameters
        ----------
        object_name
            Name of the object to find.
        category_name
            Name of the category to retrieve
        class_name
            ClassEnum of the class of the category. Used to filter memberships by `class_id`

        Returns
        -------
        int
            The ID corresponding to the category, or None if not found.

        Raises
        ------
        KeyError
            If ID does not exists on the database.
        ValueError
            If multiple IDs are returned for the given object.
        """
        return self._get_id(Schema.Objects, object_name, class_name=class_name, category_name=category_name)

    def check_id_exists(
        self, table: Schema, object_name: str, /, *, class_name: ClassEnum | None = None
    ) -> bool:
        """Check if the id exist for the given object and table.

        Parameters
        ----------
        table : Schema
            The table from which to retrieve the ID.
        object_name : ClassEnum
            The name of the object for which to retrieve the ID.

        Returns
        -------
        bool
            True if the object was found.

        Raises
        ------
        KeyError
            If ID does not exists on the database.
        ValueError
            If multiple IDs are returned for the given filters.

        """
        try:
            _ = self._get_id(table, object_name, class_name=class_name)
        except KeyError:
            return False
        except ValueError:
            raise ValueError(
                f"Multiple IDs returned for {object_name} and {class_name}. Try passing addtional filters"
            )
        return True

    def _get_id(
        self,
        table: Schema,
        object_name,
        /,
        *,
        class_name: ClassEnum | None = None,
        collection_name: CollectionEnum | None = None,
        parent_class_name: ClassEnum | None = None,
        child_class_name: ClassEnum | None = None,
        category_name: str | None = None,
    ) -> int:
        """Return the ID for a given table and object name combination.

        Parameters
        ----------
        table : Schema
            The table from which to retrieve the ID.
        object_name : str
            The name of the object for which to retrieve the ID.
        category_name
            Name of the category to retrieve
        class_name
            ClassEnum of the class of the category. Used to filter memberships by `class_id`
        parent_class, Optional
            ClassEnum of the parent object. Used to filter memberships by `parent_class_id`
        child_class, Optional
            ClassEnum of the child object. Used to filter memberships by `child_class_id`
        collection_name : CollectionEnum, optional
            The collection ID to filter by.

        Returns
        -------
        int or None
            The ID corresponding to the object, or None if not found.

        Raises
        ------
        KeyError
            If ID does not exists on the database.
        ValueError
            If multiple IDs are returned for the given filters.

        """
        table_name = table.name
        column_name = table.label
        params = {
            "object_name": object_name,
        }

        # tuple that should be unique for any id returned
        query_key = (table.name, object_name, class_name, parent_class_name, child_class_name)
        if query_key in self._QUERY_CACHE:
            return self._QUERY_CACHE[query_key]

        query = f"SELECT {column_name} FROM `{table_name}`"
        conditions = []
        join_clauses = []

        if class_name is not None:
            assert isinstance(class_name, ClassEnum)
            class_id = self._get_id(Schema.Class, class_name)
            conditions.append("class_id = :class_id")
            params["class_id"] = class_id

        if parent_class_name is not None:
            assert isinstance(parent_class_name, ClassEnum)
            parent_class_id = self._get_id(Schema.Class, parent_class_name.name)
            join_clauses.append(
                f" LEFT JOIN t_class as parent_class ON {table_name}.parent_class_id = parent_class.class_id"
            )
            conditions.append("parent_class_id = :parent_class_id")
            params["parent_class_id"] = parent_class_id

        if child_class_name is not None:
            assert isinstance(child_class_name, ClassEnum)
            child_class_id = self._get_id(Schema.Class, child_class_name.name)
            join_clauses.append(
                f" LEFT JOIN t_class AS child_class ON {table_name}.child_class_id = child_class.class_id"
            )
            conditions.append("child_class_id = :child_class_id")
            params["child_class_id"] = child_class_id

        if collection_name is not None:
            assert isinstance(collection_name, CollectionEnum)
            collection_id = self._get_id(Schema.Collection, collection_name.name)
            conditions.append("collection_id = :collection_id")
            params["collection_id"] = collection_id

        if category_name and class_name:
            category_id = self.get_category_id(category_name, class_name)
            conditions.append("category_id = :category_id")
            params["category_id"] = category_id

        # Build final query
        query += " ".join(join_clauses)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += (
            f" AND {table_name}.name = :object_name"
            if conditions
            else f" WHERE {table_name}.name = :object_name"
        )
        result = self.query(query, params)

        if not result:
            msg = f"No object found with the requested {params=}"
            raise KeyError(msg)

        if len(result) > 1:
            msg = f"Multiple ids returned for {object_name} and {class_name}. Try passing addtional filters"
            raise ValueError(msg)

        ret: int = result[0][0]  # Get first element of tuple

        self._QUERY_CACHE[query_key] = ret

        return ret

    def get_membership_id(
        self,
        /,
        *,
        child_name: str,
        parent_name: str,
        child_class: ClassEnum,
        parent_class: ClassEnum,
        collection: CollectionEnum,
    ):
        """Return the ID for a given membership.

        Parameters
        ----------
        child_name
            Name of the child to find. Used to filter `child_object_name`.
        parent_name
            Name of the parent to find. Used to filter `parent_object_name`.
        child_class
            ClassEnum of the parent object. Used to filter memberships by `parent_class_id`.
        parent_class
            ClassEnum of the class of the category. Used to filter memberships by `class_id`.
        collection
            The enum collection from which to retrieve the ID. Used to filter by `collection_id`.

        Returns
        -------
        int
            The ID corresponding to the object, or None if not found.

        Raises
        ------
        KeyError
            If ID does not exists on the database.
        ValueError
            If multiple IDs are returned for the given parent/child class provided.
        """
        # Get all the ids
        child_class_id = self._get_id(Schema.Class, child_class.name)
        parent_class_id = self._get_id(Schema.Class, parent_class.name)
        child_object_id = self.get_object_id(child_name, class_name=child_class)
        parent_object_id = self.get_object_id(parent_name, class_name=parent_class)
        collection_id = self.get_collection_id(collection, child_class=child_class, parent_class=parent_class)

        query_id = """
        SELECT
            membership_id
        FROM
            t_membership
        WHERE
            child_object_id = :child_object_id
        AND
            parent_object_id = :parent_object_id
        AND
            parent_class_id = :parent_class_id
        AND
            child_class_id = :child_class_id
        AND
            collection_id = :collection_id
        """
        params = {
            "child_object_id": child_object_id,
            "parent_object_id": parent_object_id,
            "collection_id": collection_id,
            "child_class_id": child_class_id,
            "parent_class_id": parent_class_id,
        }
        result = self.query(query_id, params)

        if not result:
            msg = f"No object found with the requested {params=}"
            raise KeyError(msg)

        if len(result) > 1:
            raise ValueError(
                f"Multiple ids returned for {parent_name}.{child_name}. Try passing addtional filters"
            )
        return result[0][0]

    def get_memberships(
        self,
        *object_names: str | list[str],
        object_class: ClassEnum,
        parent_class: ClassEnum | None = None,
        collection: CollectionEnum | None = None,
        include_system_membership: bool = False,
    ) -> list[tuple]:
        """Retrieve all memberships for the given object(s).

        Parameters
        ----------
        object_names : str or list[str]
            Name or list of names of the objects to get their memberships.
            You can pass multiple string arguments or a single list of strings.
        object_class : ClassEnum
            Class of the objects.
        parent_class : ClassEnum | None, optional
            Class of the parent object. Defaults to object_class if not provided.
        collection : CollectionEnum | None, optional
            Collection to filter memberships.
        include_system_membership : bool, optional
            If False (default), exclude system memberships (where parent_class is System).
            If True, include them.

        Returns
        -------
        list[tuple]
            A list of tuples representing memberships. Each tuple is structured as:
            (parent_class_id, child_class_id, parent_object_name, child_object_name, collection_id,
            return self.query(query_string=query_string, params=params)
            parent_class_name, collection_name).

        Raises
        ------
        KeyError
            If any of the object_names do not exist.
        """
        # Handle the case where a single list is provided as the only positional argument
        if len(object_names) == 1 and isinstance(object_names[0], list):
            object_names = tuple(object_names[0])

        object_ids = tuple(
            self._get_id(Schema.Objects, object_name, class_name=object_class) for object_name in object_names
        )
        if not object_ids:
            raise KeyError(f"Objects {object_names=} not found on the database. Check that they exist.")

        query_string = """
        SELECT
            mem.parent_class_id,
            mem.child_class_id,
            parent_object.name AS parent,
            child_object.name AS child,
            mem.collection_id,
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
        conditions = []
        if not include_system_membership:
            conditions.append(f"parent_class.name <> '{ClassEnum.System.name}'")
        if len(object_ids) == 1:
            conditions.append(
                f"(child_object.object_id = {object_ids[0]} OR parent_object.object_id = {object_ids[0]})"
            )
        else:
            conditions.append(
                f"(child_object.object_id in {object_ids} OR parent_object.object_id in {object_ids})"
            )
        if not parent_class:
            parent_class = object_class
        if collection:
            conditions.append(
                f"parent_class.name = '{parent_class.value}' and collections.name = '{collection.value}'"
            )
        if conditions:
            query_string += " WHERE " + " AND ".join(conditions)
        return self.query(query_string)

    def get_scenario_id(self, scenario_name: str) -> int:
        """Return scenario id for a given scenario name."""
        id_exists = self.check_id_exists(Schema.Objects, scenario_name, class_name=ClassEnum.Scenario)
        if id_exists:
            scenario_id = self.get_object_id(scenario_name, class_name=ClassEnum.Scenario)
        else:
            scenario_id = self.add_object(scenario_name, ClassEnum.Scenario, CollectionEnum.Scenarios)
        return scenario_id

    def get_valid_properties(
        self,
        collection: CollectionEnum,
        parent_class: ClassEnum | None = None,
        child_class: ClassEnum | None = None,
    ) -> list[str]:
        """Return list of valid property names per collection."""
        collection_id = self.get_collection_id(collection, parent_class=parent_class, child_class=child_class)
        query_string = "SELECT name from t_property where collection_id = ?"
        result = self.query(query_string, (collection_id,))
        return [d[0] for d in result]

    def execute_query(self, query: str, params=None) -> None:
        """Execute of insert query to the database."""
        with self._conn as conn:
            _ = conn.execute(query, params) if params else conn.execute(query)
        return

    def query(self, query_string: str, params=None) -> list[tuple]:
        """Execute of query to the database.

        This function just wraps the functionality of the SQLite API and fetch
        the results for us.


        Parameters
        ----------
        query
            String to get passed to the database connector.
        params
            Tuple or dict for passing

        Note
        ----
            We do not valid any query.

            This function could be slow depending the complexity of the query passed.
        """
        with self._conn as conn:
            res = conn.execute(query_string, params) if params else conn.execute(query_string)
        ret = res.fetchall()

        return ret

    def ingest_from_records(self, tag: str, record_data: Sequence):
        """Insert elements from xml to database."""
        logger.trace("Ingesting {}", tag)
        for record in record_data:
            # Add backticks so we can insert table names with protected names on SQL.
            # This is just to enable column names like "default", but also adds compatibility with MySQL
            columns = ", ".join([f"`{key}`" for key in record.keys()])
            str_replacement = ", ".join([f":{s}" for s in record.keys()])

            # We use SQLite string replcement to pass a dictionary to the insert query.
            # The format is "insert into `table`(column) values(:column)"
            ingestion_sql = f"insert into {tag} ({columns}) values({str_replacement})"
            logger.trace(ingestion_sql)

            # NOTE: We might want to have additional error checking at some point.
            # This should work for the mean time
            try:
                with self._conn as conn:
                    conn.execute(ingestion_sql, record)
            except sqlite3.Error as err:
                raise err

            logger.trace("Finished ingesting {}", tag)
        return

    def save(self, fpath: Path | str):
        """Save memory representation to file format."""
        fpath = fpath if isinstance(fpath, Path) else Path(fpath)
        with sqlite3.connect(fpath) as conn:
            self._conn.backup(conn)
        conn.close()
        logger.info("Backed up plexos database to {}", fpath)

    def to_xml(self, fpath: Path | str, namespace="http://tempuri.org/MasterDataSet.xsd") -> None:
        """Convert SQLite to XML format.


        This method takes all the tables of th SQLite and creates the
        appropiate tags based on the column name.

        Parameters
        ----------
        fpath
            Path to serialize the database
        namespace
            Plexos MasterDataSet URI

        """
        with self._conn as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()

            # Extract table names from the query result
            table_names = [table[0] for table in tables]

            # Create XML structure
            root = ET.Element("MasterDataSet")

            # Iterate over each table

            # Iterate over each table
            for table_name in table_names:
                # Fetch data from the table
                rows = self._fetch_table_data(cursor, table_name)
                if not rows:
                    continue

                column_types_tuple = self.query(f"SELECT name, type from pragma_table_info('{table_name}');")
                column_types: dict[str, str] = {key: value for key, value in column_types_tuple}

                # Create XML elements for the table
                self._create_table_element(root, column_types=column_types, table_name=table_name, rows=rows)

        # Create XML tree
        tree = ET.ElementTree(root)
        ET.indent(tree)

        # Sorting elements by their text
        sorted_elements = sorted(root.findall("*"), key=lambda e: e.tag)
        root[:] = sorted_elements

        # Rebuilding the XML tree with sorted elements
        logger.trace("Saving xml file")
        root.set("xmlns", namespace)
        with open(fpath, "wb") as f:
            tree.write(
                f,
            )
        logger.info("Saved xml file to {}", fpath)
        # tree.write(fpath, encoding="utf-8", xml_declaration=True)

    def _create_table_element(self, root, column_types: dict[str, str], table_name: str, rows: list[tuple]):
        """Create XML elements for a table."""
        for row in rows:
            table_element = ET.SubElement(root, table_name)
            for (column_name, column_type), column_value in zip(column_types.items(), row):
                if column_value is None:
                    continue
                column_element = ET.SubElement(table_element, column_name)
                match column_type:
                    case "BIT":
                        match column_value:
                            case 1:
                                column_element.text = "true"
                            case 0:
                                column_element.text = "false"
                    case _:
                        column_element.text = str(column_value)

    def _fetch_table_data(self, cursor, table_name):
        """Fetch data from a table."""
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall()

    def _sqlite_config(self):
        """Call all sqlite configuration prior schema creation."""
        with self._conn as conn:
            conn.execute("PRAGMA synchronous = OFF")  # Make it asynchronous
            conn.execute("PRAGMA journal_mode = OFF")  # Make it asynchronous

    def _properties_to_sql_ingest(
        self, component_properties: list[dict], memberships: dict, property_ids: dict
    ) -> list[tuple[int, int, Any]]:
        """Convert a list of properties into a list of tuples with memberships for SQL ingestion."""
        data = component_properties.copy()
        result = []
        for component in data:
            name = component.pop("name")
            membership_id = memberships[name]
            # Iterate through each key-value pair in the dictionary
            for key, value in component.items():
                property_id = property_ids.get(key, None)
                if property_id is not None:
                    result.append((membership_id, property_id, value))
        return result

    def _create_table_schema(self) -> None:
        fpath = files("plexosdb").joinpath("schema.sql")
        logger.debug("Using {} for creating plexos schema.", fpath)

        with self._conn as conn:
            conn.executescript(fpath.read_text(encoding="utf-8-sig"))
        logger.trace("Schema created successfully", fpath)
        return None

    def _populate_database(self, xml_fname: str | None, xml_handler: XMLHandler | None = None):
        fpath = xml_fname
        if fpath is None and not xml_handler:
            msg = (
                "Base XML file was not provided. "
                "Make sure that you are passing either `xml_fname` or xml_handler`."
            )
            raise FileNotFoundError(msg)

        if not xml_handler:
            xml_handler = XMLHandler.parse(fpath=fpath)  # type: ignore

        # Start data ingestion to the datbase
        xml_tags = set([e.tag for e in xml_handler.root])  # Extract set of valid tags from xml
        for tag in xml_tags:
            schema = str2enum(tag)
            if schema:
                record_dict = xml_handler.get_records(schema)
                self.ingest_from_records(tag, record_dict)

    def _create_collations(self) -> None:
        """Add collate function for helping search enums."""
        self._conn.create_collation("NOSPACE", no_space)
        return

    def execute(self, query: str, params: tuple | dict | None = None) -> None:
        """Execute a SQL statement without returning any results.

        Parameters
        ----------
        query : str
            The SQL query to execute.
        params : tuple or dict, optional
            Parameters to pass with the query.
        """
        conn = self._conn
        try:
            if params:
                conn.execute(query, params)
            else:
                conn.execute(query)
            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
            raise e

    def last_insert_rowid(self) -> int:
        """Return the last inserted row ID."""
        with self._conn as conn:
            cursor = conn.execute("SELECT last_insert_rowid();")
            return cursor.fetchone()[0]

    def _get_sql_query(self, query_name: str):
        fpath = files("plexosdb.queries").joinpath(query_name)
        return fpath.read_text(encoding="utf-8-sig")

    def _execute_insert(self, query: str, params: tuple) -> int:
        """Execute an insert query and return the last inserted row ID.

        Parameters
        ----------
        query : str
            The SQL insert query to execute.
        params : tuple
            Parameters to bind to the query.

        Returns
        -------
        int
            The last inserted row ID.

        Raises
        ------
        TypeError
            If the last row ID is None.
        """
        with self._conn as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            last_id = cursor.lastrowid
        if last_id is None:
            raise TypeError("Could not fetch the last row id. Check query format.")
        return last_id

    def _execute_with_mapping(self, membership_mapping: dict[int, int], query_template: str) -> None:
        """Filter the membership_mapping and execute a formatted query."""
        valid_mapping = {}
        for old, new in membership_mapping.items():
            data_rows = self.query("SELECT 1 FROM t_data WHERE membership_id = ? LIMIT 1", (old,))
            if data_rows:
                valid_mapping[old] = new

        if not valid_mapping:
            return

        mapping_values = ", ".join(f"({old}, {new})" for old, new in valid_mapping.items())
        query = query_template.format(mapping_values=mapping_values)
        self.execute(query, {})

    def _copy_object_memberships(
        self, original_object_name: str, object_name: str, object_type: ClassEnum
    ) -> dict[int, int]:
        membership_mapping: dict[int, int] = {}
        all_memberships = self.get_memberships(
            original_object_name, object_class=object_type, include_system_membership=True
        )
        # Loop first where the object is the child
        for mem in all_memberships:
            parent_name = mem[2]
            child_name = mem[3]
            parent_class_name = mem[5]
            child_class_name = mem[6]
            collection_name = mem[7]
            if child_name == original_object_name:
                try:
                    existing_membership_id = self.get_membership_id(
                        child_name=object_name,
                        parent_name=parent_name,
                        child_class=ClassEnum[child_class_name],
                        parent_class=ClassEnum[parent_class_name],
                        collection=CollectionEnum[collection_name],
                    )
                except KeyError:
                    existing_membership_id = self.add_membership(
                        parent_name,
                        object_name,
                        parent_class=ClassEnum[parent_class_name],
                        child_class=ClassEnum[child_class_name],
                        collection=CollectionEnum[collection_name],
                    )
                old_mem_id = self.get_membership_id(
                    child_name=original_object_name,
                    parent_name=parent_name,
                    child_class=ClassEnum[child_class_name],
                    parent_class=ClassEnum[parent_class_name],
                    collection=CollectionEnum[collection_name],
                )
                membership_mapping[old_mem_id] = existing_membership_id
        # Now loop over when the object is the parent
        for mem in all_memberships:
            parent_name = mem[2]
            child_name = mem[3]
            parent_class_name = mem[5]
            child_class_name = mem[6]
            collection_name = mem[7]
            if parent_name == original_object_name:
                new_mem_id = self.add_membership(
                    object_name,
                    child_name,
                    parent_class=ClassEnum[parent_class_name],
                    child_class=ClassEnum[child_class_name],
                    collection=CollectionEnum[collection_name],
                )
                old_mem_id = self.get_membership_id(
                    child_name=child_name,
                    parent_name=original_object_name,
                    child_class=ClassEnum[child_class_name],
                    parent_class=ClassEnum[parent_class_name],
                    collection=CollectionEnum[collection_name],
                )
                membership_mapping[old_mem_id] = new_mem_id

        return membership_mapping

    def _copy_properties_sql(
        self,
        membership_mapping: dict[int, int],
    ) -> None:
        """
        Copy all t_data rows corresponding to properties from the original object into new rows
        for the new object.
        """
        query_template = """
        WITH mapping(old_membership_id, new_membership_id) AS (
            VALUES {mapping_values}
        )
        INSERT INTO t_data (membership_id, property_id, value, state)
        SELECT m.new_membership_id,
                d.property_id,
                d.value,
                d.state
        FROM mapping m
        JOIN t_data d ON d.membership_id = m.old_membership_id;
        """
        self._execute_with_mapping(membership_mapping, query_template)

    def _copy_tags(self, membership_mapping: dict[int, int]) -> None:
        """Copy all t_tag rows for the original object's t_data rows to new t_tag rows."""
        query_template = """
        WITH mapping (old_membership_id, new_membership_id) AS (
            VALUES {mapping_values}
        ),
        old_data AS (
            SELECT data_id, property_id,
                ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY data_id) AS rn
            FROM t_data d
            JOIN mapping m ON d.membership_id = m.old_membership_id
        ),
        new_data AS (
            SELECT data_id, property_id,
                ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY data_id) AS rn
            FROM t_data d
            JOIN mapping m ON d.membership_id = m.new_membership_id
        )
        INSERT INTO t_tag (data_id, object_id, state, action_id)
        SELECT new_d.data_id, t.object_id, t.state, t.action_id
        FROM old_data old_d
        JOIN new_data new_d USING (property_id, rn)
        JOIN t_tag t ON t.data_id = old_d.data_id;
        """
        self._execute_with_mapping(membership_mapping, query_template)

    def _copy_text(self, membership_mapping: dict[int, int]) -> None:
        """Copy all t_text rows from the original t_data rows to the new ones."""
        query_template = """
        WITH mapping (old_membership_id, new_membership_id) AS (
            VALUES {mapping_values}
        ),
        old_data AS (
            SELECT data_id, property_id,
                ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY data_id) AS rn
            FROM t_data d
            JOIN mapping m ON d.membership_id = m.old_membership_id
        ),
        new_data AS (
            SELECT data_id, property_id,
                ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY data_id) AS rn
            FROM t_data d
            JOIN mapping m ON d.membership_id = m.new_membership_id
        )
        INSERT INTO t_text (data_id, class_id, value, state, action_id)
        SELECT new_d.data_id, t.class_id, t.value, t.state, t.action_id
        FROM old_data old_d
        JOIN new_data new_d USING (property_id, rn)
        JOIN t_text t ON t.data_id = old_d.data_id;
        """
        self._execute_with_mapping(membership_mapping, query_template)

    def _duplicate_data_row(self, data_id: int) -> int:
        """Duplicate a row from t_data and return the new data_id."""
        row = self.query(
            "SELECT membership_id, property_id, value FROM t_data WHERE data_id = ?",
            (data_id,),
        )
        if not row:
            raise ValueError(f"Data row with id {data_id} not found.")
        membership_id, property_id, value = row[0]
        self.execute(
            "INSERT INTO t_data (membership_id, property_id, value) VALUES (?, ?, ?)",
            (membership_id, property_id, value),
        )
        return self.last_insert_rowid()

    def _update_band_for_data(self, data_id: int, band: str | None, duplicate: bool = True) -> None:
        """Update or insert a band record for the given data_id."""
        if band is None:
            upsert_query = (
                "INSERT INTO t_band (data_id, band_id, state) VALUES (?, NULL, 1) "
                "ON CONFLICT(data_id, band_id) DO UPDATE SET state = 1"
            )
            self.execute(upsert_query, (data_id,))
        else:
            existing = self.query(
                "SELECT band_id FROM t_band WHERE data_id = ? AND band_id = ?",
                (data_id, band),
            )
            if existing:
                upsert_query = (
                    "INSERT INTO t_band (data_id, band_id, state) VALUES (?, ?, 1) "
                    "ON CONFLICT(data_id, band_id) DO UPDATE SET state = 1"
                )
                self.execute(upsert_query, (data_id, band))
            else:
                if duplicate:
                    new_data_id = self._duplicate_data_row(data_id)
                    value = self.query(
                        "SELECT value FROM t_data WHERE data_id = ?",
                        (data_id,),
                    )[0][0]
                    membership_id, property_id = self.query(
                        "SELECT membership_id, property_id FROM t_data WHERE data_id = ?",
                        (data_id,),
                    )[0]
                    self.execute(
                        "INSERT INTO t_data (membership_id, property_id, value) VALUES (?, ?, ?)",
                        (membership_id, property_id, value),
                    )
                    self.execute(
                        "INSERT INTO t_band (data_id, band_id, state) VALUES (?, ?, 1)",
                        (new_data_id, band),
                    )
                else:
                    value = self.query(
                        "SELECT value FROM t_data WHERE data_id = ?",
                        (data_id,),
                    )[0][0]
                    membership_id, property_id = self.query(
                        "SELECT membership_id, property_id FROM t_data WHERE data_id = ?",
                        (data_id,),
                    )[0]
                    self.execute(
                        "INSERT INTO t_data (membership_id, property_id, value) VALUES (?, ?, ?)",
                        (membership_id, property_id, value),
                    )
                    self.execute(
                        "INSERT INTO t_band (data_id, band_id, state) VALUES (?, ?, 1)",
                        (data_id, band),
                    )

    def _update_scenario_for_data(self, data_id: int, scenario: str) -> None:
        """Update or insert a scenario association for a given data_id."""
        scenario_id = self.get_scenario_id(scenario)
        self.execute_query(
            "INSERT into t_tag(object_id, data_id) VALUES (?,?)",
            (scenario_id, data_id),
        )

    def modify_property(
        self,
        object_type: ClassEnum,
        object_name: str,
        property_name: str,
        new_value: str | None,
        scenario: str | None = None,
        band: str | None = None,
        collection: CollectionEnum | None = None,
        parent_class: ClassEnum | None = None,
    ) -> None:
        """Modify the property value for a given object.

        Parameters
        ----------
        object_type : ClassEnum
            The type of object (e.g., Generator).
        object_name : str
            The name of the object.
        property_name : str
            The property name to modify.
        new_value : str | None
            The new value to apply.
        scenario : str | None, optional
            Scenario for updating, by default None.
        band : str | None, optional
            Band for updating, by default None.
        collection : CollectionEnum | None, optional
            The collection, by default None.
        parent_class : ClassEnum | None, optional
            The parent class, by default None.
        """
        if not (object_name and property_name):
            raise ValueError("Both object_name and property_name must be provided.")
        collection = collection or CollectionEnum.Generators
        parent_class = parent_class or ClassEnum.System
        coll_id = self.get_collection_id(collection, parent_class, object_type)
        valid_props = self.get_valid_properties(
            collection=collection,
            parent_class=parent_class,
            child_class=object_type,
        )
        if property_name not in valid_props:
            raise PropertyNameError(
                f"Property '{property_name}' is not valid for {object_type.value} objects "
                f"in collection {collection.value} with parent class {parent_class.value}."
            )
        select_query = (
            "SELECT d.data_id "
            "FROM t_object o "
            "JOIN t_class c ON o.class_id = c.class_id "
            "JOIN t_membership m ON m.child_object_id = o.object_id "
            "JOIN t_data d ON d.membership_id = m.membership_id "
            "JOIN t_property p ON d.property_id = p.property_id "
            "WHERE c.name = ? AND o.name = ? AND p.name = ? AND p.collection_id = ?"
        )
        data_rows = self.query(select_query, (object_type.value, object_name, property_name, coll_id))
        data_ids = [row[0] for row in data_rows]
        if scenario is not None and band is not None:
            for data_id in data_ids:
                new_data_id = self._duplicate_data_row(data_id)
                self.execute(
                    "UPDATE t_data SET value = ? WHERE data_id = ?",
                    (new_value, new_data_id),
                )
                self._update_band_for_data(new_data_id, band, duplicate=False)
                self._update_scenario_for_data(new_data_id, scenario)
        else:
            for data_id in data_ids:
                if scenario is not None:
                    new_data_id = self._duplicate_data_row(data_id)
                    self.execute(
                        "UPDATE t_data SET value = ? WHERE data_id = ?",
                        (new_value, new_data_id),
                    )
                    self._update_scenario_for_data(new_data_id, scenario)
                    if band is not None:
                        self._update_band_for_data(new_data_id, band, duplicate=False)
                else:
                    if new_value is not None:
                        update_query = "UPDATE t_data SET value = ? WHERE data_id = ?"
                        self.execute(update_query, (new_value, data_id))
                    if band is not None:
                        self._update_band_for_data(data_id, band)

    def _normalize_names(self, value: str | Iterable[str] | None, var_name: str) -> list[str]:
        """Normalize a name or list of names into a unique list of strings."""
        if value is None:
            return []
        if isinstance(value, str):
            names = [value]
        elif hasattr(value, "__iter__"):
            names = list(set(value))
        else:
            raise ValueError(f"{var_name} must be a string or an iterable of strings.")
        if not names:
            raise ValueError(f"No {var_name} provided.")
        return names

    def _check_properties_names(
        self,
        property_names: str | Iterable | None,
        parent_class: ClassEnum,
        child_class: ClassEnum,
        collection: CollectionEnum,
    ):
        if property_names is None:
            return []
        property_names = self._normalize_names(property_names, "property_names")
        valid_props = self.get_valid_properties(
            collection=collection,
            parent_class=parent_class,
            child_class=ClassEnum.Generator,
        )
        invalid = [prop for prop in property_names if prop not in valid_props]
        if invalid:
            msg = (
                f"Invalid properties for {ClassEnum.Generator.value} "
                f"objects in collection {collection.value}"
            )
            raise PropertyNameError(msg)
        return property_names

    def _process_update_for_data_id(
        self, data_id: int, new_value: str | None, scenario: str | None, band: str | None
    ) -> None:
        if scenario is not None:
            new_id = self._duplicate_data_row(data_id)
            self.execute(
                "UPDATE t_data SET value = ? WHERE data_id = ?",
                (new_value, new_id),
            )
            self._update_scenario_for_data(new_id, scenario)
            if band is not None:
                self._update_band_for_data(new_id, band, duplicate=False)
        else:
            if new_value is not None:
                self.execute(
                    "UPDATE t_data SET value = ? WHERE data_id = ?",
                    (new_value, data_id),
                )
            if band is not None:
                self._update_band_for_data(data_id, band)

    def bulk_modify_properties(self, updates: list[dict]) -> None:
        """Update multiple properties in a single transaction."""
        try:
            self._conn.execute("BEGIN")
            for upd in updates:
                object_type = upd["object_type"]
                object_name = upd["object_name"]
                property_name = upd["property_name"]
                new_value = upd["new_value"]
                scenario = upd.get("scenario")
                band = upd.get("band")
                collection = upd.get("collection", CollectionEnum.Generators)
                parent_class = upd.get("parent_class", ClassEnum.System)

                coll_id = self.get_collection_id(collection, parent_class, object_type)
                select_query = """
                SELECT d.data_id
                FROM t_object o
                JOIN t_class c ON o.class_id = c.class_id
                JOIN t_membership m ON m.child_object_id = o.object_id
                JOIN t_data d ON d.membership_id = m.membership_id
                JOIN t_property p ON d.property_id = p.property_id
                WHERE c.name = ? AND o.name = ? AND p.name = ? AND p.collection_id = ?
                """
                data_rows = self.query(select_query, (object_type.value, object_name, property_name, coll_id))
                data_ids = [row[0] for row in data_rows]
                for data_id in data_ids:
                    self._process_update_for_data_id(data_id, new_value, scenario, band)
            if self._conn.in_transaction:
                self._conn.commit()
        except Exception as e:
            if self._conn.in_transaction:
                self._conn.rollback()
            raise e
