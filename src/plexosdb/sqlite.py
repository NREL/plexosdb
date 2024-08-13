"""Simple interface with the Plexos database."""

import sqlite3
import uuid
import xml.etree.ElementTree as ET  # noqa: N817
from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path
from typing import Any

from loguru import logger

from .utils import batched
from .enums import ClassEnum, CollectionEnum, Schema, str2enum
from .xml_handler import XMLHandler

SYSTEM_CLASS_NAME = "System"
MASTER_FILE = files("plexosdb").joinpath("master.xml")


class PlexosSQLite:
    """Class that wraps the connection to the SQL database.

    Since we always start from a file XML, the default behaviour is to create an in-memory representation
    of the database. The usage is not to persist it into disk since the output file is always a XML, but it is
    possible by using the method `backup`.
    """

    DB_FILENAME = "plexos.db"
    _conn: sqlite3.Connection

    def __init__(self, xml_fname: str | None = None, xml_handler: XMLHandler | None = None) -> None:
        super().__init__()
        self._conn = sqlite3.connect(":memory:")
        self._sqlite_config()
        self._create_table_schema()
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
        with self._conn as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO {Schema.AttributeData.name}(object_id, attribute_id, value) "
                f"VALUES({placeholders})",
                params,
            )
            attribute_id = cursor.lastrowid  # type: ignore
        if attribute_id is None:
            raise TypeError("Could not fetch the last row of the insert. Check query format.")
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
        existing_rank = self.query(
            rank_query,
            {"class_id": class_id},
        )[0][0]

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
        object_id = self.get_object_id(object_name, class_name=object_class)
        collection_id = self.get_collection_id(
            collection, child_class=object_class, parent_class=parent_class
        )
        valid_properties = self.get_valid_properties(collection_id)
        if property_name not in valid_properties:
            msg = (
                f"Property {property_name} does not exist for collection: {collection}. "
                f"Run `self.get_valid_properties({ collection }) to verify valid properties."
            )
            raise KeyError(msg)
        property_id = self.get_property_id(
            property_name, collection=collection, child_class=object_class, parent_class=parent_class
        )
        assert object_id is not None

        # Add system membership
        parent_object_name = parent_object_name or SYSTEM_CLASS_NAME  # Default to system class
        parent_class = parent_class or ClassEnum.System

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
            scenario_id = self.check_id_exists(Schema.Objects, scenario, class_name=ClassEnum.Scenario)
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
              inner join t_object on t_membership.child_object_id = t_object.object_id
            WHERE
              t_membership.parent_object_id = {parent_object_id} and
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
        collection_id
            Collection for system membership. E.g., for generators class_enum=CollectionEnum.SystemGenerators

        Notes
        -----
        By default, we add all objects to the system membership.

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
        collection_id = self.get_collection_id(collection, parent_class=parent_class, child_class=child_class)
        valid_properties = self.get_valid_properties(collection_id)

        if property_name not in valid_properties:
            msg = (
                f"Property {property_name} does not exist for collection: {collection}. "
                f"Run `self.get_valid_properties({ collection }) to verify valid properties."
            )
            raise KeyError(msg)

        query_id = """
        SELECT
            property_id
        FROM `t_property`
        WHERE
            name = :property_name
        AND collection_id = :collection_id
        """
        params = {"property_name": property_name, "collection_id": collection_id}
        result = self.query(query_id, params)

        return result[0][0]  # Get first element of tuple

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
            raise
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
        collection_id : CollectionEnum, optional
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

        query_id = f"SELECT {column_name} FROM `{table_name}` WHERE name = :object_name"
        params = {
            "object_name": object_name,
        }
        if class_name:
            class_id = self._get_id(Schema.Class, class_name)
            query_id += " and class_id = :class_id"
            params["class_id"] = class_id

        if parent_class_name:
            parent_class_id = self._get_id(Schema.Class, parent_class_name.name)
            query_id += " and parent_class_id = :parent_class_id"
            params["parent_class_id"] = parent_class_id

        if child_class_name:
            child_class_id = self._get_id(Schema.Class, child_class_name.name)
            query_id += " and child_class_id = :child_class_id"
            params["child_class_id"] = child_class_id

        if category_name and class_name:
            category_id = self.get_category_id(category_name, class_name)
            query_id += " and category_id = :category_id"
            params["category_id"] = category_id

        result = self.query(query_id, params)

        if not result:
            msg = f"No object found with the requested {params=}"
            raise KeyError(msg)

        if len(result) > 1:
            msg = f"Multiple ids returned for {object_name} and {class_name}. Try passing addtional filters"
            raise ValueError(msg)
        return result[0][0]  # Get first element of tuple

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
        *object_names: list[str],
        object_class: ClassEnum,
        parent_class: ClassEnum | None = None,
        collection: CollectionEnum | None = None,
    ) -> list[tuple]:
        """Retrieve all memberships for the given object except the system membership.

        This function returns a list of tuples representing memberships, which can be filtered by
        `parent_class` and `collection` if specified.

        Parameters
        ----------
        object_names
            Name of the objects to get their memberships
        object_class
            Class of the objects
        parent_class, Optional
            Class of the parent object. Used to filter memberships by `parent_class_id`
        collection, Optional
            Collection of the memberships. Used to filter memberships by `collection_id`

        Returns
        -------
        list of tuple
            A list of tuples representing memberships. Each tuple is structured as:
            (parent_class_id, child_class_id, parent_object_name, child_object_name, collection_id).

        Raises
        ------
        KeyError
            If any of the `object_names` do not exist or if the `object_class` is invalid.
        sqlite3.OperationalError
            If there is an error in executing the SQL query.

        Examples
        --------
        >>> db = PlexosSQLite("2-bus_example.xml")
        >>> memberships = db.get_memberships("SolarPV_01", "ThermalCC", object_class=ClassEnum.Generators)
        >>> print(memberships)
        [(parent_class_id, child_class_id, parent_object_name, child_object_name, collection_id)]
        """
        object_ids = tuple(
            self._get_id(Schema.Objects, object_name, class_name=object_class) for object_name in object_names
        )
        if not object_ids:
            raise KeyError(f"Objects {object_names=} not found on the database. Check that they exists.")

        query_string = """
        SELECT
            mem.parent_class_id,
            mem.child_class_id,
            parent_object.name parent,
            child_object.name child,
            mem.collection_id,
            parent_class.name AS parent_class_name,
            collections.name AS collection_name
        FROM
            t_membership as mem
        INNER JOIN
            t_object AS parent_object ON mem.parent_object_id = parent_object.object_id
        INNER JOIN
            t_object AS child_object ON mem.child_object_id = child_object.object_id
        LEFT JOIN
            t_class AS parent_class ON mem.parent_class_id = parent_class.class_id
        LEFT JOIN
            t_collection AS collections ON mem.collection_id = collections.collection_id
        WHERE
            mem.parent_class_id <> 1
        """

        if len(object_ids) == 1:
            query_string += (
                f"AND (child_object.object_id = {object_ids[0]} OR parent_object.object_id = {object_ids[0]})"
            )
        else:
            query_string += (
                f"AND (child_object.object_id in {object_ids} OR parent_object.object_id in {object_ids})"
            )
        if not parent_class:
            parent_class = object_class
        if collection:
            query_string += (
                f"and parent_class.name = '{parent_class.value}' and collections.name = '{collection.value}'"
            )
        return self.query(query_string)

    def get_scenario_id(self, scenario_name: str) -> int:
        """Return scenario id for a given scenario name."""
        scenario_id = self.check_id_exists(Schema.Objects, scenario_name, class_name=ClassEnum.Scenario)
        if scenario_id is None:
            scenario_id = self.add_object(scenario_name, ClassEnum.Scenario, CollectionEnum.Scenario)
        return scenario_id

    def get_valid_properties(self, collection_id: int) -> list[str]:
        """Return list of valid property names per collection."""
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
        fetchone
            Return firstrow

        Note
        ----
            We do not valid any query.

            This function could be slow depending the complexity of the query passed.
        """
        with self._conn as conn:
            res = conn.execute(query_string, params) if params else conn.execute(query_string)
        return res.fetchall()

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
            # conn.execute("PRAGMA journal_mode = OFF")  # Make it asynchronous

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
            fpath = MASTER_FILE  # type: ignore
            logger.debug("Using {} as default file", fpath)

        if not xml_handler:
            xml_handler = XMLHandler.parse(fpath=fpath)  # type: ignore

        # Start data ingestion to the datbase
        xml_tags = set([e.tag for e in xml_handler.root])  # Extract set of valid tags from xml
        for tag in xml_tags:
            schema = str2enum(tag)
            if schema:
                record_dict = xml_handler.get_records(schema)
                self.ingest_from_records(tag, record_dict)
