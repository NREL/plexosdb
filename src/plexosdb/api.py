"""Simple API for interacting with the Plexos database."""

from collections.abc import Iterable
from pathlib import Path
from typing import Any

from plexosdb.enums import ClassEnum, CollectionEnum
from plexosdb.sqlite import PlexosSQLite


class PlexosDB:
    """High-level API for plexosdb."""

    def __init__(self, xml_fname: str | None = None) -> None:
        """Initialize the API using an XML file or other data sources.

        Parameters
        ----------
        xml_fname : str | None
            The XML filename to ingest data from. If None, uses in-memory DB.

        Examples
        --------
        >>> api = PlexosDB(xml_fname="plexosdb.xml")
        """
        self._db = PlexosSQLite(xml_fname=xml_fname)

    def add_object(
        self,
        object_name: str,
        class_enum: ClassEnum,
        collection_enum: CollectionEnum,
        /,
        *,
        category: str = "-",
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
        return self._db.add_object(
            object_name, class_enum, collection_enum, category_name=category, description=description
        )

    def backup_database(self, target_path: str | Path) -> None:
        """Backup the in-memory database to a file.

        Parameters
        ----------
        target_path : str | Path
            The file path where the database backup will be saved.

        Examples
        --------
        >>> api.backup_database("backup.db")
        """
        self._db.save(target_path)

    def bulk_modify_properties(self, new_properties: list[dict]) -> None:
        """Update multiple properties in a single transaction."""
        return self._db.bulk_modify_properties(new_properties)

    def copy_object(
        self,
        original_object_name: str,
        new_object_name: str,
        object_type: ClassEnum,
        copy_properties: bool = True,
    ) -> int:
        """Copy an object and its properties, tags, and texts."""
        return self._db.copy_object(
            original_object_name, new_object_name, object_type, copy_properties=copy_properties
        )

    def get_all_generator_properties(
        self, parent_class: ClassEnum | None = None, collection: CollectionEnum | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve all generator properties without filtering by parent class or collection.

        Returns
        -------
        list[dict]
            Dictionary entries include: generator, property, value, texts,
            tags, bands, scenario, scenario_category, and unit.
        """
        results = self._db.get_objects_properties(
            ClassEnum.Generator,
            collection or CollectionEnum.Generators,
            parent_class=parent_class or ClassEnum.System,
        )
        return [
            {
                "generator": row[0],
                "property": row[1],
                "value": row[2],
                "texts": row[3],
                "tags": row[4],
                "bands": row[5],
                "scenario": row[6],
                "scenario_category": row[7],
                "unit": row[8],
            }
            for row in results
        ]

    def get_generator_properties(
        self,
        generator_names: str | Iterable[str],
        properties_names: str | Iterable[str] | None = None,
        /,
        *,
        parent_class: ClassEnum | None = None,
        collection: CollectionEnum | None = None,
        scenario: str | None = None,
        variable_tag: str | None = None,
    ) -> list[dict]:
        """Retrieve selected properties for the specified generator(s).

        Parameters
        ----------
        generator_names : str | Iterable[str]
            A single generator name or an iterable of generator names.
        property_names : str | Iterable[str] | None, optional
            Property names to filter by, by default None.
        scenario : str | None, optional
            Scenario name to filter by, by default None.
        variable_tag : str | None, optional
            Tag for filtering properties, by default None.
        collection : CollectionEnum | None, optional
            Collection enum to use for filtering, by default None.
        parent_class : ClassEnum | None, optional
            Parent class for filtering, by default None.

        Returns
        -------
        list[dict]
            Dictionary entries include: generator, property, value, texts, tags,
            bands, scenario, scenario_category, and unit.

        Raises
        ------
        ValueError
            If generator_names or property_names are not provided properly.
        PropertyNameError
            If an invalid property name is provided.
        """
        results = self._db.get_objects_properties(
            ClassEnum.Generator,
            collection or CollectionEnum.Generators,
            generator_names,
            properties=properties_names,
            parent_class=parent_class or ClassEnum.System,
            scenario=scenario,
            variable_tag=variable_tag,
        )
        return [
            {
                "generator": row[0],
                "property": row[1],
                "value": row[2],
                "texts": row[3],
                "tags": row[4],
                "bands": row[5],
                "scenario": row[6],
                "scenario_category": row[7],
                "unit": row[8],
            }
            for row in results
        ]

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
        return self._db.modify_property(
            object_type, object_name, property_name, new_value, scenario, band, collection, parent_class
        )

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
        return self._db.query(query_string=query_string, params=params)

    def to_xml(self, target_path: str | Path) -> None:
        """Export the current database content to an XML file.

        Parameters
        ----------
        target_path : str | Path
            The file path where the XML export will be saved.
        """
        self._db.to_xml(target_path)

    def _normalize_names(self, value: str | Iterable[str], var_name: str) -> list[str]:
        """Normalize a name or list of names into a unique list of strings."""
        if isinstance(value, str):
            names = [value]
        elif hasattr(value, "__iter__"):
            names = list(set(value))
        else:
            raise ValueError(f"{var_name} must be a string or an iterable of strings.")
        if not names:
            raise ValueError(f"No {var_name} provided.")
        return names
