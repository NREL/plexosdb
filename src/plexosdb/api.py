"""Simple API for interacting with the Plexos database."""

from collections.abc import Iterable
from pathlib import Path

from plexosdb.enums import ClassEnum, CollectionEnum
from plexosdb.exceptions import PropertyNameError
from plexosdb.sqlite import PlexosSQLite


class PlexosAPI:
    """High-level API for plexosdb."""

    def __init__(self, xml_fname: str | None = None) -> None:
        """Initialize the API using an XML file or other data sources.

        Parameters
        ----------
        xml_fname : str | None
            The XML filename to ingest data from. If None, uses in-memory DB.

        Examples
        --------
        >>> api = PlexosAPI(xml_fname="plexosdb.xml")
        """
        self.db = PlexosSQLite(xml_fname=xml_fname)

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

    def get_generator_properties(
        self,
        generator_names: str | Iterable[str],
        property_names: str | Iterable[str] | None = None,
        scenario: str | None = None,
        variable_tag: str | None = None,
        collection: CollectionEnum | None = None,
        parent_class: ClassEnum | None = None,
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

        Examples
        --------
        >>> props = api.get_generator_properties("SolarPV01", "Heat Rate")
        >>> props[0]["generator"]
        'SolarPV01'

        See Also
        --------
        get_all_generator_properties : Retrieve properties for all generators.
        """
        # Normalize generator names.
        generator_names = self._normalize_names(generator_names, "generator_names")

        parent_class = parent_class or ClassEnum.System

        # Normalize and validate property_names if provided.
        if property_names is not None:
            property_names = self._normalize_names(property_names, "property_names")
            collection = collection or CollectionEnum.Generators
            valid_props = self.db.get_valid_properties(
                collection=collection,
                parent_class=parent_class,
                child_class=ClassEnum.Generator,
            )
            invalid = [prop for prop in property_names if prop not in valid_props]
            if invalid:
                msg = (
                    f"Invalid properties for {ClassEnum.Generator.value} "
                    "objects in collection {collection.value}"
                )
                raise PropertyNameError(msg)
        else:
            property_names = []

        generator_class = ClassEnum.Generator.value
        gen_placeholders = ", ".join("?" for _ in generator_names)
        params: list = generator_names[:]
        collection = collection or CollectionEnum.Generators

        # Set collection clause.
        if parent_class is not None:
            coll_id = self.db.get_collection_id(collection, parent_class, ClassEnum.Generator)
            coll_clause = "AND p.collection_id = ?"
        else:
            coll_clause = ""

        query = (
            "SELECT o.name AS generator, p.name AS property, d.value AS value, "
            "REPLACE(GROUP_CONCAT(DISTINCT txt.value), ',', '; ') AS texts, "
            "REPLACE(GROUP_CONCAT(DISTINCT pt.name), ',', '; ') AS tags, "
            "REPLACE(GROUP_CONCAT(DISTINCT band.band_id), ',', '; ') AS bands, "
            "scenario.scenario_name AS scenario, scenario.scenario_category AS scenario_category, "
            "u.value AS unit "
            "FROM t_object o "
            "JOIN t_class c ON o.class_id = c.class_id "
            "JOIN t_membership m ON m.child_object_id = o.object_id "
            "JOIN t_data d ON d.membership_id = m.membership_id "
            "JOIN t_property p ON d.property_id = p.property_id "
            "LEFT JOIN t_unit u ON p.unit_id = u.unit_id "
            "LEFT JOIN t_text txt ON d.data_id = txt.data_id "
            "LEFT JOIN t_tag tag ON d.data_id = tag.data_id "
            "LEFT JOIN t_property_tag pt ON tag.action_id = pt.tag_id "
            "LEFT JOIN t_band band ON d.data_id = band.data_id "
            "LEFT JOIN ("
            " SELECT t.data_id, obj.name AS scenario_name, cat.name AS scenario_category "
            " FROM t_membership mem "
            " JOIN t_tag t ON t.object_id = mem.child_object_id "
            " JOIN t_object obj ON mem.child_object_id = obj.object_id "
            " LEFT JOIN t_category cat ON obj.category_id = cat.category_id "
            " WHERE mem.child_class_id = 78"
            ") AS scenario ON scenario.data_id = d.data_id "
            f"WHERE c.name = '{generator_class}' "
            f"AND o.name IN ({gen_placeholders}) {coll_clause}"
        )

        if coll_clause:
            params.append(coll_id)
        if property_names:
            prop_placeholders = ", ".join("?" for _ in property_names)
            query += f" AND p.name IN ({prop_placeholders})"
            params.extend(property_names)
        if scenario:
            query += " AND scenario.scenario_name = ?"
            params.append(scenario)
        if variable_tag:
            query += " AND p.tag = ?"
            params.append(variable_tag)
        query += (
            " GROUP BY d.data_id, o.name, p.name, d.value, "
            "scenario.scenario_name, scenario.scenario_category, u.value"
        )
        results = self.db.query(query, params)
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

    def get_all_generator_properties(self) -> list[dict]:
        """Retrieve all generator properties without filtering by parent class or collection.

        Returns
        -------
        list[dict]
            Dictionary entries include: generator, property, value, texts,
            tags, bands, scenario, scenario_category, and unit.

        Examples
        --------
        >>> props = api.get_all_generator_properties()
        >>> isinstance(props, list)

        See Also
        --------
        get_generator_properties : Retrieve specific generator properties.
        """
        generator_class = ClassEnum.Generator.value
        query = (
            "SELECT o.name AS generator, p.name AS property, d.value AS value, "
            "REPLACE(GROUP_CONCAT(DISTINCT txt.value), ',', '; ') AS texts, "
            "REPLACE(GROUP_CONCAT(DISTINCT pt.name), ',', '; ') AS tags, "
            "REPLACE(GROUP_CONCAT(DISTINCT band.band_id), ',', '; ') AS bands, "
            "scenario.scenario_name AS scenario, scenario.scenario_category AS scenario_category, "
            "u.value AS unit "
            "FROM t_object o "
            "JOIN t_class c ON o.class_id = c.class_id "
            "JOIN t_membership m ON m.child_object_id = o.object_id "
            "JOIN t_data d ON d.membership_id = m.membership_id "
            "JOIN t_property p ON d.property_id = p.property_id "
            "LEFT JOIN t_unit u ON p.unit_id = u.unit_id "
            "LEFT JOIN t_text txt ON d.data_id = txt.data_id "
            "LEFT JOIN t_tag tag ON d.data_id = tag.data_id "
            "LEFT JOIN t_property_tag pt ON tag.action_id = pt.tag_id "
            "LEFT JOIN t_band band ON d.data_id = band.data_id "
            "LEFT JOIN ("
            " SELECT t.data_id, obj.name AS scenario_name, cat.name AS scenario_category "
            " FROM t_membership mem "
            " JOIN t_tag t ON t.object_id = mem.child_object_id "
            " JOIN t_object obj ON mem.child_object_id = obj.object_id "
            " LEFT JOIN t_category cat ON obj.category_id = cat.category_id "
            " WHERE mem.child_class_id = 78"
            ") AS scenario ON scenario.data_id = d.data_id "
            f"WHERE c.name = '{generator_class}' "
            "GROUP BY "
            "d.data_id, o.name, p.name, d.value, scenario.scenario_name, scenario.scenario_category, u.value"
        )
        results = self.db.query(query)
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

        Raises
        ------
        ValueError
            If object_name or property_name are not provided.
        PropertyNameError
            If the property_name is invalid.

        Examples
        --------
        >>> api.modify_property(
        ...     object_type=ClassEnum.Generator,
        ...     object_name="SolarPV01",
        ...     property_name="Heat Rate",
        ...     new_value="120",
        ...     scenario="NewScenario",
        ... )

        See Also
        --------
        bulk_modify_properties : For updating multiple properties at once.
        """
        if not (object_name and property_name):
            raise ValueError("Both object_name and property_name must be provided.")
        collection = collection or CollectionEnum.Generators
        parent_class = parent_class or ClassEnum.System
        coll_id = self.db.get_collection_id(collection, parent_class, object_type)
        valid_props = self.db.get_valid_properties(
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
        data_rows = self.db.query(select_query, (object_type.value, object_name, property_name, coll_id))
        data_ids = [row[0] for row in data_rows]
        if scenario is not None and band is not None:
            for data_id in data_ids:
                new_data_id = self._duplicate_data_row(data_id)
                self.db._conn.execute(
                    "UPDATE t_data SET value = ? WHERE data_id = ?",
                    (new_value, new_data_id),
                )
                self._update_band_for_data(new_data_id, band, duplicate=False)
                self._update_scenario_for_data(new_data_id, scenario)
        else:
            for data_id in data_ids:
                if scenario is not None:
                    new_data_id = self._duplicate_data_row(data_id)
                    self.db.execute(
                        "UPDATE t_data SET value = ? WHERE data_id = ?",
                        (new_value, new_data_id),
                    )
                    self._update_scenario_for_data(new_data_id, scenario)
                    if band is not None:
                        self._update_band_for_data(new_data_id, band, duplicate=False)
                else:
                    if new_value is not None:
                        update_query = "UPDATE t_data SET value = ? WHERE data_id = ?"
                        self.db._conn.execute(update_query, (new_value, data_id))
                    if band is not None:
                        self._update_band_for_data(data_id, band)

    def _update_scenario_for_data(self, data_id: int, scenario: str) -> None:
        """Update or insert a scenario association for a given data_id.

        Parameters
        ----------
        data_id : int
            The data row ID.
        scenario : str
            The scenario name to associate.

        Examples
        --------
        >>> api._update_scenario_for_data(25, "WinterScenario")

        See Also
        --------
        modify_property : Uses _update_scenario_for_data during property updates.
        """
        scenario_id = self.db.get_scenario_id(scenario)
        self.db.execute_query(
            "INSERT into t_tag(object_id, data_id) VALUES (?,?)",
            (scenario_id, data_id),
        )

    def _update_band_for_data(self, data_id: int, band: str | None, duplicate: bool = True) -> None:
        """Update or insert a band record for the given data_id.

        Parameters
        ----------
        data_id : int
            The data row ID.
        band : str | None
            The band identifier. If None, band_id is set to NULL.
        duplicate : bool, optional
            Whether to duplicate the t_data row if no record exists, by default True.

        Examples
        --------
        >>> api._update_band_for_data(20, "band_A")

        See Also
        --------
        modify_property : May call _update_band_for_data during property updates.
        """
        if band is None:
            upsert_query = (
                "INSERT INTO t_band (data_id, band_id, state) VALUES (?, NULL, 1) "
                "ON CONFLICT(data_id, band_id) DO UPDATE SET state = 1"
            )
            self.db.execute(upsert_query, (data_id,))
        else:
            existing = self.db.query(
                "SELECT band_id FROM t_band WHERE data_id = ? AND band_id = ?",
                (data_id, band),
            )
            if existing:
                upsert_query = (
                    "INSERT INTO t_band (data_id, band_id, state) VALUES (?, ?, 1) "
                    "ON CONFLICT(data_id, band_id) DO UPDATE SET state = 1"
                )
                self.db.execute(upsert_query, (data_id, band))
            else:
                if duplicate:
                    new_data_id = self._duplicate_data_row(data_id)
                    value = self.db.query(
                        "SELECT value FROM t_data WHERE data_id = ?",
                        (data_id,),
                    )[0][0]
                    membership_id, property_id = self.db.query(
                        "SELECT membership_id, property_id FROM t_data WHERE data_id = ?",
                        (data_id,),
                    )[0]
                    self.db.execute(
                        "INSERT INTO t_data (membership_id, property_id, value) VALUES (?, ?, ?)",
                        (membership_id, property_id, value),
                    )
                    self.db.execute(
                        "INSERT INTO t_band (data_id, band_id, state) VALUES (?, ?, 1)",
                        (new_data_id, band),
                    )
                else:
                    value = self.db.query(
                        "SELECT value FROM t_data WHERE data_id = ?",
                        (data_id,),
                    )[0][0]
                    membership_id, property_id = self.db.query(
                        "SELECT membership_id, property_id FROM t_data WHERE data_id = ?",
                        (data_id,),
                    )[0]
                    self.db.execute(
                        "INSERT INTO t_data (membership_id, property_id, value) VALUES (?, ?, ?)",
                        (membership_id, property_id, value),
                    )
                    self.db.execute(
                        "INSERT INTO t_band (data_id, band_id, state) VALUES (?, ?, 1)",
                        (data_id, band),
                    )

    def _duplicate_data_row(self, data_id: int) -> int:
        """Duplicate a row from t_data and return the new data_id.

        Parameters
        ----------
        data_id : int
            The data row ID to duplicate.

        Returns
        -------
        int
            The new data_id from the duplicated row.

        Raises
        ------
        ValueError
            If the data row is not found.

        Examples
        --------
        >>> new_id = api._duplicate_data_row(10)

        See Also
        --------
        bulk_modify_properties : Uses _duplicate_data_row to create new rows.
        """
        row = self.db.query(
            "SELECT membership_id, property_id, value FROM t_data WHERE data_id = ?",
            (data_id,),
        )
        if not row:
            raise ValueError(f"Data row with id {data_id} not found.")
        membership_id, property_id, value = row[0]
        self.db.execute(
            "INSERT INTO t_data (membership_id, property_id, value) VALUES (?, ?, ?)",
            (membership_id, property_id, value),
        )
        return self.db.last_insert_rowid()

    def _get_scenario_for_data(self, data_id: int) -> str | None:
        """
        Retrieve the scenario name associated with the given data_id.

        Parameters
        ----------
        data_id : int
            The data row ID.

        Returns
        -------
        str | None
            The scenario name if associated; otherwise, None.

        Examples
        --------
        >>> scenario = api._get_scenario_for_data(15)

        See Also
        --------
        get_generator_properties : Uses similar join logic for scenario retrieval.
        """
        query = (
            "SELECT obj.name "
            "FROM t_membership mem "
            "JOIN t_tag t ON t.object_id = mem.child_object_id "
            "JOIN t_object obj ON mem.child_object_id = obj.object_id "
            "JOIN t_class cl ON mem.child_class_id = cl.class_id "
            "WHERE cl.name = ? AND t.data_id = ? "
            "LIMIT 1"
        )
        res = self.db.query(
            query,
            (
                ClassEnum.Scenario.value,
                data_id,
            ),
        )
        return res[0][0] if res else None

    def backup_database(self, target_path: str | Path) -> None:
        """
        Backup the in-memory database to a file.

        Parameters
        ----------
        target_path : str | Path
            The file path where the database backup will be saved.

        Examples
        --------
        >>> api.backup_database("backup.db")

        See Also
        --------
        to_xml : Export the database to an XML file.
        """
        self.db.save(target_path)

    def to_xml(self, target_path: str | Path) -> None:
        """Export the current database content to an XML file.

        Parameters
        ----------
        target_path : str | Path
            The file path where the XML export will be saved.

        Examples
        --------
        >>> api.to_xml("export.xml")

        See Also
        --------
        backup_database : Instead of XML export, performs a database file backup.
        """
        self.db.to_xml(target_path)

    def _process_update_for_data_id(
        self, data_id: int, new_value: str | None, scenario: str | None, band: str | None
    ) -> None:
        if scenario is not None:
            new_id = self._duplicate_data_row(data_id)
            self.db._conn.execute(
                "UPDATE t_data SET value = ? WHERE data_id = ?",
                (new_value, new_id),
            )
            self._update_scenario_for_data(new_id, scenario)
            if band is not None:
                self._update_band_for_data(new_id, band, duplicate=False)
        else:
            if new_value is not None:
                self.db._conn.execute(
                    "UPDATE t_data SET value = ? WHERE data_id = ?",
                    (new_value, data_id),
                )
            if band is not None:
                self._update_band_for_data(data_id, band)

    def bulk_modify_properties(self, updates: list[dict]) -> None:
        """Update multiple properties in a single transaction."""
        try:
            self.db._conn.execute("BEGIN")
            for upd in updates:
                object_type = upd["object_type"]
                object_name = upd["object_name"]
                property_name = upd["property_name"]
                new_value = upd["new_value"]
                scenario = upd.get("scenario")
                band = upd.get("band")
                collection = upd.get("collection", CollectionEnum.Generators)
                parent_class = upd.get("parent_class", ClassEnum.System)

                coll_id = self.db.get_collection_id(collection, parent_class, object_type)
                select_query = (
                    "SELECT d.data_id "
                    "FROM t_object o "
                    "JOIN t_class c ON o.class_id = c.class_id "
                    "JOIN t_membership m ON m.child_object_id = o.object_id "
                    "JOIN t_data d ON d.membership_id = m.membership_id "
                    "JOIN t_property p ON d.property_id = p.property_id "
                    "WHERE c.name = ? AND o.name = ? AND p.name = ? AND p.collection_id = ?"
                )
                data_rows = self.db.query(
                    select_query, (object_type.value, object_name, property_name, coll_id)
                )
                data_ids = [row[0] for row in data_rows]
                for data_id in data_ids:
                    self._process_update_for_data_id(data_id, new_value, scenario, band)
            if self.db._conn.in_transaction:
                self.db._conn.commit()
        except Exception as e:
            if self.db._conn.in_transaction:
                self.db._conn.rollback()
            raise e

    def _execute_with_mapping(self, membership_mapping: dict[int, int], query_template: str) -> None:
        """
        Filter the membership_mapping to include only those where
        there are associated t_data rows, then execute the given query_template
        which must include a '{mapping_values}' placeholder to insert the mapping CTE values.
        """
        valid_mapping = {}
        for old, new in membership_mapping.items():
            data_rows = self.db.query("SELECT 1 FROM t_data WHERE membership_id = ? LIMIT 1", (old,))
            if data_rows:
                valid_mapping[old] = new

        if not valid_mapping:
            return

        mapping_values = ", ".join(f"({old}, {new})" for old, new in valid_mapping.items())
        query = query_template.format(mapping_values=mapping_values)
        self.db.execute(query, {})

    def _copy_properties_sql(
        self,
        original_object_name: str,
        membership_mapping: dict[int, int],
    ) -> None:
        """
        Copy all t_data rows corresponding to properties from the original object into new rows
        for the new object. Uses only numeric ids.
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
            JOIN t_data d ON d.membership_id = m.old_membership_id
        """
        self._execute_with_mapping(membership_mapping, query_template)

    def _copy_tags_sql(self, membership_mapping: dict[int, int]) -> None:
        """
        Copy all t_tag rows from the original t_data rows to the new ones.
        For non-scenario tags (non-null action_id) the new membership must differ from the original;
        scenario tags (with NULL action_id) are copied regardless.
        Matching for new t_tag rows is done via the mapping CTE.
        """
        query_template = """
            WITH mapping(old_membership_id, new_membership_id) AS (
                VALUES {mapping_values}
            )
            INSERT INTO t_tag (data_id, object_id, state, action_id)
            SELECT new_d.data_id, t.object_id, t.state, t.action_id
            FROM t_tag t
            JOIN t_data orig ON orig.data_id = t.data_id
            JOIN mapping m ON orig.membership_id = m.old_membership_id
            JOIN t_data new_d ON new_d.membership_id = m.new_membership_id
            WHERE (m.new_membership_id <> orig.membership_id OR t.action_id IS NULL)
        """
        self._execute_with_mapping(membership_mapping, query_template)

    def _copy_texts_sql(self, membership_mapping: dict[int, int]) -> None:
        """
        Copy all t_text rows from the original t_data rows to the new ones.
        Matching for new t_text rows is done via the mapping CTE.
        """
        query_template = """
            WITH mapping(old_membership_id, new_membership_id) AS (
                VALUES {mapping_values}
            )
            INSERT INTO t_text (data_id, class_id, value, state, action_id)
            SELECT new_d.data_id, txt.class_id, txt.value, txt.state, txt.action_id
            FROM t_text txt
            JOIN t_data orig ON orig.data_id = txt.data_id
            JOIN mapping m ON orig.membership_id = m.old_membership_id
            JOIN t_data new_d ON new_d.membership_id = m.new_membership_id
        """
        self._execute_with_mapping(membership_mapping, query_template)

    def copy_object(
        self,
        original_object_name: str,
        new_object_name: str,
        object_type: ClassEnum,
        copy_properties: bool = True,
    ) -> int:
        """Copy object."""
        _ = self.db.get_object_id(original_object_name, object_type)

        new_object_id = self.db.add_object(new_object_name, object_type, CollectionEnum.Generators)
        if new_object_id is None:
            raise ValueError(f"Failed to add new object '{new_object_name}'.")

        # Copy memberships and obtain a mapping.
        membership_mapping = self._copy_object_memberships(original_object_name, new_object_name, object_type)

        if copy_properties:
            # Copy t_data rows.
            self._copy_properties_sql(original_object_name, membership_mapping)
            # Copy associated t_tag rows.
            self._copy_tags_sql(membership_mapping)
            # Copy associated t_text rows.
            self._copy_texts_sql(membership_mapping)
        return new_object_id

    def _copy_object_memberships(
        self, original_object_name: str, object_name: str, object_type: ClassEnum
    ) -> dict[int, int]:
        membership_mapping: dict[int, int] = {}
        # Get all memberships for the original object.
        all_memberships = self.db.get_memberships(
            original_object_name, object_class=object_type, include_system_membership=True
        )

        # Use case-insensitive comparison for object names.
        orig_lower = original_object_name.lower()

        # Loop for copying memberships when the original object is the child.
        for mem in all_memberships:
            parent_name = mem[2]
            child_name = mem[3]
            parent_class_name = mem[5]
            child_class_name = mem[6]
            collection_name = mem[7]
            if child_name.lower() == orig_lower:
                new_mem_id = self.db.add_membership(
                    parent_name,
                    object_name,
                    parent_class=ClassEnum[parent_class_name],
                    child_class=ClassEnum[child_class_name],
                    collection=CollectionEnum[collection_name],
                )
                old_mem_id = self.db.get_membership_id(
                    child_name=original_object_name,
                    parent_name=parent_name,
                    child_class=ClassEnum[child_class_name],
                    parent_class=ClassEnum[parent_class_name],
                    collection=CollectionEnum[collection_name],
                )
                membership_mapping[old_mem_id] = new_mem_id

        # Loop for copying memberships when the original object is the parent.
        for mem in all_memberships:
            parent_name = mem[2]
            child_name = mem[3]
            parent_class_name = mem[5]
            child_class_name = mem[6]
            collection_name = mem[7]
            if parent_name.lower() == orig_lower:
                new_mem_id = self.db.add_membership(
                    object_name,
                    child_name,
                    parent_class=ClassEnum[parent_class_name],
                    child_class=ClassEnum[child_class_name],
                    collection=CollectionEnum[collection_name],
                )
                old_mem_id = self.db.get_membership_id(
                    child_name=child_name,
                    parent_name=original_object_name,
                    child_class=ClassEnum[child_class_name],
                    parent_class=ClassEnum[parent_class_name],
                    collection=CollectionEnum[collection_name],
                )
                membership_mapping[old_mem_id] = new_mem_id
        return membership_mapping
