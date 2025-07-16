"""SQLite database manager."""

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar, overload

from loguru import logger

T = TypeVar("T")


@dataclass(slots=True)
class SQLiteConfig:
    """SQLite database configuration."""

    cache_size_mb: int = 20
    mmap_size_gb: int = 30
    synchronous: str = "NORMAL"
    journal_mode: str = "WAL"
    foreign_keys: bool = True
    temp_store: str = "MEMORY"

    @classmethod
    def for_in_memory(cls) -> "SQLiteConfig":
        """Create optimized config for in-memory databases."""
        return cls(
            cache_size_mb=50,
            mmap_size_gb=0,
            synchronous="OFF",
            journal_mode="MEMORY",
            foreign_keys=True,
            temp_store="MEMORY",
        )

    @classmethod
    def for_file_database(cls) -> "SQLiteConfig":
        """Create optimized config for file-based databases."""
        return cls(
            cache_size_mb=20,
            mmap_size_gb=2,
            synchronous="NORMAL",
            journal_mode="WAL",
            foreign_keys=True,
            temp_store="MEMORY",
        )


class SQLiteManager(Generic[T]):
    """SQLite database manager with optimized transaction support."""

    _con: sqlite3.Connection | None = None

    def __init__(
        self,
        fpath_or_conn: str | Path | sqlite3.Connection | None = None,
        *,
        config: SQLiteConfig | None = None,
        initialize: bool = True,
    ) -> None:
        match fpath_or_conn:
            case None:
                logger.info("Creating in-memory database.")
                self._con = sqlite3.connect(":memory:")
                self._config = config or SQLiteConfig.for_in_memory()
            case str() | Path():
                file_path = Path(fpath_or_conn)
                if not file_path.exists():
                    logger.info("Database {} does not exist. Creating it.", file_path)
                self._con = sqlite3.connect(str(file_path), isolation_level=None)
                self._config = config or SQLiteConfig.for_file_database()
            case sqlite3.Connection():
                logger.info("Using existing connection for the database.")
                self._con = fpath_or_conn
                self._config = config or SQLiteConfig.for_file_database()
            case _:
                raise TypeError
        if initialize:
            self._set_sqlite_configuration(config=self._config)

    @property
    def connection(self) -> sqlite3.Connection:
        """SQLite connection."""
        assert self._con is not None, "Database connection is not initialized"
        return self._con

    @property
    def config(self) -> SQLiteConfig:
        """SQLite configuration."""
        return self._config

    @property
    def sqlite_version(self) -> int:
        """SQLite version."""
        return self.query("select sqlite_version()")[0][0]

    @property
    def tables(self) -> list[str]:
        """List of table names."""
        return self.list_table_names()

    def _set_sqlite_configuration(self, config: SQLiteConfig) -> None:
        """Apply configuration based on a configuration object."""
        foreign_keys_setting = "ON" if config.foreign_keys else "OFF"
        self.execute(f"PRAGMA foreign_keys = {foreign_keys_setting}")

        self.execute(f"PRAGMA synchronous = {config.synchronous}")
        self.execute(f"PRAGMA journal_mode = {config.journal_mode}")
        self.execute(f"PRAGMA temp_store = {config.temp_store}")

        cache_size_kb = -abs(self._config.cache_size_mb * 1024)
        self.execute(f"PRAGMA cache_size = {cache_size_kb}")

        mmap_size_bytes = config.mmap_size_gb * 1024 * 1024 * 1024
        self.execute(f"PRAGMA mmap_size = {mmap_size_bytes}")

        if self._is_in_memory():
            self.execute("PRAGMA locking_mode = EXCLUSIVE")
        else:
            self.execute("PRAGMA auto_vacuum = INCREMENTAL")
        return

    def _is_in_memory(self) -> bool:
        """Check if the connection is to an in-memory database."""
        result = self.query("PRAGMA database_list")
        return result[0][2] == ""  # Empty file path indicates in-memory

    def add_collation(self, name: str, callable_func: Callable[[str, str], int]) -> bool:
        """Register a collation function.

        Parameters
        ----------
        name : str
            Name of the collation
        callable_func : callable
            Function implementing the collation

        Returns
        -------
        bool
            True if creation succeeded, False if it failed
        """
        try:
            self.connection.create_collation(name, callable_func)
            return True
        except sqlite3.Error:
            return False

    def backup(self, target_path: str | Path) -> bool:
        """Backup the database to a file.

        Parameters
        ----------
        target_path : str or Path
            Path to save the database backup

        Returns
        -------
        bool
            True if backup succeeded, False if it failed
        """
        try:
            target_path = Path(target_path) if isinstance(target_path, str) else target_path

            # Ensure parent directories exist
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Commit any pending changes before backup
            self.connection.commit()

            # Perform backup with proper connection handling
            # The key is using a separate connection with context manager
            with sqlite3.connect(str(target_path)) as dest_conn:
                # Use smaller pages for better reliability
                self.connection.backup(dest_conn)

            return True
        except (sqlite3.Error, OSError) as e:
            logger.error(f"Database backup failed: {e}")
            return False

    def close(self) -> None:
        """Close the database connection and release resources."""
        # Skip if already closed
        if self._con is None:
            return

        try:
            # First try to release any locks and pending transactions
            if self._con.in_transaction:
                try:
                    self._con.rollback()
                except sqlite3.Error:
                    logger.debug("Rollback failed during close")

            # For file databases, try to flush data
            try:
                if not self._is_in_memory():
                    self._con.commit()
                self._con.close()
            except sqlite3.Error as e:
                logger.warning(f"Error closing database connection: {e}")
        finally:
            # Always null the connection reference
            self._con = None

    def execute(self, query: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> bool:
        """Execute a SQL statement that doesn't return results.

        Each execution is its own transaction unless used within a transaction context.

        Parameters
        ----------
        query : str
            SQL statement to execute
        params : tuple or dict, optional
            Parameters to bind to the statement

        Returns
        -------
        bool
            True if execution succeeded, False if it failed

        Raises
        ------
        sqlite3.Error
            If a database error occurs within a transaction
        """
        in_transaction = self.connection.in_transaction
        try:
            logger.trace(query)
            if params:
                self.connection.execute(query, params)
            else:
                self.connection.execute(query)

            if not in_transaction:
                self.connection.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"SQL execute error: {e}")
            if in_transaction:
                raise
            try:
                self.connection.rollback()
            except sqlite3.Error as rb_error:
                logger.error(f"Rollback error: {rb_error}")
            return False

    def executemany(self, query: str, params_seq: list[tuple[Any, ...]] | list[dict[str, Any]]) -> bool:
        """Execute a SQL statement with multiple parameter sets.

        Parameters
        ----------
        query : str
            SQL statement to execute
        params_seq : list of tuples or dicts
            Sequence of parameter sets to bind

        Returns
        -------
        bool
            True if execution succeeded, False if it failed

        Raises
        ------
        sqlite3.Error
            If a database error occurs within a transaction
        """
        in_transaction = self.connection.in_transaction
        try:
            logger.trace(query)
            self.connection.executemany(query, params_seq)
            if not in_transaction:
                self.connection.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f"SQL execute error: {e}")
            if in_transaction:
                raise
            try:
                self.connection.rollback()
            except sqlite3.Error as rb_error:
                logger.error(f"Rollback error: {rb_error}")
            return False

    def executescript(self, script: str) -> bool:
        """Execute a SQL script containing multiple statements.

        Parameters
        ----------
        script : str
            SQL script to execute

        Returns
        -------
        bool
            True if execution succeeded, False if it failed
        """
        statements = [stmt.strip() for stmt in script.split(";") if stmt.strip()]
        cursor = None
        in_transaction = self.connection.in_transaction
        try:
            if not in_transaction:
                self.connection.execute("BEGIN IMMEDIATE TRANSACTION")

            cursor = self.connection.cursor()

            for statement in statements:
                if statement:  # Skip empty statements
                    cursor.execute(statement)

            if not in_transaction:
                self.connection.commit()

            return True
        except sqlite3.Error as error:
            logger.error("SQL script execution failed: {}", error)
            if not in_transaction and self.connection:
                self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

    def iter_query(
        self,
        query: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        batch_size: int = 1000,
    ) -> Iterator[tuple[Any, ...]]:
        """Execute a read-only query and return an iterator of results.

        This is memory-efficient for large result sets. Use only for SELECT statements.

        Parameters
        ----------
        query : str
            SQL query to execute (SELECT statements only)
        params : tuple or dict, optional
            Parameters to bind to the query
        batch_size : int, default=1000
            Number of records to fetch in each batch

        Yields
        ------
        tuple
            One database row at a time

        Raises
        ------
        sqlite3.Error
            If a database error occurs
        """
        cursor = self.connection.cursor()

        try:
            # Execute query
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Fetch and yield rows in batches
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield from rows

        except sqlite3.Error:
            # Let the caller handle database errors
            raise

        finally:
            # Always close cursor
            cursor.close()

    def last_insert_rowid(self) -> int:
        """Get the ID of the last inserted row.

        Returns
        -------
        int
            ID of the last inserted row

        Raises
        ------
        sqlite3.Error
            If a database error occurs
        """
        try:
            return self.query("SELECT last_insert_rowid()")[0][0]
        except IndexError:
            # This shouldn't happen with last_insert_rowid() but handle it anyway
            return 0

    def list_table_names(self) -> list[str]:
        """Return a list of current table names on the database."""
        sql = "SELECT name FROM sqlite_master WHERE type ='table'"
        return [r[0] for r in self.fetchall(sql)]

    def optimize(self) -> bool:
        """Run optimization routines on the database.

        VACUUM can't run inside a transaction, so this method may commit
        any pending changes before optimizing the database.

        Returns
        -------
        bool
            True if optimization succeeded, False if it failed
        """
        try:
            # These can run inside a transaction
            self.execute("PRAGMA optimize")
            self.execute("ANALYZE")

            # VACUUM requires special handling - can't be in transaction
            if self.connection.in_transaction:
                logger.warning("Committing transaction before VACUUM - optimization may not be atomic")
                self.connection.commit()

            # Execute VACUUM directly on the connection
            self.connection.execute("VACUUM")

            return True
        except sqlite3.Error as error:
            logger.error("Database optimization failed: {}", error)
            return False

    def _validate_query_type(self, query: str) -> None:
        """Validate that query is read-only for query methods."""
        query_upper = query.strip().upper()
        write_keywords = {"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER"}
        first_word = query_upper.split()[0] if query_upper.split() else ""

        if first_word in write_keywords:
            raise ValueError(f"Use execute() for {first_word} statements, not query()")

    # Add generic type support for query results
    @overload
    def query(self, query: str, params: None = None) -> list[tuple[Any, ...]]: ...

    @overload
    def query(self, query: str, params: tuple[Any, ...] | dict[str, Any]) -> list[tuple[Any, ...]]: ...

    def query(
        self, query: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Execute a read-only query and return all results.

        Note: This method should ONLY be used for SELECT statements.
        For INSERT/UPDATE/DELETE, use execute() instead.

        Parameters
        ----------
        query : str
            SQL query to execute (SELECT statements only)
        params : tuple or dict, optional
            Parameters to bind to the query

        Returns
        -------
        list
            Query results (tuples or named tuples based on initialization)

        Raises
        ------
        sqlite3.Error
            If a database error occurs
        """
        self._validate_query_type(query)
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or tuple())
            return cursor.fetchall()
        except sqlite3.Error:
            # Let the caller handle database errors
            raise
        finally:
            cursor.close()

    def fetchall(
        self, query: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return all results as a list of rows.

        This method is a standard DB-API style alias for query().

        Parameters
        ----------
        query : str
            SQL query to execute (SELECT statements only)
        params : tuple or dict, optional
            Parameters to bind to the query

        Returns
        -------
        list
            All rows (as tuples or named tuples based on row_factory setting)

        Raises
        ------
        sqlite3.Error
            If a database error occurs

        See Also
        --------
        query : Equivalent method with PlexosDB-specific naming
        fetchone : Get only the first row of results
        fetchall_dict : Return results as dictionaries

        Examples
        --------
        >>> db = SQLiteManager()
        >>> db.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        >>> db.execute("INSERT INTO test VALUES (1, 'Alice')")
        >>> db.fetchall("SELECT * FROM test")
        [(1, 'Alice')]
        """
        return self.query(query, params)

    def fetchall_dict(
        self, query: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a query and return all results as a list of dictionaries.

        Parameters
        ----------
        query : str
            SQL query to execute (SELECT statements only)
        params : tuple or dict, optional
            Parameters to bind to the query

        Returns
        -------
        list[dict[str, Any]]
            All rows as dictionaries with column names as keys

        Raises
        ------
        sqlite3.Error
            If a database error occurs

        See Also
        --------
        query : Return results as tuples
        iter_dicts : Memory-efficient iterator over dictionaries

        Examples
        --------
        >>> db = SQLiteManager()
        >>> db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        >>> db.execute("INSERT INTO users VALUES (1, 'Alice')")
        >>> db.fetchall_dict("SELECT * FROM users")
        [{'id': 1, 'name': 'Alice'}]
        >>> user = db.fetchall_dict("SELECT * FROM users")[0]
        >>> print(f"User {user['id']}: {user['name']}")
        User 1: Alice
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or tuple())
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except sqlite3.Error:
            # Let the caller handle database errors
            raise
        finally:
            cursor.close()

    def fetchmany(
        self, query: str, size: int = 1000, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return a specified number of rows.

        Parameters
        ----------
        query : str
            SQL query to execute (SELECT statements only)
        size : int, default=1000
            Maximum number of rows to return
        params : tuple or dict, optional
            Parameters to bind to the query

        Returns
        -------
        list
            Up to 'size' rows from the query result

        Raises
        ------
        sqlite3.Error
            If a database error occurs

        See Also
        --------
        fetchall : Get all rows
        fetchone : Get only one row
        iter_query : Iterator over results

        Examples
        --------
        >>> db = SQLiteManager()
        >>> db.execute("CREATE TABLE items (id INTEGER, name TEXT)")
        >>> db.executemany("INSERT INTO items VALUES (?, ?)", [(i, f"Item {i}") for i in range(1, 101)])
        >>> # Get first 10 items
        >>> first_batch = db.fetchmany("SELECT * FROM items", size=10)
        >>> len(first_batch)
        10
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or tuple())
            return cursor.fetchmany(size)
        except sqlite3.Error:
            # Let the caller handle database errors
            raise
        finally:
            cursor.close()

    def fetchone(self, query: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> Any | None:
        """Execute a query and return only the first result row.

        Parameters
        ----------
        query : str
            SQL query to execute (SELECT statements only)
        params : tuple or dict, optional
            Parameters to bind to the query

        Returns
        -------
        tuple or namedtuple or None
            First row of results or None if no results

        Raises
        ------
        sqlite3.Error
            If a database error occurs

        See Also
        --------
        fetchall : Get all rows
        fetchone_dict : Get first row as dictionary

        Examples
        --------
        >>> db = SQLiteManager()
        >>> db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        >>> db.execute("INSERT INTO users VALUES (1, 'Alice')")
        >>> user = db.fetchone("SELECT * FROM users WHERE id = ?", (1,))
        >>> user
        (1, 'Alice')
        >>> user = db.fetchone("SELECT * FROM users WHERE id = ?", (999,))
        >>> user is None
        True
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or tuple())
            return cursor.fetchone()
        except sqlite3.Error:
            # Let the caller handle database errors
            raise
        finally:
            cursor.close()

    def fetchone_dict(
        self, query: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Execute a query and return only the first result row as a dictionary.

        Parameters
        ----------
        query : str
            SQL query to execute (SELECT statements only)
        params : tuple or dict, optional
            Parameters to bind to the query

        Returns
        -------
        dict[str, Any] or None
            First row as dictionary with column names as keys, or None if no results

        Raises
        ------
        sqlite3.Error
            If a database error occurs

        See Also
        --------
        fetchone : Get first row as tuple
        fetchall_dict : Get all rows as dictionaries

        Examples
        --------
        >>> db = SQLiteManager()
        >>> db.execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)")
        >>> db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
        >>> user = db.fetchone_dict("SELECT * FROM users WHERE id = ?", (1,))
        >>> print(f"{user['name']}'s email is {user['email']}")
        Alice's email is alice@example.com
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or tuple())
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        except sqlite3.Error:
            # Let the caller handle database errors
            raise
        finally:
            cursor.close()

    def iter_dicts(
        self,
        query: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        batch_size: int = 1000,
    ) -> Iterator[dict[str, Any]]:
        """Execute a read-only query and yield results as dictionaries.

        This is memory-efficient for large result sets. Each row is returned
        as a dictionary with column names as keys.

        Parameters
        ----------
        query : str
            SQL query to execute (SELECT statements only)
        params : tuple or dict, optional
            Parameters to bind to the query
        batch_size : int, default=1000
            Number of records to fetch in each batch

        Yields
        ------
        dict[str, Any]
            One database row at a time as a dictionary

        Raises
        ------
        sqlite3.Error
            If a database error occurs

        See Also
        --------
        iter_query : Iterator over tuples or named tuples
        fetchall_dict : Get all results as dictionaries at once

        Examples
        --------
        >>> db = SQLiteManager()
        >>> db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        >>> db.executemany("INSERT INTO users VALUES (?, ?)", [(i, f"User {i}") for i in range(1, 1001)])
        >>> # Process users efficiently one at a time
        >>> for user in db.iter_dicts("SELECT * FROM users"):
        ...     print(f"Processing user {user['name']}")
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or tuple())
            columns = [desc[0] for desc in cursor.description]

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield dict(zip(columns, row))
        except sqlite3.Error:
            # Let the caller handle database errors
            raise
        finally:
            cursor.close()

    @contextmanager
    def transaction(self):
        """Begin a transaction that can span multiple operations.

        This provides explicit transaction control for grouping multiple operations
        into a single atomic unit that either all succeed or all fail.

        Returns
        -------
        context manager
            A context manager for the transaction

        Examples
        --------
        >>> with db.transaction():
        >>>     db.execute("INSERT INTO table1 VALUES (1, 'a')")
        >>>     db.execute("INSERT INTO table2 VALUES (1, 'b')")

        Raises
        ------
        sqlite3.Error
            If a database error occurs during transaction
        """
        try:
            self.connection.execute("BEGIN")
            yield self
        except sqlite3.Error:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def insert_records(
        self,
        table_name: str,
        records: dict[str, Any] | list[dict[str, Any]],
    ) -> bool:
        """Insert records into a table using dictionaries with column names as keys.

        Fails if duplicate records exist (uses INSERT without OR REPLACE).

        Parameters
        ----------
        table_name : str
            Name of the table to insert records into
        records : dict or list of dicts
            Dictionary or list of dictionaries with column names as keys

        Returns
        -------
        bool
            True if insertion succeeded, False if it failed

        Raises
        ------
        ValueError
            If table doesn't exist, records are empty, or have inconsistent keys
        sqlite3.Error
            If a database error occurs within a transaction or duplicate records exist
        """
        if not records:
            msg = "Records cannot be empty"
            raise ValueError(msg)

        if table_name not in self.tables:
            msg = f"Table '{table_name}' does not exist"
            raise ValueError(msg)

        records_list = [records] if isinstance(records, dict) else records

        if not records_list or not records_list[0]:
            msg = "Records cannot be empty"
            raise ValueError(msg)

        first_keys = set(records_list[0].keys())
        if not all(set(record.keys()) == first_keys for record in records_list):
            msg = "All records must have the same keys to be inserted."
            raise KeyError(msg)

        columns = list(first_keys)
        placeholders = ", ".join("?" * len(columns))
        columns_str = ", ".join(columns)
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        values_list = [tuple(record[col] for col in columns) for record in records_list]

        return self.executemany(query, values_list)

    def __enter__(self):
        """Support using SQLiteManager as a context manager."""
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> None:
        """Automatically close connection when exiting context."""
        self.close()
