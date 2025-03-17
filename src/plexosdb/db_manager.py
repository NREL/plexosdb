"""SQLite database manager."""

import sqlite3
from collections import namedtuple
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, NamedTuple

from loguru import logger

from plexosdb.utils import no_space


def create_namedtuple_factory(cursor: sqlite3.Cursor, row: tuple) -> NamedTuple:
    """Convert a database row into a named tuple.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        The SQLite cursor object
    row : tuple
        The database row as a tuple

    Returns
    -------
    NamedTuple
        A named tuple representing the row
    """
    fields = [col[0] for col in cursor.description]
    Row = namedtuple("Row", fields)  # type: ignore
    return Row(*row)


def create_sqlite_connection(database: str | None = None, in_memory: bool = False) -> sqlite3.Connection:
    """Create a SQLite database connection.

    Parameters
    ----------
    database : str, optional
        Path to the SQLite database file or None for in-memory database
    in_memory : bool, default=False
        Whether to create an in-memory database regardless of database path

    Returns
    -------
    sqlite3.Connection
        A new SQLite connection
    """
    if in_memory or database is None:
        return sqlite3.connect(":memory:")
    return sqlite3.connect(database, isolation_level=None)  # Autocommit mode


class SQLiteManager:
    """SQLite database manager with optimized transaction support."""

    _conn: sqlite3.Connection | None = None

    def __init__(
        self,
        db_path: str | Path | None = None,
        conn: sqlite3.Connection | None = None,
        use_named_tuples: bool = True,
        initialize: bool = True,
        create_collations: bool = True,
        in_memory: bool = True,
    ) -> None:
        """Initialize the database manager.

        Parameters
        ----------
        db_path : str or Path, optional
            Path to the SQLite database file or None for in-memory database
        db_schema_contents : Path or str, optional
            Path to SQL schema file to initialize the database structure
        conn : sqlite3.Connection, optional
            Existing SQLite connection to use instead of creating a new one
        use_named_tuples : bool
            Whether to return named tuples for query results. Default = False
        initialize: bool
            Whether to initialize a new schema for the database. Default = True
        create_collations: bool
            Whether to add default python collation functions to the database. Default = True
        in_memory: bool
            Force in-memory database regardless of db_path. Default True

        See Also
        --------
        no_space Collation function for searching withouth spaces.
        """
        self._in_memory = in_memory or db_path is None

        if not conn:
            path_str = str(db_path) if db_path is not None else None
            conn = create_sqlite_connection(path_str, self._in_memory)
        self._conn = conn

        if initialize:
            self._set_sqlite_configuration()

            # Register collations first (might be needed by the schema)
            if create_collations:
                self._create_collations("NOSPACE", no_space)

        # Set row factory to namedtuple if requested for easier access to data.
        if use_named_tuples:
            self._conn.row_factory = create_namedtuple_factory

    @property
    def conn(self) -> sqlite3.Connection:
        """SQLite connection."""
        assert self._conn is not None, "Database connection is not initialized"
        return self._conn

    def _set_sqlite_configuration(self) -> None:
        """Configure SQLite for optimal performance."""
        # Use our class flag to determine the configuration
        if self._in_memory:
            # In-memory optimizations
            self.execute("PRAGMA synchronous = NORMAL;")
            self.execute("PRAGMA journal_mode = WAL;")
            self.execute("PRAGMA temp_store = MEMORY")
            self.execute("PRAGMA mmap_size = 30000000000")  # 30GB
            self.execute("PRAGMA cache_size = -20000")  # ~20MB
        else:
            # File-based optimizations for durability
            self.execute("PRAGMA synchronous = FULL;")
            self.execute("PRAGMA journal_mode = DELETE;")  # More compatible
            self.execute("PRAGMA temp_store = MEMORY")
            self.execute("PRAGMA cache_size = -2000")  # ~2MB (smaller for disk)

        self.execute("PRAGMA foreign_keys = ON")

    def _create_collations(self, name: str, callable_func: Callable[[str, str], int]) -> bool:
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
            self.conn.create_collation(name, callable_func)
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
            self.conn.commit()

            # Perform backup with proper connection handling
            # The key is using a separate connection with context manager
            with sqlite3.connect(str(target_path)) as dest_conn:
                # Use smaller pages for better reliability
                self.conn.backup(dest_conn)

            return True
        except (sqlite3.Error, OSError) as e:
            logger.error(f"Database backup failed: {e}")
            return False

    def close(self) -> None:
        """Close the database connection and release resources."""
        # Skip if already closed
        if self._conn is None:
            return

        try:
            # First try to release any locks and pending transactions
            if self._conn.in_transaction:
                try:
                    self._conn.rollback()
                except sqlite3.Error:
                    logger.debug("Rollback failed during close")

            # For file databases, try to flush data
            try:
                if not self._in_memory:
                    self._conn.commit()
                self._conn.close()
            except sqlite3.Error as e:
                logger.warning(f"Error closing database connection: {e}")
        finally:
            # Always null the connection reference
            self._conn = None

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
        in_transaction = self.conn.in_transaction
        try:
            logger.trace(query)
            if params:
                self.conn.execute(query, params)
            else:
                self.conn.execute(query)

            if not in_transaction:
                self.conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"SQL execute error: {e}")
            if in_transaction:
                raise
            try:
                self.conn.rollback()
            except sqlite3.Error as rb_error:
                logger.error(f"Rollback error: {rb_error}")
            return False

    def executemany(self, query: str, params_seq: list[tuple[Any, ...] | dict[str, Any]]) -> bool:
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
        in_transaction = self.conn.in_transaction
        try:
            logger.trace(query)
            self.conn.executemany(query, params_seq)
            if not in_transaction:
                self.conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f"SQL execute error: {e}")
            if in_transaction:
                raise
            try:
                self.conn.rollback()
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
        in_transaction = self.conn.in_transaction
        try:
            if not in_transaction:
                self.conn.execute("BEGIN IMMEDIATE TRANSACTION")

            cursor = self.conn.cursor()

            for statement in statements:
                if statement:  # Skip empty statements
                    cursor.execute(statement)

            if not in_transaction:
                self.conn.commit()

            return True
        except sqlite3.Error as error:
            logger.error("SQL script execution failed: {}", error)
            if not in_transaction and self.conn:
                self.conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

    def iter_query(
        self,
        query_string: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
        batch_size: int = 1000,
    ) -> Iterator:
        """Execute a read-only query and return an iterator of results.

        This is memory-efficient for large result sets. Use only for SELECT statements.

        Parameters
        ----------
        query_string : str
            SQL query to execute (SELECT statements only)
        params : tuple or dict, optional
            Parameters to bind to the query
        batch_size : int, default=1000
            Number of records to fetch in each batch

        Yields
        ------
        tuple or NamedTuple
            One database row at a time

        Raises
        ------
        sqlite3.Error
            If a database error occurs
        """
        cursor = self.conn.cursor()

        try:
            # Execute query
            if params:
                cursor.execute(query_string, params)
            else:
                cursor.execute(query_string)

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
            if self.conn.in_transaction:
                logger.warning("Committing transaction before VACUUM - optimization may not be atomic")
                self.conn.commit()

            # Execute VACUUM directly on the connection
            self.conn.execute("VACUUM")

            return True
        except sqlite3.Error as error:
            logger.error("Database optimization failed: {}", error)
            return False

    def query(self, query_string: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> list:
        """Execute a read-only query and return all results.

        Note: This method should ONLY be used for SELECT statements.
        For INSERT/UPDATE/DELETE, use execute() instead.

        Parameters
        ----------
        query_string : str
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
        cursor = self.conn.cursor()
        try:
            if params:
                cursor.execute(query_string, params)
            else:
                cursor.execute(query_string)

            return cursor.fetchall()
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
            self.conn.execute("BEGIN")
            yield self
        except sqlite3.Error:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

    def __enter__(self):
        """Support using SQLiteManager as a context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Automatically close connection when exiting context."""
        self.close()
        return False  # Don't suppress exceptions from the with block
