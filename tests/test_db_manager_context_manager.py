"""Tests for SQLiteManager context manager protocol.

Tests for __enter__ and __exit__ methods that implement the context manager
protocol, allowing SQLiteManager to be used with the 'with' statement.
"""

from __future__ import annotations


def test_context_manager_enter_returns_self() -> None:
    """Test that __enter__ returns the SQLiteManager instance."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()

    result = db.__enter__()

    assert result is db
    db.__exit__(None, None, None)


def test_context_manager_exit_closes_connection() -> None:
    """Test that __exit__ closes the database connection."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()

    assert db._con is not None

    db.__exit__(None, None, None)

    assert db._con is None


def test_context_manager_with_statement() -> None:
    """Test using SQLiteManager as context manager with 'with' statement."""
    from plexosdb.db_manager import SQLiteManager

    with SQLiteManager() as db:
        # Inside the context, connection should be available
        assert db._con is not None

        # Should be able to use database operations
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO test (id) VALUES (1)")
        result = db.query("SELECT * FROM test")
        assert len(result) == 1

    # After exiting context, connection should be closed
    assert db._con is None


def test_context_manager_with_statement_operations() -> None:
    """Test various database operations within context manager."""
    from plexosdb.db_manager import SQLiteManager

    with SQLiteManager() as db:
        # Create table
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")

        # Insert multiple rows
        db.executemany(
            "INSERT INTO items (name) VALUES (?)",
            [("Item 1",), ("Item 2",), ("Item 3",)],
        )

        # Query and verify
        rows = db.query("SELECT * FROM items")
        assert len(rows) == 3

        # Use iterator
        count = 0
        for row in db.iter_query("SELECT * FROM items"):
            count += 1
        assert count == 3


def test_context_manager_exception_still_closes() -> None:
    """Test that connection is closed even when exception occurs in context."""
    from plexosdb.db_manager import SQLiteManager

    try:
        with SQLiteManager() as db:
            assert db._con is not None
            raise ValueError("Test exception")
    except ValueError:
        pass

    # After exception, connection should still be closed
    assert db._con is None


def test_context_manager_sqlite_error_in_context() -> None:
    """Test that connection is closed even when sqlite3 error occurs."""
    from plexosdb.db_manager import SQLiteManager

    try:
        with SQLiteManager() as db:
            assert db._con is not None

            # Cause a database error
            db.execute("INVALID SQL SYNTAX !!!!")
    except Exception:
        pass

    # Connection should still be closed
    assert db._con is None
