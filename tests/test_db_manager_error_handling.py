"""Test error handling in db_manager methods.

This module tests the error handling paths in SQLiteManager for various database
operations, ensuring proper rollback and error logging behavior.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    pass


def test_execute_error_handling_with_rollback_error():
    """Test execute() handles both sqlite3.Error and rollback errors."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_conn.execute.side_effect = sqlite3.Error("Execute failed")
    mock_conn.rollback.side_effect = sqlite3.Error("Rollback failed")

    db._con = mock_conn

    result = db.execute("INSERT INTO test VALUES (?)", (1,))

    assert result is False


def test_execute_error_handling_in_transaction():
    """Test execute() re-raises error when in active transaction."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = True
    mock_conn.execute.side_effect = sqlite3.Error("Execute failed")

    db._con = mock_conn

    with pytest.raises(sqlite3.Error):
        db.execute("INSERT INTO test VALUES (?)", (1,))


def test_executemany_error_handling_with_rollback_error():
    """Test executemany() handles both sqlite3.Error and rollback errors."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_conn.executemany.side_effect = sqlite3.Error("ExecuteMany failed")
    mock_conn.rollback.side_effect = sqlite3.Error("Rollback failed")

    db._con = mock_conn

    result = db.executemany("INSERT INTO test VALUES (?)", [(1,), (2,)])

    assert result is False


def test_executemany_error_handling_in_transaction():
    """Test executemany() re-raises error when in active transaction."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = True
    mock_conn.executemany.side_effect = sqlite3.Error("ExecuteMany failed")

    db._con = mock_conn

    with pytest.raises(sqlite3.Error):
        db.executemany("INSERT INTO test VALUES (?)", [(1,), (2,)])


def test_executescript_error_handling():
    """Test executescript() handles sqlite3.Error during execution."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = sqlite3.Error("Script execution failed")
    mock_conn.cursor.return_value = mock_cursor

    db._con = mock_conn

    result = db.executescript("SELECT 1; SELECT 2;")

    assert result is False


def test_executescript_cursor_cleanup_on_error():
    """Test executescript() properly cleans up cursor on error."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = sqlite3.Error("Execution failed")
    mock_conn.cursor.return_value = mock_cursor

    db._con = mock_conn

    db.executescript("SELECT 1;")

    mock_cursor.close.assert_called_once()


def test_fetchmany_error_handling():
    """Test fetchmany() re-raises sqlite3.Error."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = sqlite3.Error("Query failed")
    mock_conn.cursor.return_value = mock_cursor

    db._con = mock_conn

    with pytest.raises(sqlite3.Error):
        db.fetchmany("SELECT * FROM test")


def test_fetchmany_cursor_cleanup_on_error():
    """Test fetchmany() properly cleans up cursor even on error."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = sqlite3.Error("Query failed")
    mock_conn.cursor.return_value = mock_cursor

    db._con = mock_conn

    with pytest.raises(sqlite3.Error):
        db.fetchmany("SELECT * FROM test")

    mock_cursor.close.assert_called_once()


def test_execute_successful_with_commit():
    """Test execute() successfully commits when not in transaction."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_conn.execute.return_value = MagicMock()

    db._con = mock_conn

    result = db.execute("INSERT INTO test VALUES (?)", (1,))

    assert result is True


def test_executemany_successful_with_commit():
    """Test executemany() successfully commits when not in transaction."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_conn.executemany.return_value = MagicMock()

    db._con = mock_conn

    result = db.executemany("INSERT INTO test VALUES (?)", [(1,), (2,)])

    assert result is True


def test_executescript_successful_with_commit():
    """Test executescript() successfully commits when not in transaction."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    db._con = mock_conn

    result = db.executescript("SELECT 1; SELECT 2;")

    assert result is True
