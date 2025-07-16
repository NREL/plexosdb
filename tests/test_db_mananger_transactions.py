import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from plexosdb.db_manager import SQLiteManager


def test_sqlite_manager_transaction(db_manager_instance_empty):
    """Test transaction functionality."""
    with db_manager_instance_empty.transaction():
        db_manager_instance_empty.execute(
            "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", ("Gen 1", 100.0, "Solar")
        )
        db_manager_instance_empty.execute(
            "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", ("Gen 2", 200.0, "Wind")
        )

    # Verify both inserts were committed
    result = db_manager_instance_empty.query("SELECT COUNT(*) FROM generators")
    assert result[0][0] == 2

    # Test transaction rollback on error
    try:
        with db_manager_instance_empty.transaction():
            db_manager_instance_empty.execute(
                "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", ("Gen 3", 300.0, "Gas")
            )
            # This should cause an error
            db_manager_instance_empty.execute("INSERT INTO nonexistent_table VALUES (1, 'test')")
    except sqlite3.Error:
        pass  # Expected exception

    # Verify the first insert was not committed
    result = db_manager_instance_empty.query("SELECT COUNT(*) FROM generators")
    assert result[0][0] == 2  # Still just Gen 1 and Gen 2

    # Test nested transaction behavior
    with pytest.raises(sqlite3.Error):
        with db_manager_instance_empty.transaction():
            db_manager_instance_empty.execute(
                "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)",
                ("Gen 4", 400.0, "Coal"),
            )
            with db_manager_instance_empty.transaction():
                # SQLite doesn't support nested transactions like this
                pass


def test_transaction_rollback_on_error(db_manager_instance_empty: SQLiteManager[Any]) -> None:
    """Test that transaction rolls back on error."""
    db = db_manager_instance_empty
    with pytest.raises(sqlite3.Error):
        with db.transaction():
            db.execute(
                "INSERT INTO generators (id, name, capacity) VALUES (?, ?)",
                (1, "gen1", 30),
            )
            db.execute("INSERT INTO nonexistent_table VALUES (1, 'test')")

    result = db.query("SELECT COUNT(*) FROM generators")
    assert result[0][0] == 0, "Transaction should have been rolled back"


def test_context_manager(db_path_on_disk):
    """Test using SQLiteManager as a context manager."""
    db_file_path = str(db_path_on_disk)

    # First, create and populate the database with a context manager
    with SQLiteManager(fpath_or_conn=db_file_path) as db:
        # Create tables
        # Insert test data - use explicit transaction to ensure atomicity
        with db.transaction():
            db.execute("CREATE TABLE generators(name,capacity,fuel_type)")
            db.execute(
                "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)",
                ("Gen 1", 100.0, "Solar"),
            )
            # Add more data to ensure it's not a single-row issue
            db.execute(
                "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)",
                ("Gen 2", 200.0, "Wind"),
            )
        # Force commit and checkpoint
        db.execute("PRAGMA wal_checkpoint(FULL)")
        # Verify data is there within the context
        result = db.query("SELECT COUNT(*) FROM generators")
        assert result[0][0] == 2, "Expected 2 generators to be inserted"

    # The connection should be closed now and file persisted to disk
    assert Path(db_file_path).exists(), f"Database file {db_file_path} does not exist"

    # Give the OS a moment to fully release file locks if needed
    import time

    time.sleep(0.5)  # 500ms delay to be sure

    # Now open the database file directly
    print(f"Opening database file at: {db_file_path}")
    with sqlite3.connect(db_file_path) as conn:
        # Use a direct connection with no fancy options
        cursor = conn.cursor()

        # Debug info
        cursor.execute("PRAGMA database_list")
        db_info = cursor.fetchall()
        print(f"Database info: {db_info}")

        # Verify schema
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"Tables: {tables}")

        # Verify data - change to simple count query first
        cursor.execute("SELECT COUNT(*) FROM generators")
        count_result = cursor.fetchone()
        print(f"Generator count: {count_result}")
        assert count_result is not None and count_result[0] == 2, f"Expected 2 generators, got {count_result}"

        # Then check specific data
        cursor.execute("SELECT name FROM generators WHERE capacity=100.0")
        result = cursor.fetchone()
        assert result is not None, "No data found in generators table"
        assert result[0] == "Gen 1", f"Expected 'Gen 1', got {result[0]}"


def test_close_rollback_error(monkeypatch):
    """Test close method handling rollback errors."""
    db = SQLiteManager()
    db.execute("BEGIN")
    # Create a mock connection where rollback raises an exception
    mock_conn = MagicMock(wraps=db._con)
    mock_conn.in_transaction = True  # Force in_transaction to be True
    mock_conn.rollback.side_effect = sqlite3.Error("Rollback error")

    monkeypatch.setattr(db, "_con", mock_conn)

    # Close should not raise exception even if rollback fails
    db.close()

    mock_conn.rollback.assert_called_once()

    assert db._con is None
