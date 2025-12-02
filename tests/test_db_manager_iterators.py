"""Tests for SQLiteManager iterator methods.

Tests for iter_query() and iter_dicts() methods which provide memory-efficient
iteration over large result sets.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    from plexosdb.db_manager import SQLiteManager


@pytest.fixture
def db_with_large_dataset() -> Generator[SQLiteManager, None, None]:
    """Create a database with multiple rows for iteration testing."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    db.executescript(
        """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL,
            category TEXT
        );
        """
    )

    # Insert test data
    for i in range(1, 51):
        db.execute(
            "INSERT INTO items (name, value, category) VALUES (?, ?, ?)",
            (f"Item {i}", i * 1.5, f"Category {(i % 5) + 1}"),
        )

    yield db
    db.close()


def test_iter_query_yields_all_rows(db_with_large_dataset: SQLiteManager) -> None:
    """Test that iter_query yields all rows from result set."""
    rows = list(db_with_large_dataset.iter_query("SELECT * FROM items"))

    assert len(rows) == 50
    assert rows[0][0] == 1  # First item's id
    assert rows[-1][0] == 50  # Last item's id


def test_iter_query_returns_tuples(db_with_large_dataset: SQLiteManager) -> None:
    """Test that iter_query returns tuples for each row."""
    for row in db_with_large_dataset.iter_query("SELECT * FROM items LIMIT 1"):
        assert isinstance(row, tuple)
        assert len(row) == 4  # id, name, value, category


def test_iter_query_with_tuple_params(db_with_large_dataset: SQLiteManager) -> None:
    """Test iter_query with tuple parameters."""
    rows = list(db_with_large_dataset.iter_query("SELECT * FROM items WHERE id > ? AND id <= ?", (10, 15)))

    assert len(rows) == 5
    assert rows[0][0] == 11
    assert rows[-1][0] == 15


def test_iter_query_with_dict_params(db_with_large_dataset: SQLiteManager) -> None:
    """Test iter_query with dictionary parameters."""
    rows = list(
        db_with_large_dataset.iter_query("SELECT * FROM items WHERE id = :target_id", {"target_id": 25})
    )

    assert len(rows) == 1
    assert rows[0][0] == 25


def test_iter_query_custom_batch_size_small(
    db_with_large_dataset: SQLiteManager,
) -> None:
    """Test iter_query with small batch size."""
    rows = list(db_with_large_dataset.iter_query("SELECT * FROM items", batch_size=5))

    assert len(rows) == 50


def test_iter_query_custom_batch_size_large(
    db_with_large_dataset: SQLiteManager,
) -> None:
    """Test iter_query with large batch size exceeding result set."""
    rows = list(db_with_large_dataset.iter_query("SELECT * FROM items", batch_size=1000))

    assert len(rows) == 50


def test_iter_query_empty_result_set(db_with_large_dataset: SQLiteManager) -> None:
    """Test iter_query with query that returns no rows."""
    rows = list(db_with_large_dataset.iter_query("SELECT * FROM items WHERE id > 1000"))

    assert len(rows) == 0


def test_iter_query_single_row(db_with_large_dataset: SQLiteManager) -> None:
    """Test iter_query with query that returns single row."""
    rows = list(db_with_large_dataset.iter_query("SELECT * FROM items WHERE id = 1"))

    assert len(rows) == 1
    assert rows[0][1] == "Item 1"


def test_iter_query_memory_efficiency() -> None:
    """Test that iter_query processes rows in batches (doesn't load all at once)."""
    from plexosdb.db_manager import SQLiteManager

    # Create a large dataset
    db = SQLiteManager()
    db.executescript(
        """
        CREATE TABLE large_table (
            id INTEGER PRIMARY KEY,
            data TEXT
        );
        """
    )

    # Insert 1000 rows
    for i in range(1, 1001):
        db.execute("INSERT INTO large_table (data) VALUES (?)", (f"Data {i}",))

    # Iterate and verify we can process without loading all into memory
    row_count = 0
    for row in db.iter_query("SELECT * FROM large_table", batch_size=100):
        row_count += 1
        assert len(row) == 2

    assert row_count == 1000
    db.close()


def test_iter_query_with_where_clause(db_with_large_dataset: SQLiteManager) -> None:
    """Test iter_query with WHERE clause filtering."""
    rows = list(db_with_large_dataset.iter_query("SELECT * FROM items WHERE category = ?", ("Category 1",)))

    # Verify filtering works
    assert all(row[3] == "Category 1" for row in rows)
    assert len(rows) == 10  # 50 items / 5 categories = 10 per category


def test_iter_query_with_order_by(db_with_large_dataset: SQLiteManager) -> None:
    """Test iter_query with ORDER BY clause."""
    rows = list(db_with_large_dataset.iter_query("SELECT * FROM items ORDER BY id DESC"))

    # Verify ordering (descending)
    assert rows[0][0] == 50
    assert rows[-1][0] == 1


def test_iter_query_cursor_cleanup_on_success(
    db_with_large_dataset: SQLiteManager,
) -> None:
    """Test that cursor is properly cleaned up after successful iteration."""
    # First iteration
    list(db_with_large_dataset.iter_query("SELECT * FROM items LIMIT 5"))

    # Second iteration should work fine (cursor was cleaned up)
    rows = list(db_with_large_dataset.iter_query("SELECT * FROM items LIMIT 3"))
    assert len(rows) == 3


def test_iter_query_reraises_sqlite_error() -> None:
    """Test that iter_query re-raises sqlite3.Error."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = sqlite3.Error("Query failed")
    mock_conn.cursor.return_value = mock_cursor

    db._con = mock_conn

    with pytest.raises(sqlite3.Error):
        list(db.iter_query("SELECT * FROM nonexistent"))


def test_iter_query_cursor_cleanup_on_error() -> None:
    """Test that cursor is closed even when error occurs during iteration."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = sqlite3.Error("Query failed")
    mock_conn.cursor.return_value = mock_cursor

    db._con = mock_conn

    with pytest.raises(sqlite3.Error):
        list(db.iter_query("SELECT * FROM test"))

    mock_cursor.close.assert_called_once()


# ============================================================================
# iter_dicts() Tests
# ============================================================================


def test_iter_dicts_yields_dictionaries(db_with_large_dataset: SQLiteManager) -> None:
    """Test that iter_dicts yields dictionaries with column names as keys."""
    dicts = list(db_with_large_dataset.iter_dicts("SELECT * FROM items LIMIT 5"))

    assert len(dicts) == 5
    for row_dict in dicts:
        assert isinstance(row_dict, dict)
        assert "id" in row_dict
        assert "name" in row_dict
        assert "value" in row_dict
        assert "category" in row_dict


def test_iter_dicts_all_rows(db_with_large_dataset: SQLiteManager) -> None:
    """Test that iter_dicts yields all rows from result set."""
    dicts = list(db_with_large_dataset.iter_dicts("SELECT * FROM items"))

    assert len(dicts) == 50
    assert dicts[0]["id"] == 1
    assert dicts[-1]["id"] == 50


def test_iter_dicts_with_tuple_params(db_with_large_dataset: SQLiteManager) -> None:
    """Test iter_dicts with tuple parameters."""
    dicts = list(db_with_large_dataset.iter_dicts("SELECT * FROM items WHERE id > ? AND id <= ?", (5, 10)))

    assert len(dicts) == 5
    assert all(5 < d["id"] <= 10 for d in dicts)


def test_iter_dicts_with_dict_params(db_with_large_dataset: SQLiteManager) -> None:
    """Test iter_dicts with dictionary parameters."""
    dicts = list(db_with_large_dataset.iter_dicts("SELECT * FROM items WHERE id = :item_id", {"item_id": 15}))

    assert len(dicts) == 1
    assert dicts[0]["id"] == 15


def test_iter_dicts_custom_batch_size(db_with_large_dataset: SQLiteManager) -> None:
    """Test iter_dicts with custom batch size."""
    dicts = list(db_with_large_dataset.iter_dicts("SELECT * FROM items", batch_size=10))

    assert len(dicts) == 50


def test_iter_dicts_empty_result_set(db_with_large_dataset: SQLiteManager) -> None:
    """Test iter_dicts with query that returns no rows."""
    dicts = list(db_with_large_dataset.iter_dicts("SELECT * FROM items WHERE id > 1000"))

    assert len(dicts) == 0


def test_iter_dicts_column_name_mapping(db_with_large_dataset: SQLiteManager) -> None:
    """Test that iter_dicts correctly maps column names from cursor.description."""
    dicts = list(db_with_large_dataset.iter_dicts("SELECT id, name FROM items LIMIT 1"))

    assert len(dicts) == 1
    row_dict = dicts[0]
    assert "id" in row_dict
    assert "name" in row_dict
    assert "value" not in row_dict
    assert "category" not in row_dict


def test_iter_dicts_with_null_values(db_with_large_dataset: SQLiteManager) -> None:
    """Test that iter_dicts correctly handles NULL values."""
    # Update a row to have NULL value
    db_with_large_dataset.execute("UPDATE items SET value = NULL WHERE id = 10")

    dicts = list(db_with_large_dataset.iter_dicts("SELECT * FROM items WHERE id = 10"))

    assert len(dicts) == 1
    assert dicts[0]["value"] is None


def test_iter_dicts_cursor_cleanup_on_success(
    db_with_large_dataset: SQLiteManager,
) -> None:
    """Test that cursor is properly cleaned up after successful iteration."""
    # First iteration
    list(db_with_large_dataset.iter_dicts("SELECT * FROM items LIMIT 5"))

    # Second iteration should work fine (cursor was cleaned up)
    dicts = list(db_with_large_dataset.iter_dicts("SELECT * FROM items LIMIT 3"))
    assert len(dicts) == 3


def test_iter_dicts_reraises_sqlite_error() -> None:
    """Test that iter_dicts re-raises sqlite3.Error."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = sqlite3.Error("Query failed")
    mock_conn.cursor.return_value = mock_cursor

    db._con = mock_conn

    with pytest.raises(sqlite3.Error):
        list(db.iter_dicts("SELECT * FROM nonexistent"))


def test_iter_dicts_cursor_cleanup_on_error() -> None:
    """Test that cursor is closed even when error occurs during iteration."""
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = sqlite3.Error("Query failed")
    mock_conn.cursor.return_value = mock_cursor

    db._con = mock_conn

    with pytest.raises(sqlite3.Error):
        list(db.iter_dicts("SELECT * FROM test"))

    mock_cursor.close.assert_called_once()


def test_fetchmany_happy_path(db_with_large_dataset: SQLiteManager) -> None:
    """Test fetchmany() successful execution returns correct number of rows."""
    result = db_with_large_dataset.fetchmany("SELECT * FROM items", size=2)
    assert len(result) == 2
    assert result[0][1] == "Item 1"
    assert result[1][1] == "Item 2"


def test_fetchmany_default_size(db_with_large_dataset: SQLiteManager) -> None:
    """Test fetchmany() with default size parameter."""
    result = db_with_large_dataset.fetchmany("SELECT * FROM items")
    assert len(result) > 0
    assert isinstance(result, list)
