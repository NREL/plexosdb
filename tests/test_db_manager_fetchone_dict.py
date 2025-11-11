"""Tests for SQLiteManager.fetchone_dict() method."""

from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from plexosdb.db_manager import SQLiteManager


@pytest.fixture
def db_with_sample_table() -> Generator[SQLiteManager, None, None]:
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    db.executescript(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER
        );
        INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30);
        INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 25);
        INSERT INTO users VALUES (3, 'Charlie', NULL, 35);
        """
    )
    yield db
    db.close()


def test_fetchone_dict_returns_dict_with_column_names(db_with_sample_table: SQLiteManager) -> None:
    """Test that fetchone_dict returns a dictionary with column names as keys."""
    result = db_with_sample_table.fetchone_dict("SELECT * FROM users WHERE id = 1")

    assert isinstance(result, dict)
    assert "id" in result
    assert "name" in result
    assert "email" in result
    assert "age" in result
    assert result["id"] == 1
    assert result["name"] == "Alice"


def test_fetchone_dict_with_params_tuple(db_with_sample_table: SQLiteManager) -> None:
    """Test fetchone_dict with tuple parameters."""
    result = db_with_sample_table.fetchone_dict("SELECT * FROM users WHERE id = ?", (2,))

    assert result is not None
    assert result["name"] == "Bob"
    assert result["age"] == 25


def test_fetchone_dict_with_params_dict(db_with_sample_table: SQLiteManager) -> None:
    """Test fetchone_dict with dictionary parameters."""
    result = db_with_sample_table.fetchone_dict("SELECT * FROM users WHERE name = :name", {"name": "Charlie"})

    assert result is not None
    assert result["id"] == 3
    assert result["email"] is None


def test_fetchone_dict_returns_none_when_no_results(db_with_sample_table: SQLiteManager) -> None:
    """Test that fetchone_dict returns None when no results match."""
    result = db_with_sample_table.fetchone_dict("SELECT * FROM users WHERE id = ?", (999,))

    assert result is None


def test_fetchone_dict_with_null_values(db_with_sample_table: SQLiteManager) -> None:
    """Test that fetchone_dict correctly handles NULL values in columns."""
    result = db_with_sample_table.fetchone_dict("SELECT * FROM users WHERE id = 3")

    assert result is not None
    assert result["email"] is None
    assert result["name"] == "Charlie"


def test_fetchone_dict_returns_first_row_only(db_with_sample_table: SQLiteManager) -> None:
    """Test that fetchone_dict returns only the first row, not all rows."""
    result = db_with_sample_table.fetchone_dict("SELECT * FROM users")

    assert result is not None
    assert result["id"] == 1
    assert result["name"] == "Alice"


def test_fetchone_dict_with_subset_of_columns(db_with_sample_table: SQLiteManager) -> None:
    """Test fetchone_dict with SELECT specifying subset of columns."""
    result = db_with_sample_table.fetchone_dict("SELECT name, email FROM users WHERE id = 2")

    assert result is not None
    assert "name" in result
    assert "email" in result
    assert "id" not in result
    assert "age" not in result


def test_fetchone_dict_with_no_params(db_with_sample_table: SQLiteManager) -> None:
    """Test fetchone_dict without parameters."""
    result = db_with_sample_table.fetchone_dict("SELECT * FROM users LIMIT 1")

    assert result is not None
    assert isinstance(result, dict)


def test_fetchone_dict_cursor_closed_after_use(db_with_sample_table: SQLiteManager) -> None:
    """Test that cursor is properly closed even after successful execution."""
    result1 = db_with_sample_table.fetchone_dict("SELECT * FROM users WHERE id = 1")
    result2 = db_with_sample_table.fetchone_dict("SELECT * FROM users WHERE id = 2")

    assert result1 is not None
    assert result2 is not None
    assert result1["name"] == "Alice"
    assert result2["name"] == "Bob"
