from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from plexosdb.db_manager import SQLiteManager


@pytest.fixture
def db_with_users_table() -> Generator[SQLiteManager, None, None]:
    from plexosdb.db_manager import SQLiteManager

    db = SQLiteManager()
    db.executescript(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER
        );
        """
    )
    yield db
    db.close()


def test_insert_records_single_record_success(db_with_users_table: SQLiteManager) -> None:
    record = {"name": "Alice", "email": "alice@example.com", "age": 30}
    result = db_with_users_table.insert_records("users", record)

    assert result is True
    rows = db_with_users_table.query("SELECT name, email, age FROM users")
    assert len(rows) == 1
    assert rows[0] == ("Alice", "alice@example.com", 30)


def test_insert_records_batch_multiple_records(db_with_users_table: SQLiteManager) -> None:
    records = [
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]
    result = db_with_users_table.insert_records("users", records)

    assert result is True
    rows = db_with_users_table.query("SELECT COUNT(*) FROM users")
    assert rows[0][0] == 3


def test_insert_records_empty_dict_raises_error(db_with_users_table: SQLiteManager) -> None:
    with pytest.raises(ValueError, match="Records cannot be empty"):
        db_with_users_table.insert_records("users", {})


def test_insert_records_empty_list_raises_error(db_with_users_table: SQLiteManager) -> None:
    with pytest.raises(ValueError, match="Records cannot be empty"):
        db_with_users_table.insert_records("users", [])


def test_insert_records_nonexistent_table_raises_error(db_with_users_table: SQLiteManager) -> None:
    record = {"name": "Alice", "email": "alice@example.com", "age": 30}
    with pytest.raises(ValueError, match="Table 'nonexistent' does not exist"):
        db_with_users_table.insert_records("nonexistent", record)


def test_insert_records_inconsistent_keys_raises_error(db_with_users_table: SQLiteManager) -> None:
    records = [
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com"},
    ]
    with pytest.raises(KeyError, match="All records must have the same keys"):
        db_with_users_table.insert_records("users", records)


def test_insert_records_with_null_values(db_with_users_table: SQLiteManager) -> None:
    record = {"name": "Bob", "email": None, "age": None}
    result = db_with_users_table.insert_records("users", record)

    assert result is True
    rows = db_with_users_table.query("SELECT name, email, age FROM users")
    assert rows[0] == ("Bob", None, None)


def test_insert_records_unique_constraint_violation(db_with_users_table: SQLiteManager) -> None:
    record1 = {"name": "Alice", "email": "alice@example.com", "age": 30}
    record2 = {"name": "Alice2", "email": "alice@example.com", "age": 31}

    db_with_users_table.insert_records("users", record1)

    result = db_with_users_table.insert_records("users", record2)
    assert result is False


def test_insert_records_not_null_constraint_violation(db_with_users_table: SQLiteManager) -> None:
    record = {"email": "alice@example.com", "age": 30}

    result = db_with_users_table.insert_records("users", record)
    assert result is False


def test_insert_records_returns_true_and_preserves_original(db_with_users_table: SQLiteManager) -> None:
    record = {"name": "Charlie", "email": "charlie@example.com", "age": 35}
    original = record.copy()

    result = db_with_users_table.insert_records("users", record)

    assert result is True
    assert record == original
