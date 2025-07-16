import pytest
from plexosdb.db_manager import SQLiteManager
from collections.abc import Generator
from typing import Any


@pytest.fixture(scope="function")
def db_instance_empty() -> Generator[SQLiteManager[Any], None, None]:
    db: SQLiteManager[Any] = SQLiteManager()
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);")
    yield db
    db.close()


def test_executemany_success_in_transaction(db_instance_empty: SQLiteManager[Any]) -> None:
    """Test that executemany succeeds in a transaction."""
    users = [("Alice", 30), ("Bob", 25)]
    with db_instance_empty.transaction():
        db_instance_empty.executemany("INSERT INTO users (name, age) VALUES (?, ?)", users)

    result = db_instance_empty.query("SELECT COUNT(*) FROM users")
    assert result[0][0] == 2, "All users should be inserted"
