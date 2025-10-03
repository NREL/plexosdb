import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from plexosdb.db_manager import SQLiteManager

TEST_SCHEMA = (
    "CREATE TABLE generators (id INTEGER PRIMARY KEY, name TEXT, capacity REAL, fuel_type TEXT);"
    "CREATE TABLE properties (id INTEGER PRIMARY KEY, generator_id INTEGER, property_name TEXT, "
    "value REAL, FOREIGN KEY(generator_id) REFERENCES generators(id));"
)


def test_sqlite_version(db_manager_instance_empty):
    db = db_manager_instance_empty

    assert isinstance(db.sqlite_version, str)


def test_create_sqlite_manager_instance():
    """Test creating a SQLiteManager instance."""
    db = SQLiteManager()
    assert db is not None
    assert hasattr(db, "connection")
    assert isinstance(db._con, sqlite3.Connection)
    db.close()


def test_create_collations():
    """Test creating custom collations."""
    db = SQLiteManager()

    # # Test default no_space collation
    # db.execute("CREATE TABLE test_collation (id INTEGER PRIMARY KEY, value TEXT)")
    # db.execute("INSERT INTO test_collation (value) VALUES (?)", ("Hello World",))
    # db.execute("INSERT INTO test_collation (value) VALUES (?)", ("HelloWorld",))
    #
    # # Query using the NOSPACE collation
    # results = db.query("SELECT value FROM test_collation WHERE value = ? COLLATE NOSPACE", ("Hello World",))
    # assert len(results) == 2  # Should match both with and without space

    # Test adding custom collation
    def reverse_collation(s1, s2):
        return -1 if s1 > s2 else 1 if s1 < s2 else 0

    success = db.add_collation("REVERSE", reverse_collation)
    assert success is True
    db.close()


def test_sqlite_configuration(tmp_path):
    """Test SQLite configuration settings."""
    # Test in-memory database (default)
    db_mem = SQLiteManager()

    # Test file-based database
    temp_file = tmp_path / "test.sqlite"  # Special file URI for testing
    db_file = SQLiteManager(fpath_or_conn=temp_file)

    # Verify memory database has memory settings
    mem_pragmas = {
        "synchronous": db_mem.query("PRAGMA synchronous")[0][0],
        "journal_mode": db_mem.query("PRAGMA journal_mode")[0][0].upper(),
        "foreign_keys": db_mem.query("PRAGMA foreign_keys")[0][0],
    }

    # Verify file database has file settings
    file_pragmas = {
        "synchronous": db_file.query("PRAGMA synchronous")[0][0],
        "journal_mode": db_file.query("PRAGMA journal_mode")[0][0].upper(),
        "foreign_keys": db_file.query("PRAGMA foreign_keys")[0][0],
    }

    print(f"Memory database pragmas: {mem_pragmas}")
    print(f"File database pragmas: {file_pragmas}")

    # Check memory database settings (in-memory uses OFF=0, MEMORY journal mode)
    assert mem_pragmas["synchronous"] == 0, "Memory DB should use OFF synchronous mode"
    assert mem_pragmas["journal_mode"] in ["MEMORY"], "Memory DB should use MEMORY journal mode"
    assert mem_pragmas["foreign_keys"] == 1, "Foreign keys should be enabled"

    # Check file database settings (file uses NORMAL=1, WAL journal mode)
    assert file_pragmas["synchronous"] == 1, "File DB should use NORMAL synchronous mode"
    assert file_pragmas["journal_mode"] in ["WAL"], "File DB should use WAL journal mode"
    assert file_pragmas["foreign_keys"] == 1, "Foreign keys should be enabled"

    # Clean up
    db_mem.close()
    db_file.close()


def test_reading_from_different_connection(db_manager_instance_populated, db_path_on_disk):
    """Test reading from a different connection after backup."""
    # First, create a backup
    success = db_manager_instance_populated.backup(db_path_on_disk)
    assert success is True
    assert Path(db_path_on_disk).exists()

    # Connect to the backup with a new connection
    with sqlite3.connect(str(db_path_on_disk)) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM generators")
        count = cursor.fetchone()[0]
        assert count == 3

        cursor.execute("SELECT name FROM generators WHERE capacity = ?", (500.0,))
        result = cursor.fetchone()
        assert result[0] == "Coal Plant 1"


def test_sqlite_manager_arguments():
    """Test SQLiteManager initialization with different arguments."""
    # Test with custom connection
    conn = sqlite3.connect(":memory:")
    db = SQLiteManager(fpath_or_conn=conn, initialize=False)
    assert db._con == conn
    db.close()

    # Test with initialize=False
    db_no_init = SQLiteManager()
    tables = db_no_init.query("SELECT name FROM sqlite_master WHERE type='table'")
    assert len(tables) == 0
    db_no_init.close()


def test_sqlite_manager_backup(db_manager_instance_populated, tmp_path):
    """Test database backup functionality."""
    backup_path = tmp_path / "backup.db"

    # Test successful backup
    success = db_manager_instance_populated.backup(backup_path)
    assert success is True
    assert backup_path.exists()

    # Verify backup contains correct data
    with sqlite3.connect(str(backup_path)) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM generators")
        count = cursor.fetchone()[0]
        assert count == 3

    invalid_path = "NUL" if os.name == "nt" else "/dev/null/invalid.db"
    success = db_manager_instance_populated.backup(invalid_path)
    assert success is False


def test_sqlite_manager_execute(db_manager_instance_empty):
    """Test execute functionality."""
    # Test successful execute
    success = db_manager_instance_empty.execute(
        "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", ("Test Gen", 100.0, "Solar")
    )
    assert success is True

    # Verify insert worked
    result = db_manager_instance_empty.query("SELECT name FROM generators WHERE capacity=100.0")
    assert result[0][0] == "Test Gen"

    # Test execute with error - directly handle the possible exception
    try:
        success = db_manager_instance_empty.execute("INSERT INTO nonexistent_table VALUES (1, 'test')")
        # If we get here without an exception, ensure it returned False
        assert success is False
    except sqlite3.Error:
        # If we got an exception, mark the test as failed, since we're expecting it to be handled silently
        pytest.fail("execute() should handle SQLite errors but didn't")

    # Test update
    success = db_manager_instance_empty.execute(
        "UPDATE generators SET capacity=110.0 WHERE name=?", ("Test Gen",)
    )
    assert success is True
    result = db_manager_instance_empty.query("SELECT capacity FROM generators WHERE name=?", ("Test Gen",))
    assert result[0][0] == 110.0

    # Test delete
    success = db_manager_instance_empty.execute("DELETE FROM generators WHERE name=?", ("Test Gen",))
    assert success is True
    result = db_manager_instance_empty.query("SELECT COUNT(*) FROM generators")
    assert result[0][0] == 0


def test_sqlite_manager_query(db_manager_instance_populated):
    """Test query functionality."""
    # Test basic query
    result = db_manager_instance_populated.query("SELECT name, capacity FROM generators ORDER BY capacity")
    assert len(result) == 3
    assert result[0][0] == "Wind Farm 1"
    assert result[0][1] == 150.0

    # Test query with parameters
    result = db_manager_instance_populated.query("SELECT name FROM generators WHERE capacity > ?", (300,))
    names = [row[0] for row in result]
    assert len(names) == 1
    assert "Coal Plant 1" in names

    # Test query with no results
    result = db_manager_instance_populated.query("SELECT name FROM generators WHERE capacity > 1000")
    assert len(result) == 0

    # Test query with error
    with pytest.raises(sqlite3.Error):
        db_manager_instance_populated.query("SELECT * FROM nonexistent_table")


def test_sqlite_manager_executemany(db_manager_instance_empty):
    """Test executemany functionality."""
    # Test successful executemany
    generators = [
        ("Gen 1", 100.0, "Solar"),
        ("Gen 2", 200.0, "Wind"),
        ("Gen 3", 300.0, "Gas"),
        ("Gen 4", 400.0, "Coal"),
    ]

    success = db_manager_instance_empty.executemany(
        "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", generators
    )
    assert success is True

    # Verify all generators were inserted
    result = db_manager_instance_empty.query("SELECT COUNT(*) FROM generators")
    assert result[0][0] == 4

    # Test executemany with error - trying to insert into a non-existent table
    nonexistent_data = [("Data1", 100), ("Data2", 200)]

    success = db_manager_instance_empty.executemany(
        "INSERT INTO nonexistent_table (name, value) VALUES (?, ?)", nonexistent_data
    )
    assert success is False


def test_sqlite_manager_executescript(db_manager_instance_empty):
    """Test executescript functionality."""
    # Test successful executescript
    script = """
    INSERT INTO generators (name, capacity, fuel_type) VALUES ('Gen 1', 100.0, 'Solar');
    INSERT INTO generators (name, capacity, fuel_type) VALUES ('Gen 2', 200.0, 'Wind');
    """

    success = db_manager_instance_empty.executescript(script)
    assert success is True

    # Verify both inserts worked
    result = db_manager_instance_empty.query("SELECT COUNT(*) FROM generators")
    assert result[0][0] == 2

    # Test executescript with error
    script_with_error = """
    INSERT INTO generators (name, capacity, fuel_type) VALUES ('Gen 3', 300.0, 'Gas');
    INSERT INTO nonexistent_table VALUES (1, 'test');
    """

    success = db_manager_instance_empty.executescript(script_with_error)
    assert success is False

    # Verify first insert did not work (rollback)
    result = db_manager_instance_empty.query("SELECT COUNT(*) FROM generators")
    assert result[0][0] == 2  # Still just Gen 1 and Gen 2


def test_sqlite_manager_iter_query(db_manager_instance_populated):
    """Test iter_query functionality."""
    # Test basic iter_query
    results = list(
        db_manager_instance_populated.iter_query("SELECT name, capacity FROM generators ORDER BY capacity")
    )
    assert len(results) == 3
    assert results[0][0] == "Wind Farm 1"
    assert results[0][1] == 150.0

    # Test iter_query with parameters
    results = list(
        db_manager_instance_populated.iter_query("SELECT name FROM generators WHERE capacity > ?", (300,))
    )
    names = [row[0] for row in results]
    assert len(names) == 1
    assert "Coal Plant 1" in names

    # Test iter_query with custom batch size
    # First, add many rows
    generators = [(f"Gen{i}", 20 + i, "Fuel") for i in range(50)]
    db_manager_instance_populated.executemany(
        "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", generators
    )

    # Then retrieve with small batch size
    results = list(
        db_manager_instance_populated.iter_query("SELECT name FROM generators ORDER BY id", batch_size=10)
    )
    assert len(results) == 53  # 3 original + 50 new generators

    # Test iter_query with error
    with pytest.raises(sqlite3.Error):
        list(db_manager_instance_populated.iter_query("SELECT * FROM nonexistent_table"))


def test_sqlite_manager_last_insert_rowid(db_manager_instance_empty):
    """Test last_insert_rowid functionality."""
    # Test after single insert
    db_manager_instance_empty.execute(
        "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", ("Test Gen", 100.0, "Solar")
    )
    rowid = db_manager_instance_empty.last_insert_rowid()
    assert rowid == 1

    # Test after another insert
    db_manager_instance_empty.execute(
        "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", ("Test Gen 2", 200.0, "Wind")
    )
    rowid = db_manager_instance_empty.last_insert_rowid()
    assert rowid == 2

    # Test edge case with empty result
    # This is unlikely but we should test our exception handling
    original_query = db_manager_instance_empty.query

    def mock_query(*args, **kwargs):
        return []

    db_manager_instance_empty.query = mock_query
    rowid = db_manager_instance_empty.last_insert_rowid()
    assert rowid == 0

    # Restore original query method
    db_manager_instance_empty.query = original_query


def test_sqlite_manager_optimize(db_manager_instance_populated):
    """Test optimize functionality."""
    # Simple test to verify it doesn't throw errors
    success = db_manager_instance_populated.optimize()
    assert success is True


def test_execute_in_function(db_manager_instance_empty):
    """Test that execute raises exceptions when in a transaction."""
    # Start a transaction
    with pytest.raises(sqlite3.Error):
        with db_manager_instance_empty.transaction():
            # This should succeed
            db_manager_instance_empty.execute(
                "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)",
                ("Gen 1", 100.0, "Solar"),
            )

            # This should raise an exception in transaction mode instead of returning False
            db_manager_instance_empty.execute("INSERT INTO nonexistent_table VALUES (1, 'test')")

    # Verify transaction was rolled back
    result = db_manager_instance_empty.query("SELECT COUNT(*) FROM generators")
    assert result[0][0] == 0, "Transaction should have been rolled back"


def test_executemany_in_transaction(db_manager_instance_empty):
    """Test that executemany raises exceptions when in a transaction."""
    generators = [("Gen 1", 100.0, "Solar"), ("Gen 2", 200.0, "Wind")]
    invalid_data = [(1, "test"), (2, "test2")]

    # Test with transaction
    with pytest.raises(sqlite3.Error):
        with db_manager_instance_empty.transaction():
            # This should succeed
            db_manager_instance_empty.executemany(
                "INSERT INTO generators (name, capacity, fuel_type) VALUES (?, ?, ?)", generators
            )

            # This should raise an exception in transaction mode
            db_manager_instance_empty.executemany("INSERT INTO nonexistent_table VALUES (?, ?)", invalid_data)

    # Verify transaction was rolled back
    result = db_manager_instance_empty.query("SELECT COUNT(*) FROM generators")
    assert result[0][0] == 0, "Transaction should have been rolled back"


def test_failed_collation_creation(monkeypatch):
    """Test handling of failed collation creation."""
    db = SQLiteManager()

    # Create a mock connection with a create_collation method that raises an exception
    mock_conn = MagicMock()
    mock_conn.create_collation.side_effect = sqlite3.Error("Failed to create collation")

    # Replace the internal _con attribute instead of the connection property
    monkeypatch.setattr(db, "_con", mock_conn)

    # Test collation creation failure
    result = db.add_collation("TEST_COLLATION", lambda x, y: 0)
    assert result is False, "Should return False when collation creation fails"

    db.close()


def test_close_with_transaction_errors(db_manager_instance_empty):
    """Test close method handling transaction errors."""
    # First start a transaction
    db_manager_instance_empty.execute("BEGIN")

    # Now close - should handle the active transaction
    db_manager_instance_empty.close()

    # If we got here without exception, the test passes
    assert db_manager_instance_empty._con is None, "Connection should be None after close"


def test_optimize_in_transaction(db_manager_instance_empty):
    """Test optimize when in transaction."""
    # Start a transaction
    db_manager_instance_empty.execute("BEGIN")

    # Run optimize which should issue a warning but not fail
    success = db_manager_instance_empty.optimize()

    # Should still succeed despite the warning
    assert success is True, "Optimize should succeed even with transaction warning"

    # Clean up
    db_manager_instance_empty.connection.commit()


def test_optimize_additional_cases(monkeypatch):
    """Test additional cases for optimize functionality."""
    db = SQLiteManager()

    # Test optimize with mocked execute to simulate failure in ANALYZE
    original_execute = db.execute

    def mock_execute_fail(sql, *args, **kwargs):
        if sql.startswith("ANALYZE"):
            # Instead of returning False, raise an exception
            raise sqlite3.Error("Mock ANALYZE error")
        return original_execute(sql, *args, **kwargs)

    monkeypatch.setattr(db, "execute", mock_execute_fail)
    success = db.optimize()
    assert success is False, "Optimize should fail if ANALYZE fails"

    # Test optimize with closed connection
    db_closed = SQLiteManager()
    db_closed.close()

    # Instead of calling optimize directly, we need to handle the assertion error
    try:
        db_closed.optimize()
        assert False, "optimize() should raise an AssertionError when connection is closed"
    except AssertionError as e:
        # This is the expected behavior - connection is closed
        assert "Database connection is not initialized" in str(e)

    # Test simple optimize with a normal database
    db_normal = SQLiteManager()
    db_normal.executescript(TEST_SCHEMA)
    success = db_normal.optimize()  # Removed the tables parameter
    assert success is True, "Optimize should succeed with normal database"
    db_normal.close()

    # Test normal optimize - vacuum happens by default
    db_vacuum = SQLiteManager()
    success = db_vacuum.optimize()  # Removed the vacuum parameter
    assert success is True, "Optimize with vacuum should succeed"
    db_vacuum.close()


def test_close_additional_cases(monkeypatch):
    """Test additional close cases."""
    # Test close when already closed
    db = SQLiteManager()
    db.close()
    # Second close should not error
    db.close()
    assert db._con is None

    # Test close with error during commit
    db2 = SQLiteManager()

    # Create a mock connection with commit that raises an exception
    mock_conn = MagicMock()
    mock_conn.in_transaction = True
    mock_conn.commit.side_effect = sqlite3.Error("Error during commit")

    # Replace the entire connection object instead of just the commit method
    _ = db2._con
    db2._con = mock_conn

    # Close should handle the error and still close the connection
    db2.close()
    assert db2._con is None

    # Test that close properly calls the connection's close method
    db3 = SQLiteManager()

    # Create connection with close method that we can spy on
    mock_conn = MagicMock(wraps=db3._con)
    db3._con = mock_conn

    db3.close()

    mock_conn.close.assert_called_once()


def test_no_default_schema():
    """Test that no schema is created by default."""
    db = SQLiteManager()

    # Should have no tables
    tables = db.query("SELECT name FROM sqlite_master WHERE type='table'")
    assert len(tables) == 0, "No tables should be created without an explicit schema"

    db.close()


def test_in_memory_flag_setting(tmp_path):
    """Test that _is_in_memory method works correctly based on initialization parameters."""
    fpath_or_conn = tmp_path / "test.db"
    db1 = SQLiteManager(fpath_or_conn=fpath_or_conn)
    assert db1._is_in_memory() is False
    db1.close()

    db2 = SQLiteManager()  # Default is in-memory
    assert db2._is_in_memory() is True
    db2.close()

    # Test that we can create a file database
    db3 = SQLiteManager(fpath_or_conn=fpath_or_conn)
    assert db3._is_in_memory() is False
    db3.close()


def test_backup_path_handling(db_manager_instance_populated, tmp_path):
    """Test backup method path handling."""
    path_obj = tmp_path / "backup_path.db"
    success = db_manager_instance_populated.backup(path_obj)
    assert success is True
    assert path_obj.exists()

    str_path = str(tmp_path / "backup_string.db")
    success = db_manager_instance_populated.backup(str_path)
    assert success is True
    assert Path(str_path).exists()

    nested_path = tmp_path / "new_dir" / "nested.db"
    success = db_manager_instance_populated.backup(nested_path)
    assert success is True
    assert nested_path.exists()

    import os

    if os.name != "nt":  # Skip on Windows where permission model is different
        restricted_dir = tmp_path / "restricted"
        restricted_dir.mkdir()
        restricted_dir.chmod(0o000)  # Remove all permissions
        try:
            restricted_path = restricted_dir / "cant_write.db"
            success = db_manager_instance_populated.backup(restricted_path)
            assert success is False
        finally:
            restricted_dir.chmod(0o755)


def test_last_insert_rowid_index_error(db_manager_instance_empty, monkeypatch):
    """Test last_insert_rowid handling of IndexError."""

    # Create a mock query method that returns an empty result
    def mock_query(*args, **kwargs):
        return []

    # Apply the mock to the db instance
    monkeypatch.setattr(db_manager_instance_empty, "query", mock_query)

    # Test that it returns 0 instead of raising an IndexError
    rowid = db_manager_instance_empty.last_insert_rowid()
    assert rowid == 0


def test_optimize_vacuum_failure(db_manager_instance_populated, monkeypatch):
    """Test optimize handling of VACUUM failure."""
    real_connection = db_manager_instance_populated.connection

    mock_conn = MagicMock(wraps=real_connection)

    def mock_execute(sql, *args, **kwargs):
        if sql == "VACUUM":
            raise sqlite3.Error("VACUUM failed")
        return real_connection.execute(sql, *args, **kwargs)

    mock_conn.execute = mock_execute
    monkeypatch.setattr(db_manager_instance_populated, "_con", mock_conn)

    result = db_manager_instance_populated.optimize()
    assert result is False, "Optimize should fail when VACUUM fails"


def test_close_commit_for_file_db(monkeypatch):
    """Test close method committing for file-based database."""
    db = SQLiteManager()

    # Create a mock connection to spy on commit calls
    mock_conn = MagicMock(wraps=db._con)
    mock_conn.in_transaction = False  # Not in transaction

    # Mock the _is_in_memory method to return False (file database)
    def mock_is_in_memory():
        return False

    # Replace the connection and _is_in_memory method
    monkeypatch.setattr(db, "_con", mock_conn)
    monkeypatch.setattr(db, "_is_in_memory", mock_is_in_memory)

    db.close()

    # Verify commit was called (only for file databases)
    mock_conn.commit.assert_called_once()

    # Connection should be set to None
    assert db._con is None


def test_close_connection_error(monkeypatch):
    """Test close method handling errors from connection.close()."""
    db = SQLiteManager()

    # Create mock connection where close raises an exception
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_conn.close.side_effect = sqlite3.Error("Close failed")

    # Replace the connection
    monkeypatch.setattr(db, "_con", mock_conn)

    # Close should not raise exception even if connection.close fails
    db.close()

    # Verify close was attempted
    mock_conn.close.assert_called_once()

    # Connection should be set to None despite error
    assert db._con is None


def test_fetching_methods(db_manager_instance_populated):
    db = db_manager_instance_populated
    result_many = db.fetchmany("SELECT * from generators", size=1)
    assert result_many
    assert len(result_many) == 1

    result_one = db.fetchone("SELECT * from generators")
    assert result_one
    assert len(result_one) == 4
    assert [result_one] == result_many

    result_one_dict = db.fetchone_dict("SELECT * from generators")
    assert result_one_dict
    assert isinstance(result_one_dict, dict)

    for generator in db.iter_dicts("SELECT * from generators"):
        assert isinstance(generator, dict)
