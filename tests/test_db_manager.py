import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from plexosdb.db_manager import SQLiteManager


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

    def reverse_collation(s1, s2):
        return -1 if s1 > s2 else 1 if s1 < s2 else 0

    success = db.add_collation("REVERSE", reverse_collation)
    assert success is True
    db.close()


def test_sqlite_configuration(tmp_path):
    """Test SQLite configuration settings."""
    db_mem = SQLiteManager()

    temp_file = tmp_path / "test.sqlite"  # Special file URI for testing
    db_file = SQLiteManager(fpath_or_conn=temp_file)

    mem_pragmas = {
        "synchronous": db_mem.query("PRAGMA synchronous")[0][0],
        "journal_mode": db_mem.query("PRAGMA journal_mode")[0][0].upper(),
        "foreign_keys": db_mem.query("PRAGMA foreign_keys")[0][0],
    }

    file_pragmas = {
        "synchronous": db_file.query("PRAGMA synchronous")[0][0],
        "journal_mode": db_file.query("PRAGMA journal_mode")[0][0].upper(),
        "foreign_keys": db_file.query("PRAGMA foreign_keys")[0][0],
    }

    assert mem_pragmas["synchronous"] == 0, "Memory DB should use OFF synchronous mode"
    assert mem_pragmas["journal_mode"] in ["MEMORY"], "Memory DB should use MEMORY journal mode"
    assert mem_pragmas["foreign_keys"] == 1, "Foreign keys should be enabled"

    assert file_pragmas["synchronous"] == 1, "File DB should use NORMAL synchronous mode"
    assert file_pragmas["journal_mode"] in ["WAL"], "File DB should use WAL journal mode"
    assert file_pragmas["foreign_keys"] == 1, "Foreign keys should be enabled"

    db_mem.close()
    db_file.close()


def test_sqlite_manager_arguments():
    """Test SQLiteManager initialization with different arguments."""
    conn = sqlite3.connect(":memory:")
    db = SQLiteManager(fpath_or_conn=conn, initialize=False)
    assert db._con == conn
    db.close()

    db_no_init = SQLiteManager()
    tables = db_no_init.query("SELECT name FROM sqlite_master WHERE type='table'")
    assert len(tables) == 0
    db_no_init.close()


def test_sqlite_manager_backup(db_base, tmp_path):
    """Test database backup functionality."""
    backup_path = tmp_path / "backup.db"

    db = db_base._db
    success = db.backup(backup_path)
    assert success is True
    assert backup_path.exists()

    with sqlite3.connect(str(backup_path)) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM t_class")
        count = cursor.fetchone()[0]
        assert count > 0, "Table t_class is empty."

    invalid_path = "NUL" if os.name == "nt" else "/dev/null/invalid.db"
    success = db.backup(invalid_path)
    assert success is False


def test_sqlite_manager_executescript(db_base):
    """Test executescript functionality."""
    db = db_base._db
    script = """
    INSERT INTO t_object (name, description, GUID) VALUES ('Gen 1', 'SolarGen', 'test');
    INSERT INTO t_object (name, description, GUID) VALUES ('Gen 2', 'ThermalGen', 'test');
    """
    success = db.executescript(script)
    assert success is True


def test_sqlite_manager_optimize(db_base):
    """Test optimize functionality."""
    success = db_base._db.optimize()
    assert success is True


def test_failed_collation_creation(monkeypatch):
    """Test handling of failed collation creation."""
    db = SQLiteManager()

    # Store the original connection to close it later (prevents resource leak)
    original_conn = db._con

    mock_conn = MagicMock()
    mock_conn.create_collation.side_effect = sqlite3.Error("Failed to create collation")

    monkeypatch.setattr(db, "_con", mock_conn)

    result = db.add_collation("TEST_COLLATION", lambda x, y: 0)
    assert result is False, "Should return False when collation creation fails"

    # Restore and close the original connection to avoid ResourceWarning
    db._con = original_conn
    db.close()


def test_no_default_schema():
    """Test that no schema is created by default."""
    db = SQLiteManager()

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


def test_backup_path_handling(db_base, tmp_path):
    """Test backup method path handling."""
    db_manager_instance_populated = db_base._db
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


def test_config_property_returns_config():
    """Test that config property returns SQLiteConfig instance."""
    from plexosdb.db_manager import SQLiteConfig

    db = SQLiteManager()

    config = db.config

    assert config is not None
    assert isinstance(config, SQLiteConfig)
    db.close()


def test_config_property_in_memory():
    """Test config property for in-memory database."""
    db = SQLiteManager()  # In-memory by default

    config = db.config

    assert config.cache_size_mb == 50
    assert config.mmap_size_gb == 0
    assert config.synchronous == "OFF"
    assert config.journal_mode == "MEMORY"
    db.close()


def test_config_property_file_database(tmp_path):
    """Test config property for file-based database."""
    db_path = tmp_path / "test.db"
    db = SQLiteManager(fpath_or_conn=db_path)

    config = db.config

    assert config.cache_size_mb == 20
    assert config.mmap_size_gb == 2
    assert config.synchronous == "NORMAL"
    assert config.journal_mode == "WAL"
    db.close()


def test_sqlite_version_property():
    """Test sqlite_version property returns version string."""
    db = SQLiteManager()

    version = db.sqlite_version

    assert isinstance(version, str)
    # Version should be in format like "3.43.0"
    parts = version.split(".")
    assert len(parts) >= 2
    assert all(part.isdigit() for part in parts[:2])
    db.close()


def test_init_with_invalid_type_raises_type_error():
    """Test that __init__ raises TypeError for invalid input type."""
    with pytest.raises(TypeError):
        SQLiteManager(fpath_or_conn=12345)  # Invalid type: int


def test_init_with_list_raises_type_error():
    """Test that __init__ raises TypeError for list input."""
    with pytest.raises(TypeError):
        SQLiteManager(fpath_or_conn=[])  # Invalid type: list


def test_init_with_dict_raises_type_error():
    """Test that __init__ raises TypeError for dict input."""
    with pytest.raises(TypeError):
        SQLiteManager(fpath_or_conn={})  # Invalid type: dict


def test_transaction_context_manager_success():
    """Test transaction context manager executes successfully."""
    db = SQLiteManager()
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

    with db.transaction():
        db.execute("INSERT INTO test (value) VALUES (?)", ("test1",))
        db.execute("INSERT INTO test (value) VALUES (?)", ("test2",))

    # Verify data was committed
    rows = db.query("SELECT * FROM test")
    assert len(rows) == 2

    db.close()


def test_transaction_context_manager_rollback_on_error():
    """Test transaction rolls back when sqlite3.Error occurs."""
    db = SQLiteManager()
    db.execute("CREATE TABLE unique_test (id INTEGER PRIMARY KEY UNIQUE, value TEXT)")
    db.execute("INSERT INTO unique_test VALUES (1, 'initial')")

    try:
        with db.transaction():
            # This will fail because of duplicate primary key (database error)
            db.execute("INSERT INTO unique_test VALUES (1, 'duplicate')")
    except sqlite3.Error:
        pass

    # Verify only initial row exists (rollback occurred)
    rows = db.query("SELECT * FROM unique_test")
    assert len(rows) == 1
    assert rows[0][1] == "initial"

    db.close()


def test_transaction_reraises_sqlite_error():
    """Test transaction re-raises sqlite3.Error."""
    from unittest.mock import MagicMock

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = sqlite3.Error("Transaction failed")

    db._con = mock_conn

    with pytest.raises(sqlite3.Error):
        with db.transaction():
            pass


def test_transaction_rollback_called_on_error():
    """Test that rollback is called when sqlite3.Error occurs in transaction."""
    from unittest.mock import MagicMock

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = [None, sqlite3.Error("Error in transaction")]

    db._con = mock_conn

    try:
        with db.transaction():
            # This would trigger the second execute call which raises the error
            db.connection.execute("SELECT 1")
    except sqlite3.Error:
        pass

    mock_conn.rollback.assert_called_once()


def test_close_handles_rollback_error():
    """Test close() handles rollback errors gracefully."""
    from unittest.mock import MagicMock

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = True
    mock_conn.rollback.side_effect = sqlite3.Error("Rollback failed")
    mock_conn.close.return_value = None

    db._con = mock_conn

    db.close()

    assert db._con is None


def test_close_handles_commit_error():
    """Test close() handles commit errors gracefully."""
    from unittest.mock import MagicMock

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_conn.commit.side_effect = sqlite3.Error("Commit failed")
    mock_conn.close.return_value = None

    db._con = mock_conn

    db._is_in_memory = MagicMock(return_value=False)

    db.close()

    assert db._con is None


def test_close_handles_close_error():
    """Test close() handles connection.close() errors gracefully."""
    from unittest.mock import MagicMock

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_conn.close.side_effect = sqlite3.Error("Close failed")

    db._con = mock_conn

    # Mock _is_in_memory to return True (in-memory database)
    db._is_in_memory = MagicMock(return_value=True)

    # Should not raise, should log warning and continue
    db.close()

    # Connection should be nulled
    assert db._con is None


def test_optimize_with_active_transaction():
    """Test optimize() commits transaction before VACUUM."""
    db = SQLiteManager()
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO test (id) VALUES (1)")

    db.connection.execute("BEGIN")

    # Optimize should commit the transaction before VACUUM
    result = db.optimize()

    assert result is True


def test_optimize_error_handling():
    """Test optimize() handles errors gracefully."""
    from unittest.mock import MagicMock

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_conn.in_transaction = False
    mock_conn.execute.side_effect = sqlite3.Error("Optimize failed")

    db._con = mock_conn

    result = db.optimize()

    assert result is False


def test_last_insert_rowid_returns_zero_on_empty():
    """Test last_insert_rowid returns 0 when query returns empty."""
    from unittest.mock import MagicMock

    db = SQLiteManager()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cursor

    db._con = mock_conn

    # Mock query to return empty result
    db.query = MagicMock(return_value=[])

    result = db.last_insert_rowid()

    assert result == 0
