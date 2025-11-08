import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock


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

    mock_conn = MagicMock()
    mock_conn.create_collation.side_effect = sqlite3.Error("Failed to create collation")

    monkeypatch.setattr(db, "_con", mock_conn)

    result = db.add_collation("TEST_COLLATION", lambda x, y: 0)
    assert result is False, "Should return False when collation creation fails"

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
