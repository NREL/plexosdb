from typing import Any

import pytest

from plexosdb.db_manager import SQLiteManager


def test_insert_records_single_dict(db_manager_instance_empty_with_schema: SQLiteManager[Any]) -> None:
    db = db_manager_instance_empty_with_schema
    generator_record: dict[str, Any] = {
        "data_id": 1,
        "value": 500.0,
        "state": 1,
    }
    success = db.insert_records("t_data", generator_record)
    assert success is True

    properties = db.fetchall("SELECT data_id, value, state FROM t_data")
    assert len(properties) == 1
    assert properties[0][0] == 1
    assert properties[0][1] == 500.0
    assert properties[0][2] == 1


def test_insert_records_multiple_dicts(db_manager_instance_empty_with_schema: SQLiteManager[Any]) -> None:
    db = db_manager_instance_empty_with_schema
    generator_records = [
        {"data_id": 1, "value": 500.0, "state": 1},
        {"data_id": 2, "value": 300.0, "state": 1},
        {"data_id": 3, "value": 150.0, "state": 1},
    ]

    success = db.insert_records("t_data", generator_records)
    assert success is True

    properties = db.fetchall("SELECT * FROM t_data ORDER BY data_id")
    assert len(properties) == 3
    assert properties[0][0] == 1
    assert properties[1][0] == 2
    assert properties[2][0] == 3


def test_insert_records_empty_records(db_manager_instance_empty_with_schema):
    """Test that empty records raise ValueError."""
    db = db_manager_instance_empty_with_schema
    with pytest.raises(ValueError, match="Records cannot be empty"):
        db.insert_records("t_data", [])

    with pytest.raises(ValueError, match="Records cannot be empty"):
        db.insert_records("generators", {})


def test_insert_records_nonexistent_table(db_manager_instance_empty):
    """Test that inserting into a nonexistent table raises ValueError."""
    generator_record = {"name": "Solar Plant", "capacity": 200.0, "fuel_type": "Solar"}

    with pytest.raises(ValueError, match="Table 'nonexistent_table' does not exist"):
        db_manager_instance_empty.insert_records("nonexistent_table", generator_record)


def test_insert_records_inconsistent_keys(db_manager_instance_empty: SQLiteManager):
    """Test that records with inconsistent keys raise ValueError."""
    generator_records = [
        {"name": "Coal Plant 1", "capacity": 500.0, "fuel_type": "Coal"},
        {"name": "Gas Plant 1", "max_power": 300.0},
    ]

    with pytest.raises(KeyError, match="All records must have the same keys"):
        db_manager_instance_empty.insert_records("generators", generator_records)


def test_insert_records_empty_dict_in_list(db_manager_instance_empty: SQLiteManager):
    """Test that an empty dictionary in the list raises ValueError."""
    generator_records = [{"name": "Coal Plant 1", "capacity": 500.0, "fuel_type": "Coal"}, {}]

    with pytest.raises(KeyError, match="All records must have the same keys"):
        db_manager_instance_empty.insert_records("generators", generator_records)


def test_insert_records_duplicate_primary_key(db_manager_instance_empty: SQLiteManager):
    """Test that duplicate primary keys cause insertion to fail."""
    generator_record1 = {"id": 1, "name": "Coal Plant 1", "capacity": 500.0, "fuel_type": "Coal"}
    success = db_manager_instance_empty.insert_records("generators", generator_record1)
    assert success is True

    generator_record2 = {"id": 1, "name": "Gas Plant 1", "capacity": 300.0, "fuel_type": "Gas"}
    success = db_manager_instance_empty.insert_records("generators", generator_record2)
    assert success is False

    generators = db_manager_instance_empty.fetchall("SELECT * FROM generators")
    assert len(generators) == 1
    assert generators[0][1] == "Coal Plant 1"


def test_insert_records_partial_columns(db_manager_instance_empty: SQLiteManager):
    """Test inserting records with only some columns (others will be NULL)."""
    generator_record = {"name": "Solar Plant"}

    success = db_manager_instance_empty.insert_records("generators", generator_record)
    assert success is True

    generators = db_manager_instance_empty.fetchall("SELECT * FROM generators")
    assert len(generators) == 1
    assert generators[0][1] == "Solar Plant"
    assert generators[0][2] is None
    assert generators[0][3] is None


def test_insert_records_foreign_key_constraint(db_manager_instance_empty: SQLiteManager):
    """Test that foreign key constraints are respected."""
    property_record = {"generator_id": 999, "property_name": "Heat Rate", "value": 9500.0}

    success = db_manager_instance_empty.insert_records("properties", property_record)
    assert success is False

    properties = db_manager_instance_empty.fetchall("SELECT * FROM properties")
    assert len(properties) == 0


def test_insert_records_with_transaction(db_manager_instance_empty: SQLiteManager):
    """Test insert_records within a transaction context."""
    generator_records = [
        {"name": "Coal Plant 1", "capacity": 500.0, "fuel_type": "Coal"},
        {"name": "Gas Plant 1", "capacity": 300.0, "fuel_type": "Gas"},
    ]

    with db_manager_instance_empty.transaction():
        success = db_manager_instance_empty.insert_records("generators", generator_records)
        assert success is True

        generators = db_manager_instance_empty.fetchall("SELECT * FROM generators")
        assert len(generators) == 2

    generators = db_manager_instance_empty.fetchall("SELECT * FROM generators")
    assert len(generators) == 2


def test_insert_records_transaction_rollback_on_error(db_manager_instance_empty):
    """Test that transaction rolls back on insert_records error."""
    generator_records = [
        {"name": "Coal Plant 1", "capacity": 500.0, "fuel_type": "Coal"},
        {"id": 1, "name": "Gas Plant 1", "capacity": 300.0, "fuel_type": "Gas"},
        {"id": 1, "name": "Wind Farm 1", "capacity": 150.0, "fuel_type": "Wind"},
    ]

    try:
        with db_manager_instance_empty.transaction():
            db_manager_instance_empty.insert_records("generators", generator_records[:2])
            db_manager_instance_empty.insert_records("generators", [generator_records[2]])
    except Exception:
        pass

    generators = db_manager_instance_empty.fetchall("SELECT * FROM generators")
    assert len(generators) == 0


def test_insert_records_column_order_independence(db_manager_instance_empty: SQLiteManager[Any]):
    """Test that column order in dictionaries doesn't matter."""
    generator_record1 = {"name": "Coal Plant 1", "capacity": 500.0, "fuel_type": "Coal"}
    generator_record2 = {"fuel_type": "Gas", "name": "Gas Plant 1", "capacity": 300.0}

    success1 = db_manager_instance_empty.insert_records("generators", generator_record1)
    success2 = db_manager_instance_empty.insert_records("generators", generator_record2)

    assert success1 is True
    assert success2 is True

    generators = db_manager_instance_empty.fetchall("SELECT * FROM generators ORDER BY name")
    assert len(generators) == 2
    assert generators[0][1] == "Coal Plant 1"
    assert generators[0][2] == 500.0
    assert generators[1][1] == "Gas Plant 1"
    assert generators[1][2] == 300.0


def test_insert_records_all_data_types(db_manager_instance_empty: SQLiteManager[Any]):
    """Test inserting records with various data types."""
    db_manager_instance_empty.execute("""
        CREATE TABLE test_types (
            id INTEGER PRIMARY KEY,
            name TEXT,
            capacity REAL,
            is_renewable BOOLEAN,
            metadata BLOB
        )
    """)

    test_record = {
        "name": "Hybrid Plant",
        "capacity": 250.5,
        "is_renewable": True,
        "metadata": b"operational_data",
    }

    success = db_manager_instance_empty.insert_records("test_types", test_record)
    assert success is True

    records = db_manager_instance_empty.fetchall("SELECT * FROM test_types")
    assert len(records) == 1
    record = records[0]
    assert record[1] == "Hybrid Plant"
    assert record[2] == 250.5
    assert record[3] == 1
    assert record[4] == b"operational_data"
