"""Test utility functions for string validation and data preparation.

This module tests validation, conversion, normalization, and helper functions
in the utils module with various input types and edge cases.
"""

from __future__ import annotations

import pytest


def test_validate_string_with_none_input():
    """Verify validate_string returns None for None input."""
    from plexosdb.utils import validate_string

    result = validate_string(None)
    assert result is None


def test_validate_string_with_integer_string():
    """Verify validate_string converts integer string to int."""
    from plexosdb.utils import validate_string

    result = validate_string("42")
    assert isinstance(result, int)
    assert result == 42


def test_validate_string_with_negative_integer():
    """Verify validate_string converts negative integer string."""
    from plexosdb.utils import validate_string

    result = validate_string("-42")
    assert isinstance(result, int)
    assert result == -42


def test_validate_string_with_zero():
    """Verify validate_string converts zero string to int."""
    from plexosdb.utils import validate_string

    result = validate_string("0")
    assert isinstance(result, int)
    assert result == 0


def test_validate_string_with_float_string():
    """Verify validate_string converts float string to float."""
    from plexosdb.utils import validate_string

    result = validate_string("3.14")
    assert isinstance(result, float)
    assert result == 3.14


def test_validate_string_with_negative_float():
    """Verify validate_string converts negative float string."""
    from plexosdb.utils import validate_string

    result = validate_string("-3.14")
    assert isinstance(result, float)
    assert result == -3.14


def test_validate_string_with_scientific_notation():
    """Verify validate_string converts scientific notation to float."""
    from plexosdb.utils import validate_string

    result = validate_string("1e10")
    assert isinstance(result, float)


@pytest.mark.parametrize("true_str", ["true", "TRUE"])
def test_validate_string_with_true_variants(true_str):
    """Verify validate_string converts true variants to boolean."""
    from plexosdb.utils import validate_string

    result = validate_string(true_str)
    assert result is True


@pytest.mark.parametrize("false_str", ["false", "FALSE"])
def test_validate_string_with_false_variants(false_str):
    """Verify validate_string converts false variants to boolean."""
    from plexosdb.utils import validate_string

    result = validate_string(false_str)
    assert result is False


def test_validate_string_with_list_string():
    """Verify validate_string parses list string."""
    from plexosdb.utils import validate_string

    result = validate_string("[1, 2, 3]")
    assert isinstance(result, list)
    assert result == [1, 2, 3]


def test_validate_string_with_dict_string():
    """Verify validate_string parses dictionary string."""
    from plexosdb.utils import validate_string

    result = validate_string("{'key': 'value'}")
    assert isinstance(result, dict)


def test_validate_string_with_tuple_string():
    """Verify validate_string parses tuple string."""
    from plexosdb.utils import validate_string

    result = validate_string("(1, 2, 3)")
    assert isinstance(result, tuple)


def test_validate_string_with_unparseable_string():
    """Verify validate_string returns original unparseable string."""
    from plexosdb.utils import validate_string

    original = "not_a_number"
    result = validate_string(original)
    assert result == original


def test_validate_string_with_special_characters():
    """Verify validate_string handles special characters."""
    from plexosdb.utils import validate_string

    result = validate_string("test@string.com")
    assert isinstance(result, str)
    assert result == "test@string.com"


def test_normalize_names_with_single_string():
    """Verify normalize_names returns list with single string."""
    from plexosdb.utils import normalize_names

    result = normalize_names("test")
    assert result == ["test"]


def test_normalize_names_with_multiple_strings():
    """Verify normalize_names returns all provided strings."""
    from plexosdb.utils import normalize_names

    result = normalize_names("test1", "test2", "test3")
    assert set(result) == {"test1", "test2", "test3"}


def test_normalize_names_with_list_argument():
    """Verify normalize_names works with list argument."""
    from plexosdb.utils import normalize_names

    result = normalize_names(["test1", "test2", "test3"])
    assert set(result) == {"test1", "test2", "test3"}


def test_normalize_names_with_tuple_argument():
    """Verify normalize_names works with tuple argument."""
    from plexosdb.utils import normalize_names

    result = normalize_names(("test1", "test2"))
    assert set(result) == {"test1", "test2"}


def test_normalize_names_deduplicates_results():
    """Verify normalize_names removes duplicates."""
    from plexosdb.utils import normalize_names

    result = normalize_names("test", "test", "other", "other")
    assert len(result) == 2
    assert set(result) == {"test", "other"}


def test_normalize_names_filters_none_values():
    """Verify normalize_names filters out None values."""
    from plexosdb.utils import normalize_names

    result = normalize_names("test", None, "other", None)
    assert set(result) == {"test", "other"}
    assert None not in result


def test_normalize_names_converts_mixed_types():
    """Verify normalize_names converts all types to strings."""
    from plexosdb.utils import normalize_names

    result = normalize_names(1, 2, "test", 3.14)
    assert "1" in result
    assert "2" in result
    assert "test" in result
    assert "3.14" in result


def test_normalize_names_with_empty_list():
    """Verify normalize_names handles empty list."""
    from plexosdb.utils import normalize_names

    result = normalize_names([])
    assert result == []


def test_normalize_names_with_single_none():
    """Verify normalize_names with only None returns empty list."""
    from plexosdb.utils import normalize_names

    result = normalize_names(None)
    assert result == []


def test_no_space_collate_equal_strings():
    """Verify no_space returns 0 for equal strings without spaces."""
    from plexosdb.utils import no_space

    result = no_space("test string", "teststring")
    assert result == 0


def test_no_space_collate_equal_different_spaces():
    """Verify no_space returns 0 for different space placement."""
    from plexosdb.utils import no_space

    result = no_space("a b c", "abc")
    assert result == 0


def test_no_space_collate_less_than():
    """Verify no_space returns -1 for less than comparison."""
    from plexosdb.utils import no_space

    result = no_space("abc", "xyz")
    assert result == -1


def test_no_space_collate_greater_than():
    """Verify no_space returns 1 for greater than comparison."""
    from plexosdb.utils import no_space

    result = no_space("xyz", "abc")
    assert result == 1


def test_prepare_sql_data_params_single_record():
    """Verify prepare_sql_data_params with single record."""
    from plexosdb.utils import prepare_sql_data_params

    records = [{"name": "gen1", "capacity": 100}]
    memberships = [{"name": "gen1", "membership_id": 1}]
    property_mapping = [("capacity", 10)]

    result = prepare_sql_data_params(records, memberships, property_mapping)

    assert len(result) == 1
    assert result[0] == (1, 10, 100)


def test_prepare_sql_data_params_multiple_properties():
    """Verify prepare_sql_data_params with multiple properties."""
    from plexosdb.utils import prepare_sql_data_params

    records = [{"name": "gen1", "capacity": 100, "efficiency": 0.95}]
    memberships = [{"name": "gen1", "membership_id": 1}]
    property_mapping = [("capacity", 10), ("efficiency", 11)]

    result = prepare_sql_data_params(records, memberships, property_mapping)

    assert len(result) == 2
    assert (1, 10, 100) in result
    assert (1, 11, 0.95) in result


def test_prepare_sql_data_params_multiple_records():
    """Verify prepare_sql_data_params with multiple records."""
    from plexosdb.utils import prepare_sql_data_params

    records = [
        {"name": "gen1", "capacity": 100},
        {"name": "gen2", "capacity": 200},
    ]
    memberships = [
        {"name": "gen1", "membership_id": 1},
        {"name": "gen2", "membership_id": 2},
    ]
    property_mapping = [("capacity", 10)]

    result = prepare_sql_data_params(records, memberships, property_mapping)

    assert len(result) == 2


def test_prepare_sql_data_params_missing_membership_skipped():
    """Verify prepare_sql_data_params skips records with missing membership."""
    from plexosdb.utils import prepare_sql_data_params

    records = [{"name": "gen_unknown", "capacity": 100}]
    memberships = [{"name": "gen1", "membership_id": 1}]
    property_mapping = [("capacity", 10)]

    result = prepare_sql_data_params(records, memberships, property_mapping)

    assert len(result) == 0


def test_prepare_sql_data_params_missing_property_skipped():
    """Verify prepare_sql_data_params skips missing properties."""
    from plexosdb.utils import prepare_sql_data_params

    records = [{"name": "gen1", "unknown_prop": 100}]
    memberships = [{"name": "gen1", "membership_id": 1}]
    property_mapping = [("capacity", 10)]

    result = prepare_sql_data_params(records, memberships, property_mapping)

    assert len(result) == 0


def test_create_membership_record_single_object():
    """Verify create_membership_record with single object."""
    from plexosdb.utils import create_membership_record

    result = create_membership_record(
        object_ids=[1],
        child_object_class_id=2,
        parent_object_id=1,
        parent_object_class_id=1,
        collection_id=1,
    )

    assert len(result) == 1
    assert result[0]["child_object_id"] == 1
    assert result[0]["parent_object_id"] == 1
    assert result[0]["collection_id"] == 1


def test_create_membership_record_multiple_objects():
    """Verify create_membership_record with multiple objects."""
    from plexosdb.utils import create_membership_record

    result = create_membership_record(
        object_ids=[1, 2, 3],
        child_object_class_id=2,
        parent_object_id=1,
        parent_object_class_id=1,
        collection_id=1,
    )

    assert len(result) == 3
    assert result[0]["child_object_id"] == 1
    assert result[1]["child_object_id"] == 2
    assert result[2]["child_object_id"] == 3


def test_create_membership_record_empty_object_ids():
    """Verify create_membership_record with empty object_ids."""
    from plexosdb.utils import create_membership_record

    result = create_membership_record(
        object_ids=[],
        child_object_class_id=2,
        parent_object_id=1,
        parent_object_class_id=1,
        collection_id=1,
    )

    assert len(result) == 0


def test_create_membership_record_all_fields_present():
    """Verify create_membership_record includes all required fields."""
    from plexosdb.utils import create_membership_record

    result = create_membership_record(
        object_ids=[1],
        child_object_class_id=2,
        parent_object_id=10,
        parent_object_class_id=1,
        collection_id=5,
    )

    record = result[0]
    assert record["parent_class_id"] == 1
    assert record["parent_object_id"] == 10
    assert record["collection_id"] == 5
    assert record["child_class_id"] == 2
    assert record["child_object_id"] == 1


def test_batched_with_exact_divisions():
    """Verify batched returns correct batches with exact divisions."""
    from plexosdb.utils import batched

    data = list(range(10))
    result = [batch for batch in batched(data, 2)]

    assert len(result) == 5
    assert result[0] == (0, 1)
    assert result[1] == (2, 3)
    assert result[-1] == (8, 9)


def test_batched_with_remainder():
    """Verify batched handles remainder items."""
    from plexosdb.utils import batched

    data = list(range(11))
    result = [batch for batch in batched(data, 2)]

    assert len(result) == 6
    assert result[-1] == (10,)


def test_batched_with_batch_size_one():
    """Verify batched works with batch_size=1."""
    from plexosdb.utils import batched

    data = list(range(3))
    result = [batch for batch in batched(data, 1)]

    assert len(result) == 3
    assert all(len(batch) == 1 for batch in result)


def test_batched_with_batch_size_larger_than_data():
    """Verify batched handles batch size larger than data."""
    from plexosdb.utils import batched

    data = list(range(3))
    result = [batch for batch in batched(data, 10)]

    assert len(result) == 1
    assert result[0] == (0, 1, 2)


def test_batched_with_empty_list():
    """Verify batched returns empty iterator for empty list."""
    from plexosdb.utils import batched

    data = []
    result = [batch for batch in batched(data, 2)]

    assert len(result) == 0


def test_batched_with_generator():
    """Verify batched works with generator input."""
    from plexosdb.utils import batched

    def gen():
        yield from range(8)

    result = [batch for batch in batched(gen(), 3)]

    assert len(result) == 3
    assert result[0] == (0, 1, 2)
    assert result[1] == (3, 4, 5)
    assert result[2] == (6, 7)


def test_get_sql_query_returns_string():
    """Verify get_sql_query returns string content."""
    from plexosdb.utils import get_sql_query

    result = get_sql_query("object_query.sql")
    assert isinstance(result, str)
    assert len(result) > 0


def test_get_sql_query_property_query():
    """Verify get_sql_query returns property query."""
    from plexosdb.utils import get_sql_query

    result = get_sql_query("property_query.sql")
    assert isinstance(result, str)
    assert len(result) > 0
