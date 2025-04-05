import pytest

from plexosdb.utils import batched, get_sql_query


@pytest.mark.parametrize(
    "fname",
    ["object_query.sql", "property.sql", "property_query.sql", "simple_object_query.sql"],
)
def test_get_default_queries(fname):
    query = get_sql_query(fname)
    assert isinstance(query, str)


def test_batched():
    test_list = list(range(10))

    for element in batched(test_list, 2):
        assert len(element) == 2
