"""Tests for Query class."""

import pytest

pytest.importorskip("pandas", reason="pandas not available")

from bids2table.pybids import Query


def test_query_sentinels_are_unique():
    """Test that Query sentinel values are distinct objects."""
    assert Query.OPTIONAL is not Query.NONE
    assert Query.OPTIONAL is not Query.ANY
    assert Query.NONE is not Query.ANY


def test_query_sentinels_are_singletons():
    """Test that Query sentinels are the same object across imports."""
    from bids2table.pybids import Query as Query2

    assert Query.OPTIONAL is Query2.OPTIONAL
    assert Query.NONE is Query2.NONE
    assert Query.ANY is Query2.ANY


@pytest.mark.parametrize(("query"), [Query.OPTIONAL, Query.NONE, Query.ANY])
def test_query_repr(query: Query):
    """Test Query string representation."""
    assert repr(query) == "Query"
