""" Unit tests for the _bids2table module. """
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access
from typing import Generator

import pandas as pd
import pytest

from bids2table import _bids2table, exceptions


@pytest.fixture
def test_df() -> Generator[pd.DataFrame, None, None]:
    """
    Fixture that yields a pandas DataFrame with test data for unit tests.
    The DataFrame has columns 'subject', 'session', 'task', 'run', and 'value'.
    """
    yield pd.DataFrame(
        {
            "subject": ["sub-01", "sub-02", "sub-03", "sub-03"],
            "session": ["ses-01", "ses-01", "ses-01", "ses-02"],
        }
    )


def test_filter_one_value(test_df: pd.DataFrame) -> None:
    """Test filtering by a single value."""
    filters = {"subject": "sub-01"}
    expected = pd.DataFrame(
        {
            "subject": ["sub-01"],
            "session": ["ses-01"],
        }
    )

    actual = _bids2table._filter(test_df, filters)
    actual.reset_index(drop=True, inplace=True)

    assert actual.equals(expected)


def test_filter_list_values(test_df: pd.DataFrame) -> None:
    """Test filtering by a list of values."""
    filters = {"subject": ["sub-01", "sub-02"]}
    expected = pd.DataFrame(
        {
            "subject": ["sub-01", "sub-02"],
            "session": ["ses-01", "ses-01"],
        }
    )

    actual = _bids2table._filter(test_df, filters)
    actual.reset_index(drop=True, inplace=True)

    assert actual.equals(expected)


def test_filter_multiple_values(test_df: pd.DataFrame) -> None:
    """Test filtering by multiple values."""
    filters = {"subject": "sub-03", "session": "ses-01"}
    expected = pd.DataFrame(
        {
            "subject": ["sub-03"],
            "session": ["ses-01"],
        }
    )

    actual = _bids2table._filter(test_df, filters)
    actual.reset_index(drop=True, inplace=True)

    assert actual.equals(expected)


def test_filter_no_values(test_df: pd.DataFrame) -> None:
    """Test filtering with no filters."""
    actual = _bids2table._filter(test_df, None)
    actual.reset_index(drop=True, inplace=True)
    
    assert actual.equals(test_df)


def test_filter_invalid_key(test_df: pd.DataFrame) -> None:
    """Test filtering with an invalid key."""
    filters = {"invalid_key": "sub-01"}

    with pytest.raises(exceptions.InvalidFilterError):
        _bids2table._filter(test_df, filters)
