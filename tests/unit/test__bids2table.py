""" Unit tests for the _bids2table module. """
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access
import pandas as pd
import pytest

from bids2table import _bids2table, exceptions


@pytest.fixture
def test_df() -> pd.DataFrame:
    """Returns a test DataFrame with entities and dataset columns."""
    entities = pd.DataFrame(
        {
            "subject": ["sub-01", "sub-02", "sub-03", "sub-03"],
            "session": ["ses-01", "ses-01", "ses-01", "ses-02"],
        }
    )

    dataset = pd.DataFrame(
        {
            "dataset": ["ds-01", "ds-01", "ds-01", "ds-01"],
        }
    )

    return pd.concat(
        [entities, dataset],
        axis=1,
        keys=["entities", "dataset"],
    )


def test_filter_one_value(test_df: pd.DataFrame) -> None:
    """Test filtering by a single value."""
    filters = {"subject": "sub-01"}
    expected = test_df.iloc[[0]]

    actual = _bids2table._filter(test_df, filters)

    assert actual.equals(expected)


def test_filter_list_values(test_df: pd.DataFrame) -> None:
    """Test filtering by a list of values."""
    filters = {"subject": ["sub-01", "sub-02"]}
    expected = test_df.iloc[[0, 1]]

    actual = _bids2table._filter(test_df, filters)

    assert actual.equals(expected)


def test_filter_multiple_values(test_df: pd.DataFrame) -> None:
    """Test filtering by multiple values."""
    filters = {"subject": "sub-03", "session": "ses-01"}
    expected = test_df.iloc[[2]]

    actual = _bids2table._filter(test_df, filters)

    assert actual.equals(expected)


def test_filter_no_values(test_df: pd.DataFrame) -> None:
    """Test filtering with no filters."""
    actual = _bids2table._filter(test_df, None)

    assert actual.equals(test_df)


def test_filter_invalid_key(test_df: pd.DataFrame) -> None:
    """Test filtering with an invalid key."""
    filters = {"invalid_key": "sub-01"}

    with pytest.raises(exceptions.InvalidFilterError):
        _bids2table._filter(test_df, filters)
