from pathlib import Path
from typing import Any, Dict

import pytest

from bids2table import bids2table
from bids2table.table import BIDSTable

BIDS_EXAMPLES = Path(__file__).parent.parent / "bids-examples"


@pytest.fixture(scope="module")
def tab() -> BIDSTable:
    return bids2table(BIDS_EXAMPLES / "ds001")


def test_table(tab: BIDSTable):
    assert tab.shape == (128, 40)

    groups = tab.nested.columns.unique(0).tolist()
    assert groups == ["ds", "ent", "meta", "file"]

    assert tab.dataset.shape == (128, 4)
    assert tab.entities.shape == (128, 32)
    assert tab.metadata.shape == (128, 1)
    assert tab.flat_metadata.shape == (128, 2)
    assert tab.file.shape == (128, 3)

    subtab: BIDSTable = tab.iloc[:10]
    assert subtab.dataset.shape == (10, 4)


@pytest.mark.parametrize(
    "key,filter,expected_count",
    [
        ("sub", {"value": "04"}, 8),
        ("subject", {"value": "04"}, 8),
        ("RepetitionTime", {"value": 2.0}, 48),
        ("subject", {"value": "04"}, 8),
        ("sub", {"items": ["04", "06"]}, 16),
        ("sub", {"like": "4"}, 16),
        ("sub", {"regex": "0[456]"}, 24),
    ],
)
def test_table_filter(
    tab: BIDSTable, key: str, filter: Dict[str, Any], expected_count: int
):
    subtab = tab.filter(key, **filter)
    assert isinstance(subtab, BIDSTable)
    assert len(subtab) == expected_count


if __name__ == "__main__":
    pytest.main([__file__])
