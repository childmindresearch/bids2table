from pathlib import Path

import pytest

from bids2table import bids2table
from bids2table.table import BIDSTable

BIDS_EXAMPLES = Path(__file__).parent.parent / "bids-examples"


def test_table():
    tab = bids2table(BIDS_EXAMPLES / "ds001")
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


if __name__ == "__main__":
    pytest.main([__file__])
