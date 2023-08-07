from pathlib import Path
from typing import Any, Dict, List, Union

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
    assert groups == ["ds", "ent", "meta", "finfo"]

    assert tab.ds.shape == (128, 4)
    assert tab.ent.shape == (128, 32)
    assert tab.meta.shape == (128, 1)
    assert tab.flat_metadata.shape == (128, 2)
    assert tab.finfo.shape == (128, 3)

    subtab: BIDSTable = tab.iloc[:10]
    assert subtab.ds.shape == (10, 4)

    assert len(tab.datatypes) == 2
    assert len(tab.modalities) == 1
    assert len(tab.subjects) == 16
    assert len(tab.entities) == 3


def test_table_files(tab: BIDSTable):
    files = tab.files
    assert len(files) == 128

    file = files[0]
    assert file.path.exists()
    assert (file.root / file.relative_path).exists()
    assert file.metadata == {}


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


@pytest.mark.parametrize("inplace", [True, False])
@pytest.mark.parametrize("by", ["sub", ["subject"], ["dataset", "sub"]])
def test_table_sort_entities(tab: BIDSTable, by: Union[str, List[str]], inplace: bool):
    tab = tab.copy()
    sort_tab = tab.sort_entities(by, inplace=inplace)
    assert isinstance(sort_tab, BIDSTable)
    assert len(sort_tab) == len(tab)
    assert sort_tab.subjects == sorted(tab.subjects)


if __name__ == "__main__":
    pytest.main([__file__])
