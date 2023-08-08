from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd
import pytest

from bids2table import bids2table
from bids2table.table import BIDSTable, flat_to_multi_columns, multi_to_flat_columns

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
    assert tab.flat_meta.shape == (128, 2)
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
        ("sub", {"contains": "4"}, 16),
        ("sub", {"regex": "0[456]"}, 24),
    ],
)
def test_table_filter(
    tab: BIDSTable, key: str, filter: Dict[str, Any], expected_count: int
):
    subtab = tab.filter(key, **filter)
    assert isinstance(subtab, BIDSTable)
    assert len(subtab) == expected_count


@pytest.mark.parametrize(
    "filters,expected_count",
    [
        (
            {
                "dataset": "ds001",
                "sub": {"items": ["04", "06"]},
                "RepetitionTime": {"value": 2.0},
            },
            6,
        )
    ],
)
def test_table_filter_multi(
    tab: BIDSTable, filters: Dict[str, Any], expected_count: int
):
    subtab = tab.filter_multi(**filters)
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


@pytest.mark.parametrize("sep", ["__", "."])
def test_flat_to_multi_columns(sep: str):
    df = pd.DataFrame(
        {
            f"A{sep}a": [1, 2, 3],
            f"A{sep}b": ["a", "b", "c"],
            f"B{sep}a": [4, 5, 6],
            f"B{sep}b": ["d", "e", "f"],
        }
    )
    multi_index = pd.MultiIndex.from_product([["A", "B"], ["a", "b"]])

    df_multi = flat_to_multi_columns(df, sep=sep)
    assert df_multi.columns.equals(multi_index)

    df_flat = multi_to_flat_columns(df_multi, sep=sep)
    assert df_flat.equals(df)


if __name__ == "__main__":
    pytest.main([__file__])
