import json
from pathlib import Path

import pytest

from bids2table import bids2table
from bids2table.entities import ENTITY_NAMES_TO_KEYS

BIDS_EXAMPLES = Path(__file__).parent.parent / "bids-examples"


@pytest.fixture
def empty_dataset(tmp_path: Path) -> Path:
    root = tmp_path / "empty_ds"
    root.mkdir()

    description = {
        "Name": "Empty dataset",
        "BIDSVersion": "1.8.0",
        "DatasetType": "raw",
        "License": "PD",
        "Authors": [],
    }
    with (root / "dataset_description.json").open("w") as f:
        json.dump(description, f)
    return root


@pytest.mark.parametrize(
    "persistent,with_meta", [(False, True), (True, True), (False, False)]
)
def test_bids2table(tmp_path: Path, persistent: bool, with_meta: bool):
    root = BIDS_EXAMPLES / "ds001"
    index_path = tmp_path / "index.b2t"

    tab = bids2table(
        root=root, with_meta=with_meta, persistent=persistent, index_path=index_path
    )
    assert tab.shape == (128, len(ENTITY_NAMES_TO_KEYS) + 8)

    if not with_meta:
        assert tab.loc[0, "meta__json"] is None

    # Reload from cache
    tab2 = bids2table(
        root=root, with_meta=with_meta, persistent=persistent, index_path=index_path
    )
    assert tab.equals(tab2)


def test_bids2table_empty(empty_dataset: Path):
    tab = bids2table(root=empty_dataset, persistent=True)
    assert tab.shape == (0, 0)

    # Reload from cache
    tab2 = bids2table(root=empty_dataset)
    assert tab.equals(tab2)


def test_bids2table_nonexist(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        bids2table(root=tmp_path / "nonexistent_dataset")


def test_bids2table_exclude(tmp_path: Path):
    root = BIDS_EXAMPLES / "ds001"
    index_path = tmp_path / "index_exclude.b2t"
    exclude_list = ["anat"]

    tab = bids2table(
        root=root,
        with_meta=True,
        persistent=True,
        index_path=index_path,
        exclude=exclude_list,
    )

    # Check that the excluded strings are not in the indexed table
    assert "ent__datatype" in tab.columns
    assert "anat" not in tab["ent__datatype"].values


if __name__ == "__main__":
    pytest.main([__file__])
