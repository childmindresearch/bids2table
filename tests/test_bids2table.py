import json
from pathlib import Path

import pytest

from bids2table import bids2table

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
    assert tab.shape == (128, 40)

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


if __name__ == "__main__":
    pytest.main([__file__])
