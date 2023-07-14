import json
from pathlib import Path

import pytest

from bids2table.extractors.dataset import extract_dataset_meta


@pytest.fixture
def bids_dataset(tmp_path: Path) -> Path:
    ds_dir = tmp_path / "dummy_dataset"
    ds_dir.mkdir()
    description = {
        "Name": "Dummy dataset",
        "BIDSVersion": "1.8.0",
        "DatasetType": "raw",
        "License": "PD",
        "Authors": [],
    }
    with (ds_dir / "dataset_description.json").open("w") as f:
        json.dump(description, f)
    return ds_dir


def test_extract_dataset_meta(bids_dataset: Path):
    dataset_meta = extract_dataset_meta(bids_dataset)
    assert dataset_meta["dataset"] == "dummy_dataset"
    assert dataset_meta["dataset_path"] == str(bids_dataset)
    assert dataset_meta["dataset_description"]["Name"] == "Dummy dataset"


if __name__ == "__main__":
    pytest.main([__file__])
