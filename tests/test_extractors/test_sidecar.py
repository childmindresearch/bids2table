import json
from pathlib import Path

import pytest

from bids2table.extractors.sidecar import extract_sidecar


@pytest.fixture
def bids_dataset(tmp_path: Path):
    ds_dir = tmp_path / "dummy_bids"
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

    anat_dir = ds_dir / "sub-A01" / "ses-1" / "anat"
    anat_dir.mkdir(parents=True)

    # dummy image
    image_path = anat_dir / "sub-A01_ses-1_T1w.nii.gz"
    image_path.touch()

    # dummy local sidecar
    with (anat_dir / "sub-A01_ses-1_T1w.json").open("w") as f:
        json.dump({"A": True}, f)

    # dummy inherited root sidecar
    with (ds_dir / "ses-1_T1w.json").open("w") as f:
        json.dump({"B": True}, f)

    # dummy non-matching root sidecar
    with (ds_dir / "ses-2_T1w.json").open("w") as f:
        json.dump({"C": True}, f)

    # Inherited from first json but not second
    expected_sidecar = {"A": True, "B": True}
    return ds_dir, image_path, expected_sidecar


def test_extract_sidecar(bids_dataset):
    _, image_path, expected_sidecar = bids_dataset
    rec = extract_sidecar(image_path)
    assert rec["sidecar"] == expected_sidecar


if __name__ == "__main__":
    pytest.main([__file__])
