import json
from pathlib import Path

import pytest

from bids2table.extractors.sidecar import extract_sidecar, is_associated_sidecar


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

    # dummy inherited root sidecar
    # applies to all sessions
    with (ds_dir / "T1w.json").open("w") as f:
        json.dump({"C": True}, f)

    # dummy non-matching root sidecar
    with (ds_dir / "ses-2_T1w.json").open("w") as f:
        json.dump({"D": True}, f)

    # dummy local key-value data, not sidecar
    with (anat_dir / "sub-A01_ses-1_keyvalue.json").open("w") as f:
        json.dump({"E": True}, f)

    # Inherited from first json but not second
    expected_sidecar = {"A": True, "B": True, "C": True}
    return ds_dir, image_path, expected_sidecar


def test_extract_sidecar(bids_dataset):
    _, image_path, expected_sidecar = bids_dataset
    rec = extract_sidecar(image_path)
    assert rec["sidecar"] == expected_sidecar


@pytest.mark.parametrize(
    "path,expected",
    [
        ("sub-A01/ses-1/anat/sub-A01_ses-1_T1w.json", True),
        ("ses-1_T1w.json", True),
        ("sub-A01/ses-1/anat/sub-A01_ses-1_keyvalue.json", False),
    ],
)
def test_is_associated_sidecar(path, expected, bids_dataset):
    ds_dir, _, _ = bids_dataset
    path = ds_dir / path
    assert path.exists()
    assert is_associated_sidecar(path) == expected


if __name__ == "__main__":
    pytest.main([__file__])
