import json
from pathlib import Path

import pytest


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

    # dummy surface paths
    (anat_dir / "sub-A01_ses-1_white.surf.gii").touch()
    (anat_dir / "sub-A01_ses-1_white.surf.json").touch()

    # dummy directories that should be treated as data files
    # (I don't like this pattern. IMO these directories should be tar files or sth.)
    ome_zarr_dir = (
        ds_dir / "sub-A01" / "ses-1" / "micr" / "sub-A01_ses-1_sample-A_SPIM.ome.zarr"
    )
    ome_zarr_dir.mkdir(parents=True)

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

    # second local key-value data, not sidecar
    (anat_dir / "sub-A01_ses-2_keyvalue.json").touch()

    # extra json file
    (anat_dir / "sub-A01_ses-2_keyvalue.json").touch()

    # Inherited from first json but not second
    expected_json = {"A": True, "B": True, "C": True}
    return ds_dir, image_path, expected_json
