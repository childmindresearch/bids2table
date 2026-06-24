"""Pytest configuration and shared fixtures."""

from pathlib import Path

import pytest
from bids2table._pathlib import cloudpathlib_is_available


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip cloud-marked tests when cloudpathlib is unavailable."""
    if "cloud" in item.keywords and not cloudpathlib_is_available():
        pytest.skip("cloudpathlib is not available or not fully functional")


@pytest.fixture
def symlink_dataset(tmp_path: Path) -> Path:
    """Create a temporary BIDS dataset with symlinked files and sub-datasets.

    Three datasets share the same underlying files via symlinks:

    - ``dataset/bids`` — raw dataset containing DWI and symlinked anatomical files
    - ``dataset2/bids`` — symlinks to the subject directory of dataset 1
    - ``dataset3/bids`` — symlinks to the entire dataset 1 bids root

    Returns:
        The parent directory containing the three dataset trees.
    """
    data = tmp_path / "data"
    data.mkdir()

    sub = "sub-001"
    ses = "ses-001"

    # Directory paths
    ds = data / "dataset" / "bids" / sub / ses
    ds_desc = data / "dataset" / "bids" / "dataset_description.json"
    ds_dwi = ds / "dwi"
    d1_anat = data / "directory1" / "bids" / sub / ses / "anat"
    d2 = data / "directory2"

    # Create directories
    ds_dwi.mkdir(parents=True)
    d1_anat.mkdir(parents=True)
    d2.mkdir(parents=True)

    # Create files to be symlinked
    fnames = ["T1w.json", "T1w.nii.gz", "mask.nii.gz"]
    for fname in fnames:
        (d2 / fname).touch()

    # dataset1
    for suffix in [".bval", ".bvec", ".json", ".nii.gz"]:
        (ds_dwi / f"{sub}_{ses}_run-1_dwi{suffix}").touch()
    for fpath, suffix in zip(
        [d2 / fname for fname in fnames],
        ["_T1w.json", "_T1w.nii.gz", "_desc-T1w_mask.nii.gz"],
        strict=True,
    ):
        (d1_anat / f"{sub}_{ses}_run-1{suffix}").symlink_to(fpath)
    (ds / "anat").symlink_to(d1_anat, target_is_directory=True)
    ds_desc.write_text('{"Name": "Dataset 1"}')

    # dataset2
    ds2 = data / "dataset2" / "bids"
    ds2.mkdir(parents=True)
    (ds2 / "dataset_description.json").write_text('{"Name": "Dataset 2"}')
    (ds2 / sub).symlink_to(ds.parent, target_is_directory=True)

    # dataset3
    ds3 = data / "dataset3"
    ds3.mkdir(parents=True)
    (ds3 / "bids").symlink_to(data / "dataset" / "bids", target_is_directory=True)

    return data
