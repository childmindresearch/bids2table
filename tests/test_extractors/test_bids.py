import pytest

from bids2table.extractors.bids import is_bids_file


@pytest.mark.parametrize(
    "path,expected",
    [
        ("sub-A01/ses-1/anat/sub-A01_ses-1_T1w.nii.gz", True),
        ("sub-A01/ses-1/anat/sub-A01_ses-1_T1w.json", False),
        ("sub-A01/ses-1/anat/sub-A01_ses-1_keyvalue.json", True),
        ("sub-A01/ses-1/anat/sub-A01_ses-1_white.surf.json", True),
        ("sub-A01/ses-1/micr/sub-A01_ses-1_SPIM.ome.zarr", True),
        (
            "sub-A01/ses-1/micr/sub-A01_ses-1_SPIM.ome.zarr/sub-A01_ses-1_SPIM.txt",
            False,
        ),
        ("sub-A01/sub-A01_scans.tsv", False),
    ],
)
def test_is_bids_file(path, expected, bids_dataset):
    ds_dir, _, _ = bids_dataset
    path = ds_dir / path
    assert is_bids_file(path) == expected
