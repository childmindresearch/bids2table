import pytest

from bids2table.extractors.metadata import extract_metadata, is_associated_sidecar


def test_extract_metadata(bids_dataset):
    _, image_path, expected_json = bids_dataset
    rec = extract_metadata(image_path)
    assert rec["json"] == expected_json


@pytest.mark.parametrize(
    "path,expected",
    [
        ("sub-A01/ses-1/anat/sub-A01_ses-1_T1w.json", True),
        ("ses-1_T1w.json", True),
        ("sub-A01/ses-1/anat/sub-A01_ses-1_keyvalue.json", False),
        ("sub-A01/ses-1/anat/sub-A01_ses-1_white.surf.json", False),
    ],
)
def test_is_associated_sidecar(path, expected, bids_dataset):
    ds_dir, _, _ = bids_dataset
    path = ds_dir / path
    assert path.exists()
    assert is_associated_sidecar(path) == expected


if __name__ == "__main__":
    pytest.main([__file__])
