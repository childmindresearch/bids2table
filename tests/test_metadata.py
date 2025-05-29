from pathlib import Path

import pytest

from bids2table._metadata import load_bids_metadata
from bids2table._pathlib import cloudpathlib_is_available

BIDS_EXAMPLES = Path(__file__).parents[1] / "bids-examples"


@pytest.mark.parametrize("inherit", [True, False])
def test_load_bids_metadata(inherit: bool):
    path = (
        BIDS_EXAMPLES
        / "synthetic/derivatives/fmriprep/sub-01/ses-01/func"
        / "sub-01_ses-01_task-rest_space-T1w_desc-preproc_bold.nii"
    )
    metadata = load_bids_metadata(path, inherit=inherit)
    expected_metadata = {
        "TaskName": "Rest",
        "RepetitionTime": 2.5,
        "Sources": ["bids:raw:sub-01/ses-01/sub-01_ses-01_task-rest_bold.nii"],
    }
    assert metadata == expected_metadata


@pytest.mark.skipif(
    not cloudpathlib_is_available(), reason="cloudpathlib not installed"
)
def test_load_bids_metadata_s3():
    path = (
        "s3://openneuro.org/ds000102/sub-01/func/sub-01_task-flanker_run-1_bold.nii.gz"
    )
    metadata = load_bids_metadata(path)
    assert metadata["RepetitionTime"] == 2.0
    assert metadata["TaskName"] == "Flanker"
