from pathlib import Path

import pytest

import bids2table._indexing as indexing

BIDS_EXAMPLES = Path(__file__).parents[1] / "bids-examples"


def test_get_arrow_schema():
    schema = indexing.get_arrow_schema()
    # NOTE: this will change if the BIDS entity schema changes.
    assert len(schema) == 38


def test_find_bids_datasets():
    datasets = sorted(indexing.find_bids_datasets(BIDS_EXAMPLES))
    expected_datasets = sorted(
        [p.parent for p in BIDS_EXAMPLES.rglob("dataset_description.json")]
    )
    # find_bids_datasets finds a few extra derivative datasets that are missing a
    # dataset_description.json.
    assert set(expected_datasets).issubset(datasets)
    assert len(datasets) == len(expected_datasets) + 6

    datasets_no_derivatives = sorted(
        indexing.find_bids_datasets(BIDS_EXAMPLES, exclude="derivatives")
    )
    expected_datasets_no_derivatives = sorted(
        [p.parent for p in BIDS_EXAMPLES.glob("*/dataset_description.json")]
    )
    assert datasets_no_derivatives == expected_datasets_no_derivatives


@pytest.mark.parametrize(
    "root,expected_count",
    [
        ("ds102", 130),
        ("synthetic/derivatives/fmriprep", 150),
        # Special case including '*_meg.ds' directories, '*_coordsystem.json', '*_scans.tsv'
        ("ds000246", 14),
    ],
)
def test_index_bids_dataset(root: str, expected_count: int):
    table = indexing.index_bids_dataset(BIDS_EXAMPLES / root, show_progress=False)
    assert len(table) == expected_count


@pytest.mark.parametrize(
    "path,expected_name",
    [
        ("ds102/sub-03", "ds102"),
        ("synthetic/derivatives/fmriprep/sub-02", "synthetic/derivatives/fmriprep"),
    ],
)
def test_get_bids_dataset(path: str, expected_name: str):
    name, dataset_path = indexing._get_bids_dataset(BIDS_EXAMPLES / path)
    assert name == expected_name
    assert indexing._contains_bids_subject_dirs(dataset_path)


@pytest.mark.parametrize(
    "path,expected_count",
    [
        ("ds102/sub-03", 5),
        ("synthetic/derivatives/fmriprep/sub-02", 30),
        ("eeg_face13/sub-010", 5),
    ],
)
def test_index_subject_dir(path: str, expected_count: int):
    _, table = indexing._index_bids_subject_dir(BIDS_EXAMPLES / path)
    assert len(table) == expected_count


@pytest.mark.parametrize(
    "path,expected",
    [
        (
            # Basic case.
            "ds102/sub-01/func/sub-01_task-flankertask_run-01_bold.nii.gz",
            True,
        ),
        (
            # JSON sidecar.
            "ds102/sub-01/func/sub-01_task-flankertask_run-01_bold.json",
            False,
        ),
        (
            # Special case, JSON data file. Matches list of exception suffixes.
            "eeg_face13/sub-010/eeg/sub-010_coordsystem.json",
            True,
        ),
        (
            # Special case of directory that is a bids "file".
            "ds000247/sub-0007/ses-0001/meg/sub-0007_ses-0001_task-rest_run-01_meg.ds/",
            True,
        ),
        (
            # Child files should not get matched, even though they look like BIDS files.
            "ds000247/sub-0007/ses-0001/meg/sub-0007_ses-0001_task-rest_run-01_meg.ds/sub-0007_ses-0001_task-rest_run-01_meg.acq",
            False,
        ),
    ],
)
def test_is_bids_file(path: str, expected: bool):
    assert indexing._is_bids_file(Path(path)) == expected


def test_filter_include_exclude():
    names = ["blah", "sub-A01", "sub-A02", "sub-B01", "sub-B02"]
    include = "sub-*"
    exclude = ["sub-B*", "sub-A02"]
    expected = {"sub-A01"}
    filtered_names = indexing._filter_include_exclude(names, include, exclude)
    assert filtered_names == expected
