"""Tests for BIDS dataset indexing and related internals."""

import logging
from copy import deepcopy
from itertools import islice
from pathlib import Path

import bids2table._indexing as indexing
import bidsschematools.schema
import pyarrow as pa
import pytest

BIDS_EXAMPLES = Path(__file__).parents[1] / "bids-examples"


def test_get_arrow_schema():
    """The arrow schema contains the expected number of fields."""
    schema = indexing.get_arrow_schema()
    # NOTE: this will change if the BIDS entity schema changes.
    assert len(schema) == 42


def test_get_column_names():
    """Column names match the arrow schema and resolve correctly."""
    schema = indexing.get_arrow_schema()
    bids_column = indexing.get_column_names()
    assert len(bids_column) == len(schema)
    assert bids_column.dataset == "dataset"


def test_find_bids_datasets():
    """Find all BIDS datasets under the bids-examples directory."""
    datasets = sorted(
        indexing.find_bids_datasets(
            BIDS_EXAMPLES,
            exclude=["surfaces", "subjects", "code", "sourcedata"],
        )
    )
    expected_datasets = sorted(
        [p.parent for p in BIDS_EXAMPLES.rglob("dataset_description.json")]
    )
    # find_bids_datasets finds a few extra derivative datasets that are missing a
    # dataset_description.json.
    assert set(expected_datasets).issubset(datasets)
    assert len(datasets) == len(expected_datasets) + 3

    datasets_no_derivatives = sorted(
        indexing.find_bids_datasets(
            BIDS_EXAMPLES,
            exclude=["derivatives", "code", "sourcedata"],
        )
    )
    expected_datasets_no_derivatives = sorted(
        [p.parent for p in BIDS_EXAMPLES.glob("*/dataset_description.json")]
    )
    assert datasets_no_derivatives == expected_datasets_no_derivatives


@pytest.mark.cloud
def test_find_bids_datasets_s3():
    """Discover BIDS datasets on OpenNeuro S3 bucket."""
    root = "s3://openneuro.org"
    datasets = list(islice(indexing.find_bids_datasets(root, maxdepth=2), 10))
    names = sorted([ds.name for ds in datasets])
    expected_names = [
        "ds000001", "ds000002", "ds000003", "ds000005", "ds000006",
        "ds000007", "ds000008", "ds000009", "ds000011", "ds000017",
    ]  # fmt: skip
    assert names == expected_names


@pytest.mark.parametrize(
    ("root", "expected_count"),
    [
        ("ds102", 130),
        ("synthetic/derivatives/fmriprep", 150),
        # Special cases including '*_meg.ds', '*_coordsystem.json', '*_scans.tsv'
        ("ds000246", 14),
    ],
)
def test_index_dataset(root: str, expected_count: int):
    """Index a local BIDS dataset and assert the expected row count."""
    table = indexing.index_dataset(BIDS_EXAMPLES / root)
    assert len(table) == expected_count


@pytest.mark.cloud
def test_index_dataset_s3():
    """Index a BIDS dataset stored on S3."""
    root = "s3://openneuro.org/ds000102"
    expected_count = 130
    table = indexing.index_dataset(root)
    assert len(table) == expected_count


def test_index_dataset_parallel():
    """Index a dataset and verify row count (parallel path exercised by test runner)."""
    root, expected_count = "ds102", 130
    table = indexing.index_dataset(BIDS_EXAMPLES / root)
    assert len(table) == expected_count


@pytest.mark.parametrize(
    ("path", "msg"),
    [
        # Not a bids dataset.
        ("tools", "not a valid BIDS"),
        # Has dataset_description.json but no valid subject dirs.
        ("ieeg_epilepsy/derivatives/brainvisa", "no matching subject"),
    ],
)
def test_index_dataset_warns(path: str, msg: str, caplog: pytest.LogCaptureFixture):
    """Non-BIDS paths trigger a warning and return an empty table."""
    with caplog.at_level(logging.WARNING):
        tab = indexing.index_dataset(BIDS_EXAMPLES / path)
    assert len(tab) == 0
    assert msg in caplog.text


@pytest.mark.parametrize("max_workers", [0, 2])
def test_batch_index_dataset(max_workers: int):
    """Batch-index multiple datasets with/without parallel workers."""
    datasets = list(BIDS_EXAMPLES.glob("*"))
    tables = indexing.batch_index_dataset(
        datasets, max_workers=max_workers, show_progress=False
    )
    table = pa.concat_tables(tables)
    assert len(table) == 9727


@pytest.mark.parametrize("ds_name", ["dataset", "dataset2", "dataset3"])
def test_indexing_on_symlinks(symlink_dataset: Path, ds_name: str):
    """Follow symlinks when indexing BIDS datasets."""
    tables = indexing.batch_index_dataset(
        indexing.find_bids_datasets(symlink_dataset / ds_name), show_progress=False
    )
    table = pa.concat_tables(tables)
    assert len(table) == 5


@pytest.mark.parametrize(
    ("path", "expected_name"),
    [
        ("ds102/sub-03", "ds102"),
        ("synthetic/derivatives/fmriprep/sub-02", "synthetic/derivatives/fmriprep"),
    ],
)
def test_get_bids_dataset(path: str, expected_name: str):
    """Resolve the BIDS dataset name and root for nested/flat paths."""
    name, dataset_path = indexing._get_bids_dataset(BIDS_EXAMPLES / path)
    assert name == expected_name
    assert dataset_path is not None
    assert indexing._contains_bids_subject_dirs(dataset_path)


@pytest.mark.parametrize(
    ("path", "include_subjects", "expected_count"),
    [
        ("ds102", None, 26),
        ("ds102", "sub-07", 1),
        ("ds102", "sub-0*", 9),
        ("ds102", ["sub-01", "sub-02", "sub-05"], 3),
    ],
)
def test_find_bids_subject_dirs(
    path: str, include_subjects: str | list[str] | None, expected_count: int
):
    """Find subject directories with optional inclusion filters."""
    subject_dirs = indexing._find_bids_subject_dirs(
        BIDS_EXAMPLES / path, include_subjects
    )
    assert len(subject_dirs) == expected_count


@pytest.mark.parametrize(
    ("path", "expected_count"),
    [
        ("ds102/sub-03", 5),
        ("synthetic/derivatives/fmriprep/sub-02", 30),
        ("eeg_face13/sub-010", 5),
    ],
)
def test_index_subject_dir(path: str, expected_count: int):
    """Index a single subject directory and assert the expected file count."""
    _, table = indexing._index_bids_subject_dir(BIDS_EXAMPLES / path)
    assert len(table) == expected_count


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("ieeg_epilepsyNWB/derivatives/brainvisa/sub-01_ses-pre", False),
    ],
)
def test_is_bids_subject_dir(path: str, *, expected: bool):
    """Classify paths as BIDS subject directories (or not)."""
    assert indexing._is_bids_subject_dir(BIDS_EXAMPLES / path) == expected


@pytest.mark.parametrize(
    ("path", "expected"),
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
            # JSON data file with compound extension.
            "sub-0025428_ses-1_hemi-L_space-native_midthickness.surf.json",
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
def test_is_bids_file(path: str, *, expected: bool):
    """Classify paths as BIDS data files (or sidecars/directories)."""
    assert indexing._is_bids_file(Path(path)) == expected


def test_filter_include_exclude():
    """Filter names by include pattern then exclude patterns."""
    names = ["blah", "sub-A01", "sub-A02", "sub-B01", "sub-B02"]
    include = "sub-*"
    exclude = ["sub-B*", "sub-A02"]
    expected = {"sub-A01"}
    filtered_names = indexing._filter_include(names, include)
    filtered_names = indexing._filter_exclude(filtered_names, exclude)
    assert filtered_names == expected


@pytest.mark.parametrize(
    ("num", "expected"),
    [
        (12, "12"),
        (1234, "1234"),
        (65432, "65K"),
        (165432, "165K"),
        (2165432, "2.2M"),
        (52165432, "52M"),
    ],
)
def test_h_fmt(num: int, expected: str):
    """Format human-readable counts for progress bars."""
    assert indexing._hfmt(num) == expected


def test_index_dataset_accepts_schema_kwarg(tmp_path: Path):
    """Index a dataset with a custom schema and verify metadata propagation."""
    # Build a minimal BIDS dataset.
    sub = tmp_path / "ds" / "sub-A01" / "anat"
    sub.mkdir(parents=True)
    (tmp_path / "ds" / "dataset_description.json").write_text('{"Name": "ds"}')
    (sub / "sub-A01_T1w.nii.gz").touch()

    ns = deepcopy(bidsschematools.schema.load_schema())
    ns.objects.entities.subject["description"] = "Modified once"
    table = indexing.index_dataset(tmp_path / "ds", schema=ns)
    assert table.num_rows == 1
    assert "sub" in table.column_names
    assert table.schema.field("sub").metadata[b"description"] == b"Modified once"


def test_batch_index_dataset_accepts_schema_kwarg(tmp_path: Path):
    """Batch-index with a custom schema and verify metadata propagation."""
    from bids2table._indexing import batch_index_dataset

    ds1 = tmp_path / "a"
    (ds1 / "sub-A01" / "anat").mkdir(parents=True)
    (ds1 / "dataset_description.json").write_text('{"Name": "a"}')
    (ds1 / "sub-A01" / "anat" / "sub-A01_T1w.nii.gz").touch()

    ns = deepcopy(bidsschematools.schema.load_schema())
    ns.objects.entities.subject["description"] = "And again"
    tables = list(batch_index_dataset([ds1], schema=ns))
    assert len(tables) == 1
    assert tables[0].num_rows == 1
    assert tables[0].schema.field("sub").metadata[b"description"] == b"And again"
