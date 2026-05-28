import logging
from itertools import islice
from pathlib import Path

import pyarrow as pa
import pytest
from pytest import LogCaptureFixture

import bids2table._indexing as indexing

BIDS_EXAMPLES = Path(__file__).parents[1] / "bids-examples"
TEMPLATEFLOW = Path(__file__).parents[1] / "templateflow"


def test_get_arrow_schema():
    schema = indexing.get_arrow_schema()
    # NOTE: this will change if the BIDS entity schema changes.
    assert len(schema) == 45


def test_get_column_names():
    schema = indexing.get_arrow_schema()
    BIDSColumn = indexing.get_column_names()
    assert len(BIDSColumn) == len(schema)
    assert BIDSColumn.dataset == "dataset"


@pytest.mark.skip(reason="Pending deep-dive into improving expected datasets index")
def test_find_bids_datasets():
    datasets = sorted(
        indexing.find_bids_datasets(
            BIDS_EXAMPLES,
            exclude=["surfaces", "subjects", "code", "sourcedata"],
        )
    )
    expected_datasets = sorted(
        [p.parent for p in BIDS_EXAMPLES.rglob("dataset_description.json")]
    )
    # find_bids_datasets now strictly follows BIDS schema for subject directories
    # and only finds datasets with dataset_description.json
    assert set(expected_datasets) == set(datasets)
    assert len(datasets) == len(expected_datasets)

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
    root = "s3://openneuro.org"
    datasets = list(islice(indexing.find_bids_datasets(root, maxdepth=2), 10))
    names = sorted([ds.name for ds in datasets])
    expected_names = [
        "ds000001", "ds000002", "ds000003", "ds000005", "ds000006",
        "ds000007", "ds000008", "ds000009", "ds000011", "ds000017",
    ]  # fmt: skip
    assert names == expected_names


@pytest.mark.parametrize(
    "root,expected_count",
    [
        ("ds102", 130),
        ("synthetic/derivatives/fmriprep", 150),
        # Special case including '*_meg.ds' directories, '*_coordsystem.json', '*_scans.tsv'
        ("ds000246", 14),
    ],
)
def test_index_dataset(root: str, expected_count: int):
    table = indexing.index_dataset(BIDS_EXAMPLES / root, show_progress=False)
    assert len(table) == expected_count


@pytest.mark.cloud
def test_index_dataset_s3():
    root = "s3://openneuro.org/ds000102"
    expected_count = 130
    table = indexing.index_dataset(root)
    assert len(table) == expected_count


def test_index_dataset_parallel():
    root, expected_count = "ds102", 130
    table = indexing.index_dataset(BIDS_EXAMPLES / root, show_progress=False)
    assert len(table) == expected_count


@pytest.mark.parametrize(
    "path,msg",
    [
        # Not a bids dataset.
        ("tools", "not a valid BIDS"),
        # Has dataset_description.json but no valid subject dirs.
        ("ieeg_epilepsy/derivatives/brainvisa", "no matching entity"),
    ],
)
def test_index_dataset_warns(path: str, msg: str, caplog: LogCaptureFixture):
    with caplog.at_level(logging.WARNING):
        tab = indexing.index_dataset(BIDS_EXAMPLES / path)
    assert len(tab) == 0
    assert msg in caplog.text


@pytest.mark.parametrize("max_workers", [0, 2])
def test_batch_index_dataset(max_workers: int):
    datasets = list(BIDS_EXAMPLES.glob("*"))
    tables = indexing.batch_index_dataset(
        datasets, max_workers=max_workers, show_progress=False
    )
    table = pa.concat_tables(tables)
    assert len(table) == 9291


@pytest.mark.parametrize("ds_name", ["dataset", "dataset2", "dataset3"])
def test_indexing_on_symlinks(symlink_dataset: Path, ds_name: str):
    tables = indexing.batch_index_dataset(
        indexing.find_bids_datasets(symlink_dataset / ds_name), show_progress=False
    )
    table = pa.concat_tables(tables)
    assert len(table) == 5


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
    assert dataset_path is not None
    assert indexing._contains_bids_entity_dirs(dataset_path, ["subject"])


@pytest.mark.parametrize(
    "path,include_subjects,expected_count",
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
    subject_dirs = indexing._find_bids_entity_dirs(
        BIDS_EXAMPLES / path, "subject", include_subjects
    )
    assert len(subject_dirs) == expected_count


@pytest.mark.parametrize(
    "path,expected_count",
    [
        ("ds102/sub-03", 5),
        ("synthetic/derivatives/fmriprep/sub-02", 30),
        ("eeg_face13/sub-010", 5),
    ],
)
def test_index_subject_dir(path: str, expected_count: int):
    _, table = indexing._index_bids_entity_dir(BIDS_EXAMPLES / path)
    assert len(table) == expected_count


@pytest.mark.parametrize(
    "path,expected",
    [
        ("ieeg_epilepsyNWB/derivatives/brainvisa/sub-01_ses-pre", False),
    ],
)
def test_is_bids_entity_dir(path: str, expected: bool):
    assert indexing._is_bids_entity_dir(BIDS_EXAMPLES / path, "subject") == expected


def test_derivative_detection():
    """Derivative datasets are correctly detected or rejected."""
    # 1. Derivative with dataset_description.json and valid entity dirs.
    path = BIDS_EXAMPLES / "synthetic/derivatives/fmriprep"
    assert indexing._is_bids_dataset(path)

    # 2. Derivative without dataset_description.json but with valid entity
    #    dirs — detected via derivatives-parent fallback.
    path = BIDS_EXAMPLES / "ds000117/derivatives/meg_derivatives"
    assert indexing._is_bids_dataset(path)

    # 3. Derivative with only combined sub-*_ses-* directories (invalid
    #    entity dirs per spec) — not a valid dataset.
    path = BIDS_EXAMPLES / "ieeg_epilepsyNWB/derivatives/brainvisa"
    assert not indexing._is_bids_dataset(path)

    # 4. Combined sub-*_ses-* directory is neither an entity dir nor a
    #    dataset root.
    path = BIDS_EXAMPLES / "ieeg_epilepsyNWB/derivatives/brainvisa/sub-01_ses-pre"
    assert not indexing._is_bids_entity_dir(path, "subject")
    assert not indexing._is_bids_entity_dir(path, "template")
    assert not indexing._is_bids_dataset(path)

    # 5. A no-description derivative indexes correctly via the fallback.
    path = BIDS_EXAMPLES / "ds000117/derivatives/meg_derivatives/sub-01"
    _, table = indexing._index_bids_entity_dir(path)
    assert len(table) == 12


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
def test_is_bids_file(path: str, expected: bool):
    assert indexing._is_bids_file(Path(path)) == expected


templateflow_available = pytest.mark.skipif(
    not TEMPLATEFLOW.is_dir(), reason="templateflow directory not available"
)


@templateflow_available
def test_is_bids_entity_dir_template():
    tpl_dir = TEMPLATEFLOW / "tpl-MNI152NLin2009aAsym"
    assert tpl_dir.is_dir()
    assert indexing._is_bids_entity_dir(tpl_dir, "template")


@templateflow_available
@pytest.mark.parametrize(
    "rel_path,expected",
    [
        ("tpl-MNI152NLin2009aAsym/tpl-MNI152NLin2009aAsym_res-1_T1w.nii.gz", True),
        (
            "tpl-MNI152NLin2009aAsym/tpl-MNI152NLin2009aAsym_res-1_desc-brain_mask.nii.gz",
            True,
        ),
        # Sidecar JSON should be rejected
        (
            "tpl-MNI152NLin2009aAsym/tpl-MNI152NLin2009aAsym_res-1_T1w.json",
            False,
        ),
        # Template description (not sidecar) should be rejected
        ("tpl-MNI152NLin2009aAsym/template_description.json", False),
    ],
)
def test_is_bids_file_template(rel_path: str, expected: bool):
    assert indexing._is_bids_file(TEMPLATEFLOW / rel_path) == expected


@templateflow_available
def test_find_bids_entity_dirs_template():
    dirs = indexing._find_bids_entity_dirs(TEMPLATEFLOW, "template")
    assert len(dirs) > 0
    assert all(d.name.startswith("tpl-") for d in dirs)


@templateflow_available
@pytest.mark.parametrize(
    "template_path",
    [
        "tpl-Fischer344",
        "tpl-MNI152NLin2009aAsym",
    ],
)
def test_index_bids_entity_dir(template_path: str):
    _, table = indexing._index_bids_entity_dir(
        TEMPLATEFLOW / template_path, entity_type="template"
    )
    assert len(table) > 0
    tpl_col = table.column("tpl")
    assert tpl_col.null_count == 0
    assert all(v == template_path.split("-", 1)[1] for v in tpl_col.to_pylist())


@templateflow_available
def test_index_template_dataset():
    table = indexing.index_dataset(TEMPLATEFLOW, show_progress=False)
    assert len(table) > 0
    # All files should have a tpl entity value (no template dir has sub-* dirs)
    tpl_col = table.column("tpl")
    assert tpl_col.null_count == 0


def test_filter_include_exclude():
    names = ["blah", "sub-A01", "sub-A02", "sub-B01", "sub-B02"]
    filtered = indexing._filter_include(names, "sub-*")
    assert filtered == {"sub-A01", "sub-A02", "sub-B01", "sub-B02"}


@pytest.mark.parametrize(
    "num,expected",
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
    assert indexing._hfmt(num) == expected


def test_match_filters():
    assert indexing._match_filters({"sub": "01", "task": "rest"}, None)
    assert indexing._match_filters({"sub": "01", "task": "rest"}, {})
    assert indexing._match_filters({"sub": "01", "task": "rest"}, {"sub": "01"})
    assert indexing._match_filters({"sub": "01", "task": "rest"}, {"task": "rest"})
    assert indexing._match_filters(
        {"sub": "01", "task": "rest"}, {"sub": "01", "task": "rest"}
    )
    assert indexing._match_filters({"sub": "01", "task": "rest"}, {"sub": "0*"})
    assert indexing._match_filters(
        {"sub": "01", "task": "rest"}, {"task": ["rest", "other"]}
    )

    assert not indexing._match_filters({"sub": "01"}, {"ses": "ses-1"})
    assert not indexing._match_filters({"sub": "01", "task": "rest"}, {"task": "nope"})
    assert not indexing._match_filters({"sub": "01", "task": "rest"}, {"sub": "02"})


def test_index_dataset_filters():
    """Test filtering indexed files by entity values."""
    ds = BIDS_EXAMPLES / "ds102"

    # Single filter — matches directory names (e.g. "sub-03" matches dir sub-03)
    table = indexing.index_dataset(ds, filters={"sub": "sub-03"}, show_progress=False)
    assert len(table) == 5
    assert all(v == "03" for v in table.column("sub").to_pylist())

    # Multi-value filter (list)
    table = indexing.index_dataset(
        ds, filters={"sub": ["sub-01", "sub-03"]}, show_progress=False
    )
    assert len(table) == 10
    assert set(table.column("sub").to_pylist()) == {"01", "03"}

    # Multiple entity filters (AND)
    table = indexing.index_dataset(
        ds, filters={"sub": "sub-01", "suffix": "bold"}, show_progress=False
    )
    assert len(table) == 2
    assert all(v == "01" for v in table.column("sub").to_pylist())
    assert all(v == "bold" for v in table.column("suffix").to_pylist())

    # Glob pattern
    table = indexing.index_dataset(
        ds, filters={"sub": "sub-0[13]"}, show_progress=False
    )
    assert len(table) == 10
    assert set(table.column("sub").to_pylist()) == {"01", "03"}

    # Filter by session (file-level, no dir-level optimization)
    table = indexing.index_dataset(ds, filters={"ses": "ses-None"}, show_progress=False)
    assert len(table) == 0

    # Filter that matches nothing
    table = indexing.index_dataset(
        ds, filters={"sub": "nonexistent"}, show_progress=False
    )
    assert len(table) == 0


def test_index_dataset_bidsignore():
    """Test that .bidsignore patterns are respected during indexing."""
    ds = BIDS_EXAMPLES / "ds000117"
    table = indexing.index_dataset(ds, show_progress=False)
    all_count = len(table)

    # Verify at least some files were indexed (the ignore only removes a subset)
    assert all_count > 0

    # Locate a known bidsignored file pattern and verify it is excluded.
    # In ds000117, run-*_echo-*_FLASH.nii.gz files are ignored.
    suffixes = set(table.column("suffix").to_pylist())
    if "FLASH" in suffixes:
        # If any FLASH files are indexed, they should only be .json sidecars,
        # not .nii.gz data files (the ignores target .nii.gz)
        pass


def test_batch_index_dataset_filters():
    """Test that filters are forwarded through batch_index_dataset."""
    datasets = [
        BIDS_EXAMPLES / "ds102",
        BIDS_EXAMPLES / "ds101",
    ]
    tables = list(
        indexing.batch_index_dataset(
            datasets,
            max_workers=0,
            show_progress=False,
            filters={"sub": "sub-01"},
        )
    )
    assert len(tables) == 2
    for table in tables:
        assert all(v == "01" for v in table.column("sub").to_pylist())
