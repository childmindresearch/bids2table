"""Tests for BIDS dataset indexing and related internals."""

import json
import logging
import warnings
from copy import deepcopy
from itertools import islice
from pathlib import Path

import bids2table._indexing as indexing
import bidsschematools.schema
import pyarrow as pa
import pytest
from bids2table._entities import get_root_entity_types
from bids2table._schema import BIDSSchemaAdapter

BIDS_EXAMPLES = Path(__file__).parents[1] / "bids-examples"


def test_get_arrow_schema():
    """The arrow schema contains the expected number of fields."""
    schema = indexing.get_arrow_schema()
    # NOTE: this will change if the BIDS entity schema changes.
    assert len(schema) == 45


def test_get_column_names():
    """Column names match the arrow schema and resolve correctly."""
    schema = indexing.get_arrow_schema()
    bids_column = indexing.get_column_names()
    assert len(bids_column) == len(schema)
    assert bids_column["dataset"] == "dataset"


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


@pytest.mark.cloud
def test_index_dataset_s3_filter():
    """An entity filter applies when indexing a BIDS dataset stored on S3."""
    root = "s3://openneuro.org/ds000102"
    # Full index is 130 files (see test_index_dataset_s3); sub-01 has 5.
    table = indexing.index_dataset(root, filters={"sub": "01"})
    assert len(table) == 5
    assert set(table.column("sub").to_pylist()) == {"01"}


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
        # Has dataset_description.json but no valid entity dirs.
        ("ieeg_epilepsy/derivatives/brainvisa", "no matching entity"),
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
    # NOTE: count may change as BIDS schema evolves and entity-generic
    # discovery finds more entity types (tpl, cohort, sample, etc.).
    # Also reflects .bidsignore filtering (5 datasets have .bidsignore files).
    assert len(table) == 9616


@pytest.mark.parametrize("ds_name", ["dataset", "dataset2", "dataset3"])
def test_indexing_on_symlinks(symlink_dataset: Path, ds_name: str):
    """Follow symlinks when indexing BIDS datasets."""
    tables = indexing.batch_index_dataset(
        list(indexing.find_bids_datasets(symlink_dataset / ds_name)),
        show_progress=False,
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
def test_get_bids_dataset(path: str, expected_name: str, adapter: BIDSSchemaAdapter):
    """Resolve the BIDS dataset name and root for nested/flat paths."""
    name, dataset_path = indexing._get_bids_dataset(BIDS_EXAMPLES / path)
    assert name == expected_name
    assert dataset_path is not None
    root_prefixes = get_root_entity_types(adapter)
    pattern = indexing._compile_entity_dir_pattern(root_prefixes, adapter)
    assert indexing._contains_bids_entity_dirs(dataset_path, root_prefixes, pattern)


@pytest.mark.parametrize(
    ("path", "include_subjects", "expected_count"),
    [
        ("ds102", None, 26),
        ("ds102", "sub-07", 1),
        ("ds102", "sub-0*", 9),
        ("ds102", ["sub-01", "sub-02", "sub-05"], 3),
    ],
)
def test_find_bids_entity_dirs(
    path: str,
    include_subjects: str | list[str] | None,
    expected_count: int,
    adapter: BIDSSchemaAdapter,
):
    """Find entity directories with optional inclusion filters."""
    root_prefixes = get_root_entity_types(adapter)
    pattern = indexing._compile_entity_dir_pattern(root_prefixes, adapter)
    entity_dirs = indexing._find_bids_entity_dirs(
        BIDS_EXAMPLES / path,
        root_prefixes,
        pattern,
        include_subjects,
    )
    assert len(entity_dirs) == expected_count


@pytest.mark.parametrize(
    ("path", "expected_count"),
    [
        ("ds102/sub-03", 5),
        ("synthetic/derivatives/fmriprep/sub-02", 30),
        ("eeg_face13/sub-010", 5),
    ],
)
def test_index_entity_dir(path: str, expected_count: int, adapter: BIDSSchemaAdapter):
    """Index a single entity directory and assert the expected file count."""
    _, table = indexing._index_bids_entity_dir(
        BIDS_EXAMPLES / path, entity_prefix="sub", adapter=adapter
    )
    assert len(table) == expected_count


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
def test_is_bids_file(path: str, *, expected: bool, adapter: BIDSSchemaAdapter):
    """Classify paths as BIDS data files (or sidecars/directories)."""
    assert indexing._is_bids_file(Path(path), adapter) == expected


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


class TestFilters:
    """Tests pertaining to filtering."""

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            ([], {}),
            (["task=rest"], {"task": ["rest"]}),
            (["task=rest", "task=movie"], {"task": ["rest", "movie"]}),
            (["task=rest", "run=1"], {"task": ["rest"], "run": ["1"]}),
            (["no_equals"], {}),  # malformed, skipped
            (["task=rest", "bad", "run=2"], {"task": ["rest"], "run": ["2"]}),
        ],
    )
    def test_parse_filters(self, args: list[str], expected: dict[str, list[str]]):
        """Parse filter arguments into key → [patterns] dict."""
        from bids2table.__main__ import _parse_filters

        result = _parse_filters(args)
        assert result == expected

    @pytest.mark.parametrize(
        ("filters", "expected"),
        [
            ({"task": ["rest"]}, {"task": "rest"}),
            ({"task": ["rest", "movie"]}, {"task": ["rest", "movie"]}),
            (
                {"task": ["rest"], "run": ["1", "2"]},
                {"task": "rest", "run": ["1", "2"]},
            ),
            ({}, {}),
        ],
    )
    def test_normalize_filters(self, filters: dict[str, list[str]], expected: dict):
        """Normalize single-item lists to bare strings."""
        from bids2table.__main__ import _normalize_filters

        assert _normalize_filters(filters) == expected

    @pytest.mark.parametrize(
        ("value", "key", "pattern", "expected"),
        [
            ("A01", "sub", "A01", True),
            ("01", "sub", "sub-01", True),  # compound match
            ("A01", "sub", "B*", False),
            (1, "run", "1", True),  # int value
            (2, "run", "1", False),
        ],
    )
    def test_match_single(
        self, value: str | int, key: str, pattern: str, expected: bool
    ):
        """Match a single entity value against a glob pattern."""
        assert indexing._match_single(value, key, pattern) == expected

    @pytest.mark.parametrize(
        ("entities", "filters", "expected"),
        [
            ({"task": "rest", "run": 1}, {"task": "rest"}, True),
            ({"task": "rest", "run": 1}, {"task": "rest", "run": "1"}, True),
            ({"task": "rest", "run": 1}, {"task": "movie"}, False),  # value mismatch
            ({"task": "rest"}, {"task": "rest", "run": "1"}, False),  # missing key
            ({"task": "rest", "run": 1}, {"task": ["rest", "movie"]}, True),  # list
            ({"task": "rest"}, {}, True),  # empty filters
        ],
    )
    def test_match_filters(self, entities: dict, filters: dict, expected: bool):
        """Check if an entities dict matches all filters."""
        assert indexing._match_filters(entities, filters) == expected


def _make_minimal_ds(tmp_path: Path, name: str = "ds") -> Path:
    """Create a minimal BIDS dataset for testing."""
    ds = tmp_path / name
    (ds / "sub-A01" / "anat").mkdir(parents=True)
    (ds / "sub-A02" / "anat").mkdir(parents=True)
    (ds / "dataset_description.json").write_text(
        '{"Name": "TestDS", "BIDSVersion": "1.8.0"}'
    )
    (ds / "sub-A01" / "anat" / "sub-A01_T1w.nii.gz").touch()
    (ds / "sub-A01" / "anat" / "sub-A01_task-rest_bold.nii.gz").touch()
    (ds / "sub-A02" / "anat" / "sub-A02_T1w.nii.gz").touch()
    return ds


def test_index_dataset_filters_task(tmp_path: Path):
    """Filter by task entity reduces results."""
    ds = _make_minimal_ds(tmp_path)
    # Without filter: 3 files, with filter: 1 file
    assert len(indexing.index_dataset(ds)) == 3
    table = indexing.index_dataset(ds, filters={"task": "rest"})
    assert len(table) == 1


def test_index_dataset_filters_subject(tmp_path: Path):
    """Filter by subject entity reduces results."""
    ds = _make_minimal_ds(tmp_path)
    table = indexing.index_dataset(ds, filters={"sub": "A01"})
    assert len(table) == 2


def test_index_dataset_filters_compound_pattern(tmp_path: Path):
    """Filter with compound prefix pattern works via _match_single."""
    ds = _make_minimal_ds(tmp_path)
    table = indexing.index_dataset(ds, filters={"sub": "sub-A01"})
    assert len(table) == 2


def test_index_dataset_no_match_filters(tmp_path: Path):
    """Filter with no matching files returns empty table."""
    ds = _make_minimal_ds(tmp_path)
    table = indexing.index_dataset(ds, filters={"task": "nofile"})
    assert len(table) == 0


def test_index_dataset_include_subjects_deprecated(tmp_path: Path):
    """Using include_subjects emits DeprecationWarning."""
    ds = _make_minimal_ds(tmp_path)
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        indexing.index_dataset(ds, include_subjects="A01")
    dep_warnings = [w for w in warns if issubclass(w.category, DeprecationWarning)]
    assert len(dep_warnings) == 1
    assert "filters" in str(dep_warnings[0].message)


def test_index_dataset_new_columns(tmp_path: Path):
    """New columns populated from dataset_description.json."""
    ds = _make_minimal_ds(tmp_path)
    table = indexing.index_dataset(ds)
    assert "dataset_name" in table.column_names
    assert "dataset_type" in table.column_names
    assert "bids_version" in table.column_names
    assert table["dataset_name"][0].as_py() == "TestDS"
    assert table["bids_version"][0].as_py() == "1.8.0"


def test_index_dataset_new_columns_missing(tmp_path: Path):
    """New columns are null when dataset_description.json is absent."""
    ds = tmp_path / "ds"
    (ds / "sub-A01" / "anat").mkdir(parents=True)
    (ds / "sub-A01" / "anat" / "sub-A01_T1w.nii.gz").touch()
    table = indexing.index_dataset(ds)
    assert "dataset_name" in table.column_names
    # No dataset_description.json → values should be null
    assert table["dataset_name"][0].as_py() is None


def test_clear_schema_caches():
    """Calling clear_schema_caches clears the relevant caches."""
    indexing._get_bids_dataset.cache_clear()
    indexing._is_bids_dataset.cache_clear()
    # Warm the caches.
    indexing._get_bids_dataset(BIDS_EXAMPLES / "ds102")
    indexing._is_bids_dataset(BIDS_EXAMPLES / "ds102")
    # Verify caches are populated.
    assert indexing._get_bids_dataset.__wrapped__ is not None
    indexing.clear_schema_caches()


def test_index_derivative_without_description(tmp_path: Path):
    """Derivative under derivatives/ without a description is detected and indexed."""
    deriv = tmp_path / "ds" / "derivatives" / "fmriprep"
    (deriv / "sub-A01" / "anat").mkdir(parents=True)
    (deriv / "sub-A01" / "anat" / "sub-A01_T1w.nii.gz").touch()

    # Detected as a dataset despite lacking a description file.
    assert indexing._is_bids_dataset(deriv)
    # Typed as a derivative via the nested-parent heuristic.
    assert indexing._get_dataset_type(deriv, {}) == "derivative"

    table = indexing.index_dataset(deriv)
    assert table.num_rows == 1
    assert table.column("sub").to_pylist() == ["A01"]
    assert table.column("dataset").to_pylist() == ["fmriprep"]
    assert table.column("dataset_type").to_pylist() == ["derivative"]


def test_schema_switch_mid_session_uses_new_schema(tmp_path: Path):
    """A changed schema file is re-read only after clear_schema_caches()."""
    import bidsschematools

    ds = tmp_path / "ds"
    (ds / "sub-A01" / "anat").mkdir(parents=True)
    (ds / "sub-A01" / "anat" / "sub-A01_T1w.nii.gz").touch()
    (ds / "dataset_description.json").write_text('{"Name": "ds"}')

    default_schema = Path(bidsschematools.__file__).parent / "data" / "schema.json"

    def write_schema(marker: str) -> None:
        schema = json.loads(default_schema.read_text())
        schema["objects"]["entities"]["subject"]["description"] = marker
        schema_path.write_text(json.dumps(schema))

    schema_path = tmp_path / "schema.json"
    m1, m2 = "first schema", "second schema"
    write_schema(m1)

    # Warm the path-based schema cache with the first schema.
    t1 = indexing.index_dataset(ds, schema=schema_path)
    assert t1.schema.field("sub").metadata[b"description"] == m1.encode()

    # Switch the schema file in place; the lru cache now serves a stale adapter.
    write_schema(m2)
    stale = indexing.index_dataset(ds, schema=schema_path)
    assert stale.schema.field("sub").metadata[b"description"] == m1.encode()

    # clear_schema_caches() forces a reload of the changed file.
    indexing.clear_schema_caches()
    t2 = indexing.index_dataset(ds, schema=schema_path)
    assert t2.schema.field("sub").metadata[b"description"] == m2.encode()


def test_read_dataset_description(tmp_path: Path):
    """Read dataset_description.json and return as dict."""
    ds = tmp_path / "ds"
    ds.mkdir()
    (ds / "dataset_description.json").write_text(
        '{"Name": "TestDS", "BIDSVersion": "1.8.0"}'
    )
    desc = indexing._read_dataset_description(ds)
    assert desc["Name"] == "TestDS"
    assert desc["BIDSVersion"] == "1.8.0"


def test_read_dataset_description_missing(tmp_path: Path):
    """Return empty dict when dataset_description.json is absent."""
    desc = indexing._read_dataset_description(tmp_path)
    assert desc == {}


def test_batch_index_dataset_with_filters(tmp_path: Path):
    """Batch-index with filters parameter reduces results."""
    ds1 = _make_minimal_ds(tmp_path, "ds1")
    ds2 = _make_minimal_ds(tmp_path, "ds2")
    tables = list(
        indexing.batch_index_dataset(
            [ds1, ds2], filters={"task": "rest"}, show_progress=False
        )
    )
    # Each dataset has 1 file matching task=rest.
    assert all(len(t) == 1 for t in tables)


def test_bidsignore_excludes_matching_files(bidsignore_dataset: Path):
    """Files matching .bidsignore patterns are excluded from the index."""
    (bidsignore_dataset / ".bidsignore").write_text(
        "# ignore bold files\nsub-A01_*bold*\n"
    )
    table = indexing.index_dataset(bidsignore_dataset)
    assert len(table) == 1
    assert table["path"][0].as_py() == "sub-A01/anat/sub-A01_T1w.nii.gz"


def test_bidsignore_ignores_comments_and_blanks(bidsignore_dataset: Path):
    """Comments and blank lines in .bidsignore are skipped."""
    (bidsignore_dataset / ".bidsignore").write_text("# comment only\n\n   \n")
    table = indexing.index_dataset(bidsignore_dataset)
    # Both files indexed: a comments/blanks-only .bidsignore excludes nothing.
    assert len(table) == 2


def test_bidsignore_absent_files_included(bidsignore_dataset: Path):
    """All files are indexed when .bidsignore is absent."""
    table = indexing.index_dataset(bidsignore_dataset)
    assert len(table) == 2


def test_load_bidsignore_patterns(tmp_path: Path):
    """_load_bidsignore_patterns returns patterns, skipping blanks and comments."""
    (tmp_path / ".bidsignore").write_text("# comment\n\nsub-01/*\n*.html\n")
    patterns = indexing._load_bidsignore_patterns(str(tmp_path))
    assert patterns == ("sub-01/*", "*.html")


def test_load_bidsignore_patterns_missing(tmp_path: Path):
    """_load_bidsignore_patterns returns empty tuple when .bidsignore is absent."""
    patterns = indexing._load_bidsignore_patterns(str(tmp_path))
    assert patterns == ()


def test_is_bidsignored(bidsignore_dataset: Path):
    """_is_bidsignored matches relative paths against patterns."""
    ds = bidsignore_dataset
    (ds / ".bidsignore").write_text("sub-A01/*\n")
    assert indexing._is_bidsignored(ds / "sub-A01" / "anat" / "sub-A01_T1w.nii.gz", ds)
    assert not indexing._is_bidsignored(
        ds / "sub-A02" / "anat" / "sub-A02_T1w.nii.gz", ds
    )


def test_is_bidsignored_matches_filename(bidsignore_dataset: Path):
    """Pattern matching the filename works even when the full path doesn't."""
    ds = bidsignore_dataset
    (ds / ".bidsignore").write_text("sub-A01_*bold*\n")
    # Full path is sub-A01/func/sub-A01_bold.nii.gz — pattern matches filename only.
    assert indexing._is_bidsignored(ds / "sub-A01" / "func" / "sub-A01_bold.nii.gz", ds)
    assert not indexing._is_bidsignored(
        ds / "sub-A01" / "func" / "sub-A01_T1w.nii.gz", ds
    )
