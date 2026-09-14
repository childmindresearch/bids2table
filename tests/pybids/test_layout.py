"""Tests for BIDSLayout class."""

import tempfile
from collections.abc import Callable
from pathlib import Path

import pytest

pytest.importorskip("pandas", reason="pandas not available")

from bids2table.pybids import (
    BIDSFile,
    BIDSLayout,
    Query,
)

BIDS_EXAMPLES = Path(__file__).resolve().parents[2] / "bids-examples"

# Factory fixtures (from conftest) exposed to these tests.
LayoutFactory = Callable[..., BIDSLayout]
DatasetCopyFactory = Callable[[str], Path]


# Fixture for test dataset
@pytest.fixture
def test_dataset():
    """Return path to a test BIDS dataset."""
    # Use one of the bids-examples datasets
    dataset_path = Path(__file__).parents[2] / "bids-examples" / "ds114"
    if not dataset_path.exists():
        pytest.skip(f"Test dataset not found: {dataset_path}")
    return dataset_path


@pytest.fixture
def layout(test_dataset: Path):
    """Create a BIDSLayout for testing."""
    # Use temporary cache to avoid polluting test dataset
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = Path(tmpdir) / "test_cache.parquet"
        yield BIDSLayout(test_dataset, validate=False, cache_path=cache_path)


class TestBIDSLayoutInit:
    """Tests for BIDSLayout initialization."""

    def test_init_basic(self, test_dataset: Path):
        """Test basic initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "cache.parquet"
            layout = BIDSLayout(test_dataset, cache_path=cache_path)

            assert layout.root == test_dataset.absolute()
            assert layout.df is not None
            assert len(layout.df) > 0

    def test_init_with_cache(self, test_dataset: Path):
        """Test that cache is created and reused."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "cache.parquet"

            # First initialization - creates cache
            layout1 = BIDSLayout(test_dataset, cache_path=cache_path)
            assert cache_path.exists()
            n_files_1 = len(layout1.df)

            # Second initialization - uses cache
            layout2 = BIDSLayout(test_dataset, cache_path=cache_path)
            n_files_2 = len(layout2.df)

            assert n_files_1 == n_files_2

    def test_repr(self, layout: BIDSLayout):
        """Test string representation."""
        repr_str = repr(layout)
        assert "BIDSLayout" in repr_str
        assert "subjects=" in repr_str
        assert "files=" in repr_str


class TestBIDSLayoutGet:
    """Tests for BIDSLayout.get() method."""

    def test_get_basic(self, layout: BIDSLayout):
        """Test basic file query."""
        files = layout.get(return_type="filename")
        assert isinstance(files, list)
        assert len(files) > 0
        assert all(isinstance(f, str) for f in files)

    def test_get_by_subject(self, layout: BIDSLayout):
        """Test filtering by subject."""
        subjects = layout.get_subjects()
        if not subjects:
            pytest.skip("No subjects in dataset")

        subject = subjects[0]
        files = layout.get(subject=subject, return_type="filename")

        assert len(files) > 0
        # Check that all files contain subject ID
        assert all(f"sub-{subject}" in f for f in files)

    def test_get_by_suffix(self, layout: BIDSLayout):
        """Test filtering by suffix."""
        # Try common suffixes
        for suffix in ["T1w", "bold", "events"]:
            files = layout.get(suffix=suffix, return_type="filename")
            if files:
                # Check that files have the expected suffix
                assert any(suffix in f for f in files)
                break
        else:
            pytest.skip("No files with common suffixes found")

    def test_get_multiple_filters(self, layout: BIDSLayout):
        """Test filtering by multiple entities."""
        subjects = layout.get_subjects()
        if not subjects:
            pytest.skip("No subjects in dataset")

        subject = subjects[0]
        files = layout.get(subject=subject, datatype="anat", return_type="filename")

        # Should have filtered by both subject and datatype
        # (May be empty if subject doesn't have anat data)
        assert isinstance(files, list)

    def test_get_return_type_file(self, layout: BIDSLayout):
        """Test return_type='file' returns BIDSFile objects."""
        files = layout.get(return_type="file")

        assert len(files) > 0
        assert all(isinstance(f, BIDSFile) for f in files)

    def test_get_return_type_filename(self, layout: BIDSLayout):
        """Test return_type='filename' returns strings."""
        files = layout.get(return_type="filename")

        assert len(files) > 0
        assert all(isinstance(f, str) for f in files)

    def test_get_return_type_id(self, layout: BIDSLayout):
        """Test return_type='id' returns indices."""
        ids = layout.get(return_type="id")

        assert len(ids) > 0
        assert all(isinstance(i, (int | type(ids[0]))) for i in ids)

    def test_get_return_type_dir(self, layout: BIDSLayout):
        """Test return_type='dir' returns unique directories."""
        dirs = layout.get(return_type="dir")

        assert len(dirs) > 0
        assert all(isinstance(d, str) for d in dirs)
        # Should be unique
        assert len(dirs) == len(set(dirs))

    def test_get_with_list_values(self, layout: BIDSLayout):
        """Test filtering with list of values."""
        subjects = layout.get_subjects()
        if len(subjects) < 2:
            pytest.skip("Need at least 2 subjects")

        files = layout.get(subject=subjects[:2], return_type="filename")

        assert len(files) > 0

    def test_get_with_query_optional(self, layout: BIDSLayout):
        """Test Query.OPTIONAL allows missing entities."""
        files = layout.get(session=Query.OPTIONAL, return_type="filename")

        # Should return all files regardless of session
        assert len(files) > 0

    def test_get_with_query_any(self, layout: BIDSLayout):
        """Test Query.ANY matches any value."""
        files = layout.get(suffix=Query.ANY, return_type="filename")

        # Should return all files (no filtering on suffix)
        assert len(files) > 0

    def test_get_invalid_return_type(self, layout: BIDSLayout):
        """Test that invalid return_type raises error."""
        with pytest.raises(ValueError, match="Unknown return_type"):
            layout.get(return_type="invalid")


class TestBIDSLayoutEntities:
    """Tests for entity extraction methods."""

    def test_get_subjects(self, layout: BIDSLayout):
        """Test getting subject list."""
        subjects = layout.get_subjects()

        assert isinstance(subjects, list)
        assert len(subjects) > 0
        # Should be sorted
        assert subjects == sorted(subjects)
        # Should not have 'sub-' prefix
        assert all(not s.startswith("sub-") for s in subjects)

    def test_get_subjects_with_filter(self, layout: BIDSLayout):
        """Test filtering subjects by other entities."""
        all_subjects = layout.get_subjects()
        filtered_subjects = layout.get_subjects(datatype="anat")

        # Filtered should be subset (or equal)
        assert set(filtered_subjects).issubset(set(all_subjects))

    def test_get_sessions(self, layout: BIDSLayout):
        """Test getting session list."""
        sessions = layout.get_sessions()

        assert isinstance(sessions, list)
        # May be empty if no sessions in dataset
        if sessions:
            assert sessions == sorted(sessions)
            assert all(not s.startswith("ses-") for s in sessions)

    def test_get_sessions_by_subject(self, layout: BIDSLayout):
        """Test getting sessions for specific subject."""
        subjects = layout.get_subjects()
        if not subjects:
            pytest.skip("No subjects in dataset")

        subject = subjects[0]
        sessions = layout.get_sessions(subject=subject)

        assert isinstance(sessions, list)
        # Sessions should be subset of all sessions
        all_sessions = layout.get_sessions()
        assert set(sessions).issubset(set(all_sessions))


class TestBIDSLayoutMetadata:
    """Tests for metadata access."""

    def test_get_metadata(self, layout: BIDSLayout):
        """Test loading metadata for a file."""
        files = layout.get(suffix="bold", return_type="filename")
        if not files:
            pytest.skip("No BOLD files in dataset")

        file_path = files[0]
        assert isinstance(file_path, str)
        metadata = layout.get_metadata(file_path)

        assert isinstance(metadata, dict)
        # BOLD files typically have RepetitionTime
        # (but not guaranteed in all test datasets)

    def test_get_file(self, layout: BIDSLayout):
        """Test getting BIDSFile object."""
        files = layout.get(return_type="filename")
        if not files:
            pytest.skip("No files in dataset")

        file_path = files[0]
        assert isinstance(file_path, str)
        bids_file = layout.get_file(file_path)

        assert isinstance(bids_file, BIDSFile)
        assert bids_file.path == file_path

        # Can get entities
        entities = bids_file.get_entities()
        assert isinstance(entities, dict)


class TestBIDSLayoutEntityMapping:
    """Tests for PyBIDS entity name mapping."""

    def test_subject_mapping(self, layout: BIDSLayout):
        """Test that 'subject' maps to 'sub'."""
        subjects = layout.get_subjects()
        if not subjects:
            pytest.skip("No subjects in dataset")

        # Both 'subject' and 'sub' should work
        files1 = layout.get(subject=subjects[0], return_type="filename")
        files2 = layout.get(sub=subjects[0], return_type="filename")

        assert set(files1) == set(files2)

    def test_session_mapping(self, layout: BIDSLayout):
        """Test that 'session' maps to 'ses'."""
        sessions = layout.get_sessions()
        if not sessions:
            pytest.skip("No sessions in dataset")

        # Both 'session' and 'ses' should work
        files1 = layout.get(session=sessions[0], return_type="filename")
        files2 = layout.get(ses=sessions[0], return_type="filename")

        assert set(files1) == set(files2)

    def test_extension_mapping(self, layout: BIDSLayout):
        """Test that 'extension' maps to 'ext'."""
        # Both should work
        files1 = layout.get(extension=".nii.gz", return_type="filename")
        files2 = layout.get(ext=".nii.gz", return_type="filename")

        # Should return same files (or both empty)
        assert set(files1) == set(files2)


# ---------------------------------------------------------------------------
# Green lifts: concrete pybids-API behavior pinned on the workhorse datasets.
#
# These extend the shallow type/non-emptiness checks above with exact values
# re-derived from the current bids-examples checkout (see .notes/pybids-tests/).
# They use the ``make_layout`` factory from conftest (hermetic temp cache).
# ---------------------------------------------------------------------------


class TestQuerySentinels:
    """Query sentinel semantics with exact counts (7t_trt)."""

    def test_optional_is_noop(self, make_layout: LayoutFactory):
        """OPTIONAL leaves the result set unchanged."""
        layout = make_layout("7t_trt")
        count = len(layout.get(acq=Query.OPTIONAL, return_type="filename"))
        assert count == len(layout.df)

    def test_any_is_noop(self, make_layout: LayoutFactory):
        """ANY leaves the result set unchanged."""
        layout = make_layout("7t_trt")
        count = len(layout.get(acq=Query.ANY, return_type="filename"))
        assert count == len(layout.df)

    def test_none_partitions_with_concrete_values(self, make_layout: LayoutFactory):
        """NONE (missing) plus the concrete values partition the full set."""
        layout = make_layout("7t_trt")
        present = len(
            layout.get(acq=["fullbrain", "prefrontal"], return_type="filename")
        )
        missing = len(layout.get(acq=Query.NONE, return_type="filename"))
        assert present + missing == len(layout.df)
        assert present == 262
        assert missing == 373

    def test_none_intersects_concrete_filter(self, make_layout: LayoutFactory):
        """NONE + a concrete filter: every bold file here has an acquisition."""
        layout = make_layout("7t_trt")
        assert layout.get(acq=Query.NONE, suffix="bold", return_type="filename") == []

    def test_concrete_value_split(self, make_layout: LayoutFactory):
        """Each concrete acquisition value has a stable, known count."""
        layout = make_layout("7t_trt")
        assert len(layout.get(acq="fullbrain", return_type="filename")) == 175
        assert len(layout.get(acq="prefrontal", return_type="filename")) == 87


class TestSubjectsAndSessions:
    """Exact subject/session lists (re-derived per dataset)."""

    def test_7t_trt_subjects(self, make_layout: LayoutFactory):
        """7t_trt has subjects 01 through 22."""
        layout = make_layout("7t_trt")
        assert layout.get_subjects() == [f"{i:02d}" for i in range(1, 23)]

    def test_7t_trt_sessions(self, make_layout: LayoutFactory):
        """7t_trt has two sessions, present for every subject."""
        layout = make_layout("7t_trt")
        assert layout.get_sessions() == ["1", "2"]
        assert layout.get_sessions(subject="01") == ["1", "2"]

    def test_ds005_subjects_and_no_sessions(self, make_layout: LayoutFactory):
        """ds005 has subjects 01-16 and no session entity."""
        layout = make_layout("ds005")
        assert layout.get_subjects() == [f"{i:02d}" for i in range(1, 17)]
        assert layout.get_sessions() == []

    def test_ds000117_includes_emptyroom(self, make_layout: LayoutFactory):
        """ds000117 has 17 subjects, including an empty-room recording."""
        layout = make_layout("ds000117")
        subjects = layout.get_subjects()
        assert len(subjects) == 17
        assert "emptyroom" in subjects

    def test_get_subjects_subset_with_filter(self, make_layout: LayoutFactory):
        """Filtering subjects by datatype yields a subset of all subjects."""
        layout = make_layout("7t_trt")
        filtered = layout.get_subjects(datatype="anat")
        assert set(filtered).issubset(set(layout.get_subjects()))


class TestMetadataValues:
    """Exact sidecar metadata values via BIDS inheritance."""

    def test_7t_trt_bold_fullbrain_tr(self, make_layout: LayoutFactory):
        """Fullbrain BOLD has a 3.0s repetition time."""
        layout = make_layout("7t_trt")
        f = str(layout.get(suffix="bold", acq="fullbrain", return_type="filename")[0])
        assert layout.get_metadata(f)["RepetitionTime"] == 3.0

    def test_7t_trt_bold_prefrontal_tr(self, make_layout: LayoutFactory):
        """Prefrontal BOLD has a 4.0s repetition time."""
        layout = make_layout("7t_trt")
        f = str(layout.get(suffix="bold", acq="prefrontal", return_type="filename")[0])
        assert layout.get_metadata(f)["RepetitionTime"] == 4.0

    def test_7t_trt_bold_sidecar_keys_and_types(self, make_layout: LayoutFactory):
        """A fullbrain BOLD sidecar has a known key set and value types."""
        layout = make_layout("7t_trt")
        f = str(layout.get(suffix="bold", acq="fullbrain", return_type="filename")[0])
        md = layout.get_metadata(f)
        assert set(md) == {
            "CogAtlasID",
            "EchoTime",
            "EffectiveEchoSpacing",
            "PhaseEncodingDirection",
            "RepetitionTime",
            "SliceEncodingDirection",
            "SliceTiming",
            "TaskName",
        }
        # Values span scalar (str, float) and list-typed fields.
        assert {"float", "list", "str"} <= {type(v).__name__ for v in md.values()}

    def test_ds005_t1w_has_no_sidecar(self, make_layout: LayoutFactory):
        """A T1w file with no sidecar yields empty metadata (no root-JSON bleed)."""
        layout = make_layout("ds005")
        f = str(layout.get(suffix="T1w", return_type="filename")[0])
        assert layout.get_metadata(f) == {}

    def test_ds005_bold_tr(self, make_layout: LayoutFactory):
        """ds005 BOLD has a 2.0s repetition time."""
        layout = make_layout("ds005")
        f = str(layout.get(suffix="bold", return_type="filename")[0])
        assert layout.get_metadata(f)["RepetitionTime"] == 2.0

    def test_ds000117_meg_metadata(self, make_layout: LayoutFactory):
        """A MEG (.fif) sidecar carries channel and filter metadata."""
        layout = make_layout("ds000117")
        fif = layout.get(ext=".fif", return_type="filename")
        assert fif
        md = layout.get_metadata(str(fif[0]))
        assert md["MEGChannelCount"] == 306
        assert isinstance(md["MEGChannelCount"], int)
        assert "SoftwareFilters" in md
        assert "SubjectArtefactDescription" in md

    def test_ds000117_anatomical_landmarks(self, make_layout: LayoutFactory):
        """The T1w sidecar carries Nasion/LPA/RPA landmark coordinates."""
        layout = make_layout("ds000117")
        f = str(layout.get(suffix="T1w", return_type="filename")[0])
        assert layout.get_metadata(f)["AnatomicalLandmarkCoordinates"] == {
            "Nasion": [43, 111, 95],
            "LPA": [140, 74, 16],
            "RPA": [143, 74, 173],
        }

    def test_metadata_relative_and_absolute_agree(self, make_layout: LayoutFactory):
        """Relative (from get()) and absolute paths resolve to the same metadata."""
        layout = make_layout("7t_trt")
        f = str(layout.get(suffix="bold", acq="fullbrain", return_type="filename")[0])
        assert layout.get_metadata(f) == layout.get_metadata(str(layout.root / f))


class TestGetReturnShapes:
    """get() return_type shapes with exact values (7t_trt)."""

    def test_dir_exact_sorted_unique(self, make_layout: LayoutFactory):
        """return_type='dir' returns the exact sorted unique parent directories."""
        layout = make_layout("7t_trt")
        assert layout.get(sub="01", return_type="dir") == [
            "sub-01",
            "sub-01/ses-1",
            "sub-01/ses-1/anat",
            "sub-01/ses-1/fmap",
            "sub-01/ses-1/func",
            "sub-01/ses-2",
            "sub-01/ses-2/fmap",
            "sub-01/ses-2/func",
        ]

    def test_dir_is_sorted_and_unique(self, make_layout: LayoutFactory):
        """return_type='dir' is always sorted and de-duplicated."""
        layout = make_layout("7t_trt")
        dirs = layout.get(return_type="dir")
        assert dirs == sorted(dirs)
        assert len(dirs) == len(set(dirs))

    def test_id_count_matches_rows(self, make_layout: LayoutFactory):
        """return_type='id' returns one id per indexed row."""
        layout = make_layout("7t_trt")
        assert len(layout.get(return_type="id")) == len(layout.df)

    def test_file_objects_expose_path(self, make_layout: LayoutFactory):
        """return_type='file' returns BIDSFile objects whose str() is the path."""
        layout = make_layout("7t_trt")
        for f in layout.get(return_type="file")[:5]:
            assert isinstance(f, BIDSFile)
            assert f.path == str(f)

    def test_multi_entity_intersection_counts(self, make_layout: LayoutFactory):
        """Single- and list-valued filters give exact, additive counts."""
        layout = make_layout("7t_trt")
        assert len(layout.get(subject="01", return_type="filename")) == 29
        assert len(layout.get(sub=["01", "02"], return_type="filename")) == 58

    def test_filter_on_absent_value_is_empty(self, make_layout: LayoutFactory):
        """Filtering on a value not present in the dataset returns no files."""
        layout = make_layout("7t_trt")
        assert layout.get(subject="99", return_type="filename") == []


class TestDerivatives:
    """Derivatives plumbing on the synthetic + fmriprep pair."""

    def test_derivatives_as_path(self, make_layout: LayoutFactory):
        """A single Path derivative is appended to the raw layout."""
        layout = make_layout("synthetic")
        raw = len(layout.df)
        deriv = BIDS_EXAMPLES / "synthetic" / "derivatives" / "fmriprep"
        combined = make_layout("synthetic", derivatives=deriv)
        assert len(combined.df) == raw + 150

    def test_derivatives_as_list(self, make_layout: LayoutFactory):
        """A list-valued derivatives argument appends the same files."""
        layout = make_layout("synthetic")
        deriv = BIDS_EXAMPLES / "synthetic" / "derivatives" / "fmriprep"
        combined = make_layout("synthetic", derivatives=[deriv])
        assert len(combined.df) == len(layout.df) + 150

    def test_derivative_rows_distinguishable(self, make_layout: LayoutFactory):
        """Raw and derivative rows carry distinct dataset_type values."""
        deriv = BIDS_EXAMPLES / "synthetic" / "derivatives" / "fmriprep"
        layout = make_layout("synthetic", derivatives=deriv)
        assert {
            "raw",
            "derivative",
        } <= set(layout.df["dataset_type"].dropna().unique())

    def test_nonexistent_derivative_is_skipped(self, make_layout: LayoutFactory):
        """A missing derivative path logs a warning and is skipped, not fatal."""
        layout = make_layout("synthetic", derivatives=BIDS_EXAMPLES / "does_not_exist")
        assert len(layout.df) == 110


class TestIndexHygiene:
    """The indexer selects a BIDS data-file subset, not every on-disk file."""

    def _layout(self, root: Path) -> BIDSLayout:
        with tempfile.TemporaryDirectory() as tmpdir:
            return BIDSLayout(root, cache_path=Path(tmpdir) / "c.parquet")

    def test_minimal_dataset_indexes_only_data_files(self, tmp_path: Path):
        """Only the data file is indexed; sidecar/README/participants are not."""
        ds = tmp_path / "ds"
        (ds / "sub-01" / "anat").mkdir(parents=True)
        (ds / "dataset_description.json").write_text('{"Name": "ds"}')
        (ds / "sub-01" / "anat" / "sub-01_T1w.nii.gz").touch()
        (ds / "sub-01" / "anat" / "sub-01_T1w.json").touch()  # sidecar: excluded
        (ds / "README").write_text("dataset readme")  # extensionless: excluded
        (ds / "participants.tsv").touch()  # no entity prefix: excluded

        layout = self._layout(ds)
        assert layout.df["path"].tolist() == ["sub-01/anat/sub-01_T1w.nii.gz"]

    def test_emptying_datatype_dir_drops_files(
        self, mutable_dataset: DatasetCopyFactory
    ):
        """Removing a subject's T1w files drops them from a re-indexed layout."""
        root = mutable_dataset("ds005")
        before = len(self._layout(root).get(suffix="T1w", return_type="filename"))
        assert before == 16

        for f in (root / "sub-01" / "anat").glob("sub-01_T1w*"):
            f.unlink()

        after = self._layout(root).get(sub="01", suffix="T1w", return_type="filename")
        assert after == []


class TestCacheSurface:
    """Cache / legacy-DB constructor surface (hermetic, no checkout writes)."""

    ROOT = Path(__file__).resolve().parents[2] / "bids-examples" / "7t_trt"

    def test_database_path_alone_is_deprecated(self, tmp_path: Path):
        """Passing only database_path emits a DeprecationWarning (use cache_path)."""
        with pytest.warns(DeprecationWarning, match="cache_path"):
            BIDSLayout(
                self.ROOT,
                database_path=tmp_path / "legacy.db",
                reset_database=True,
            )

    def test_database_path_with_cache_is_not_deprecated(self, tmp_path: Path):
        """Passing both database_path and cache_path is not deprecated."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            BIDSLayout(
                self.ROOT,
                database_path=tmp_path / "legacy.db",
                cache_path=tmp_path / "c.parquet",
            )

    def test_reset_database_reflects_new_files(
        self, mutable_dataset: DatasetCopyFactory, tmp_path: Path
    ):
        """reset_database re-indexes (sees new files) while a stale cache does not."""
        root = mutable_dataset("ds005")
        cp = tmp_path / "c.parquet"
        BIDSLayout(root, cache_path=cp)  # populate a cache
        baseline = len(BIDSLayout(root, cache_path=cp).df)

        newsub = root / "sub-99" / "anat"
        newsub.mkdir(parents=True)
        (newsub / "sub-99_T1w.nii.gz").touch()

        stale = BIDSLayout(root, cache_path=cp)  # trusts the stale cache
        fresh = BIDSLayout(root, cache_path=cp, reset_database=True)  # re-indexes
        assert len(stale.df) == baseline
        assert len(fresh.df) == baseline + 1

    def test_corrupt_cache_falls_back_to_reindex(
        self, mutable_dataset: DatasetCopyFactory, tmp_path: Path
    ):
        """A corrupt cache is caught and the dataset is re-indexed instead."""
        root = mutable_dataset("ds005")
        cp = tmp_path / "c.parquet"
        cp.write_text("this is not a valid parquet file")
        layout = BIDSLayout(root, cache_path=cp)
        assert len(layout.df) > 0


class TestLayoutIdentity:
    """Repr, root, dataset_name, and to_df surface."""

    def test_repr_shape_and_file_count(self, make_layout: LayoutFactory):
        """repr() exposes the root, subject/session counts, and file count."""
        layout = make_layout("7t_trt")
        r = repr(layout)
        assert r.startswith("BIDSLayout(")
        assert "subjects=" in r
        assert "sessions=" in r
        assert "files=635" in r

    def test_root_is_absolute(self, make_layout: LayoutFactory):
        """The resolved root is an absolute path."""
        layout = make_layout("7t_trt")
        assert layout.root.is_absolute()

    def test_dataset_name_populated(self, make_layout: LayoutFactory):
        """The dataset_name column is a non-empty string for every row."""
        layout = make_layout("7t_trt")
        names = layout.df["dataset_name"].dropna().unique().tolist()
        assert names
        assert all(isinstance(n, str) and n for n in names)

    def test_to_df_returns_the_frame(self, make_layout: LayoutFactory):
        """to_df() returns the underlying pandas DataFrame."""
        import pandas as pd

        layout = make_layout("7t_trt")
        assert layout.to_df() is layout.df
        assert isinstance(layout.to_df(), pd.DataFrame)
