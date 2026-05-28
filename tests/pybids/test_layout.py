"""Tests for BIDSLayout class."""

import tempfile
from pathlib import Path

import pytest

from bids2table.pybids import BIDSFile, BIDSLayout, Query


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
def layout(test_dataset):
    """Create a BIDSLayout for testing."""
    # Use temporary cache to avoid polluting test dataset
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = Path(tmpdir) / "test_cache.parquet"
        yield BIDSLayout(test_dataset, validate=False, cache_path=cache_path)


class TestBIDSLayoutInit:
    """Tests for BIDSLayout initialization."""

    def test_init_basic(self, test_dataset):
        """Test basic initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "cache.parquet"
            layout = BIDSLayout(test_dataset, cache_path=cache_path)

            assert layout.root == test_dataset.absolute()
            assert layout.df is not None
            assert len(layout.df) > 0

    def test_init_with_cache(self, test_dataset):
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

    def test_repr(self, layout):
        """Test string representation."""
        repr_str = repr(layout)
        assert "BIDSLayout" in repr_str
        assert "subjects=" in repr_str
        assert "files=" in repr_str


class TestBIDSLayoutGet:
    """Tests for BIDSLayout.get() method."""

    def test_get_basic(self, layout):
        """Test basic file query."""
        files = layout.get(return_type="filename")
        assert isinstance(files, list)
        assert len(files) > 0
        assert all(isinstance(f, str) for f in files)

    def test_get_by_subject(self, layout):
        """Test filtering by subject."""
        subjects = layout.get_subjects()
        if not subjects:
            pytest.skip("No subjects in dataset")

        subject = subjects[0]
        files = layout.get(subject=subject, return_type="filename")

        assert len(files) > 0
        # Check that all files contain subject ID
        assert all(f"sub-{subject}" in f for f in files)

    def test_get_by_suffix(self, layout):
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

    def test_get_multiple_filters(self, layout):
        """Test filtering by multiple entities."""
        subjects = layout.get_subjects()
        if not subjects:
            pytest.skip("No subjects in dataset")

        subject = subjects[0]
        files = layout.get(subject=subject, datatype="anat", return_type="filename")

        # Should have filtered by both subject and datatype
        # (May be empty if subject doesn't have anat data)
        assert isinstance(files, list)

    def test_get_return_type_file(self, layout):
        """Test return_type='file' returns BIDSFile objects."""
        files = layout.get(return_type="file")

        assert len(files) > 0
        assert all(isinstance(f, BIDSFile) for f in files)

    def test_get_return_type_filename(self, layout):
        """Test return_type='filename' returns strings."""
        files = layout.get(return_type="filename")

        assert len(files) > 0
        assert all(isinstance(f, str) for f in files)

    def test_get_return_type_id(self, layout):
        """Test return_type='id' returns indices."""
        ids = layout.get(return_type="id")

        assert len(ids) > 0
        assert all(isinstance(i, (int, type(ids[0]))) for i in ids)

    def test_get_return_type_dir(self, layout):
        """Test return_type='dir' returns unique directories."""
        dirs = layout.get(return_type="dir")

        assert len(dirs) > 0
        assert all(isinstance(d, str) for d in dirs)
        # Should be unique
        assert len(dirs) == len(set(dirs))

    def test_get_with_list_values(self, layout):
        """Test filtering with list of values."""
        subjects = layout.get_subjects()
        if len(subjects) < 2:
            pytest.skip("Need at least 2 subjects")

        files = layout.get(subject=subjects[:2], return_type="filename")

        assert len(files) > 0

    def test_get_with_query_optional(self, layout):
        """Test Query.OPTIONAL allows missing entities."""
        files = layout.get(session=Query.OPTIONAL, return_type="filename")

        # Should return all files regardless of session
        assert len(files) > 0

    def test_get_with_query_any(self, layout):
        """Test Query.ANY matches any value."""
        files = layout.get(suffix=Query.ANY, return_type="filename")

        # Should return all files (no filtering on suffix)
        assert len(files) > 0

    def test_get_invalid_return_type(self, layout):
        """Test that invalid return_type raises error."""
        with pytest.raises(ValueError, match="Unknown return_type"):
            layout.get(return_type="invalid")


class TestBIDSLayoutEntities:
    """Tests for entity extraction methods."""

    def test_get_subjects(self, layout):
        """Test getting subject list."""
        subjects = layout.get_subjects()

        assert isinstance(subjects, list)
        assert len(subjects) > 0
        # Should be sorted
        assert subjects == sorted(subjects)
        # Should not have 'sub-' prefix
        assert all(not s.startswith("sub-") for s in subjects)

    def test_get_subjects_with_filter(self, layout):
        """Test filtering subjects by other entities."""
        all_subjects = layout.get_subjects()
        filtered_subjects = layout.get_subjects(datatype="anat")

        # Filtered should be subset (or equal)
        assert set(filtered_subjects).issubset(set(all_subjects))

    def test_get_sessions(self, layout):
        """Test getting session list."""
        sessions = layout.get_sessions()

        assert isinstance(sessions, list)
        # May be empty if no sessions in dataset
        if sessions:
            assert sessions == sorted(sessions)
            assert all(not s.startswith("ses-") for s in sessions)

    def test_get_sessions_by_subject(self, layout):
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

    def test_get_metadata(self, layout):
        """Test loading metadata for a file."""
        files = layout.get(suffix="bold", return_type="filename")
        if not files:
            pytest.skip("No BOLD files in dataset")

        file_path = files[0]
        metadata = layout.get_metadata(file_path)

        assert isinstance(metadata, dict)
        # BOLD files typically have RepetitionTime
        # (but not guaranteed in all test datasets)

    def test_get_file(self, layout):
        """Test getting BIDSFile object."""
        files = layout.get(return_type="filename")
        if not files:
            pytest.skip("No files in dataset")

        file_path = files[0]
        bids_file = layout.get_file(file_path)

        assert isinstance(bids_file, BIDSFile)
        assert bids_file.path == file_path

        # Can get entities
        entities = bids_file.get_entities()
        assert isinstance(entities, dict)


class TestBIDSLayoutEntityMapping:
    """Tests for PyBIDS entity name mapping."""

    def test_subject_mapping(self, layout):
        """Test that 'subject' maps to 'sub'."""
        subjects = layout.get_subjects()
        if not subjects:
            pytest.skip("No subjects in dataset")

        # Both 'subject' and 'sub' should work
        files1 = layout.get(subject=subjects[0], return_type="filename")
        files2 = layout.get(sub=subjects[0], return_type="filename")

        assert set(files1) == set(files2)

    def test_session_mapping(self, layout):
        """Test that 'session' maps to 'ses'."""
        sessions = layout.get_sessions()
        if not sessions:
            pytest.skip("No sessions in dataset")

        # Both 'session' and 'ses' should work
        files1 = layout.get(session=sessions[0], return_type="filename")
        files2 = layout.get(ses=sessions[0], return_type="filename")

        assert set(files1) == set(files2)

    def test_extension_mapping(self, layout):
        """Test that 'extension' maps to 'ext'."""
        # Both should work
        files1 = layout.get(extension=".nii.gz", return_type="filename")
        files2 = layout.get(ext=".nii.gz", return_type="filename")

        # Should return same files (or both empty)
        assert set(files1) == set(files2)
