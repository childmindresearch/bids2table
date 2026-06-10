"""Tests for custom entity functionality."""

import tempfile
from pathlib import Path

import pytest

pandas = pytest.importorskip("pandas", reason="pandas not available")

from bids2table.pybids import BIDSLayout  # noqa: E402 - skip tests if pandas not avail


@pytest.fixture
def test_dataset():
    """Return path to a test BIDS dataset."""
    dataset_path = Path(__file__).parents[2] / "bids-examples" / "ds001"
    if not dataset_path.exists():
        pytest.skip(f"Test dataset not found: {dataset_path}")
    return dataset_path


@pytest.fixture
def layout(test_dataset):
    """Create a BIDSLayout for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = Path(tmpdir) / "test_cache.parquet"
        yield BIDSLayout(test_dataset, validate=False, cache_path=cache_path)


class TestCustomEntities:
    """Tests for adding and querying custom entities."""

    def test_add_constant_value(self, layout):
        """Test adding a constant value to all rows."""
        layout.add_custom_entity("status", "pending")

        assert "status" in layout.df.columns
        assert (layout.df["status"] == "pending").all()

        # Query with custom entity
        files = layout.get(status="pending", return_type="filename")
        assert len(files) == len(layout.df)

    def test_add_from_dict_subjects(self, layout):
        """Test adding entity from subject mapping."""
        subjects = layout.get_subjects()
        if not subjects:
            pytest.skip("No subjects in dataset")

        # Create mapping for first few subjects
        qc_mapping = {subjects[0]: "pass", subjects[1]: "fail"}
        layout.add_custom_entity("qc_grade", qc_mapping)

        assert "qc_grade" in layout.df.columns

        # Check values
        sub01_rows = layout.df[layout.df["sub"] == subjects[0]]
        assert (sub01_rows["qc_grade"] == "pass").all()

        # Query with custom entity
        passed = layout.get(qc_grade="pass", return_type="filename")
        failed = layout.get(qc_grade="fail", return_type="filename")

        assert len(passed) > 0
        assert len(failed) > 0

    def test_add_from_function(self, layout):
        """Test adding entity computed from function."""

        def categorize(row):
            if row["suffix"] in ["T1w", "T2w", "inplaneT2"]:
                return "anatomical"
            elif row["suffix"] == "bold":
                return "functional"
            else:
                return "other"

        layout.add_custom_entity("modality_category", categorize)

        assert "modality_category" in layout.df.columns

        # Query with custom entity
        anat = layout.get(modality_category="anatomical", return_type="filename")
        func = layout.get(modality_category="functional", return_type="filename")

        assert len(anat) > 0
        assert len(func) > 0

    def test_add_from_list(self, layout):
        """Test adding entity from list of values."""
        # Create list matching number of files
        n_files = len(layout.df)
        batch_ids = [i % 3 for i in range(n_files)]  # Batches 0, 1, 2

        layout.add_custom_entity("batch_id", batch_ids)

        assert "batch_id" in layout.df.columns

        # Query with custom entity
        batch0 = layout.get(batch_id=0, return_type="filename")
        assert len(batch0) > 0

    def test_combine_standard_and_custom(self, layout):
        """Test querying with both standard and custom entities."""
        subjects = layout.get_subjects()
        if not subjects:
            pytest.skip("No subjects in dataset")

        # Add custom entity
        layout.df["processing_group"] = layout.df["sub"].apply(
            lambda x: "group_a" if int(x) % 2 == 0 else "group_b"
        )

        # Query with both standard and custom
        files = layout.get(
            subject=subjects[0],
            processing_group="group_a" if int(subjects[0]) % 2 == 0 else "group_b",
            return_type="filename",
        )

        assert len(files) > 0

    def test_overwrite_protection(self, layout):
        """Test that overwrite protection works."""
        layout.add_custom_entity("my_entity", "value1")

        # Try to add again without overwrite
        with pytest.raises(ValueError, match="already exists"):
            layout.add_custom_entity("my_entity", "value2")

        # Should work with overwrite=True
        layout.add_custom_entity("my_entity", "value2", overwrite=True)
        assert (layout.df["my_entity"] == "value2").all()

    def test_direct_dataframe_manipulation(self, layout):
        """Test that direct df manipulation also works for querying."""
        # Add custom entity directly (without helper method)
        layout.df["direct_entity"] = "direct_value"

        # Should be queryable
        files = layout.get(direct_entity="direct_value", return_type="filename")
        assert len(files) == len(layout.df)

    def test_modify_existing_entity(self, layout):
        """Test modifying an existing entity value."""
        # Modify a standard entity
        original_tasks = layout.df["task"].unique()

        # Recode task names
        task_mapping = {"balloonanalogrisktask": "BART"}
        layout.df["task"] = layout.df["task"].replace(task_mapping)

        # Query with new value
        bart_files = layout.get(task="BART", return_type="filename")

        # Should have files if original had balloonanalogrisktask
        if "balloonanalogrisktask" in original_tasks:
            assert len(bart_files) > 0

    def test_entity_with_subject_filter(self, layout):
        """Test custom entity combined with get_subjects filter."""
        # Add custom entity
        layout.df["experiment_phase"] = layout.df["run"].apply(
            lambda x: "early" if x == "01" else "late" if x else None
        )

        # Get subjects that have early phase files
        subjects = layout.get_subjects(experiment_phase="early")
        assert isinstance(subjects, list)

    def test_none_values_in_custom_entity(self, layout):
        """Test handling of None/NaN in custom entities."""

        def sometimes_none(row):
            # Only set value for BOLD files
            return "has_value" if row["suffix"] == "bold" else None

        layout.add_custom_entity("optional_entity", sometimes_none)

        # Query should only match non-None values
        files = layout.get(optional_entity="has_value", return_type="filename")
        assert all("bold" in f for f in files)
