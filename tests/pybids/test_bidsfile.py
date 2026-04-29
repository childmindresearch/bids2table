"""Tests for BIDSFile class."""

from bids2table.pybids import BIDSFile


def test_bidsfile_init():
    """Test BIDSFile initialization."""
    f = BIDSFile("sub-01/anat/sub-01_T1w.nii.gz")
    assert f.path == "sub-01/anat/sub-01_T1w.nii.gz"
    assert f._entities is None  # Not yet parsed


def test_bidsfile_get_entities():
    """Test entity parsing from filename."""
    f = BIDSFile("sub-01_ses-02_task-rest_bold.nii.gz")
    entities = f.get_entities()

    assert isinstance(entities, dict)
    assert entities.get("sub") == "01"
    assert entities.get("ses") == "02"
    assert entities.get("task") == "rest"
    assert entities.get("suffix") == "bold"


def test_bidsfile_entities_cached():
    """Test that entities are cached after first parse."""
    f = BIDSFile("sub-01_T1w.nii.gz")

    # First call
    entities1 = f.get_entities()
    assert f._entities is not None

    # Second call should return cached version
    entities2 = f.get_entities()
    assert entities1 is entities2  # Same object


def test_bidsfile_str():
    """Test string representation."""
    f = BIDSFile("sub-01/anat/sub-01_T1w.nii.gz")
    assert str(f) == "sub-01/anat/sub-01_T1w.nii.gz"


def test_bidsfile_repr():
    """Test developer representation."""
    f = BIDSFile("sub-01/anat/sub-01_T1w.nii.gz")
    assert repr(f) == "BIDSFile('sub-01/anat/sub-01_T1w.nii.gz')"


def test_bidsfile_equality():
    """Test equality comparison."""
    f1 = BIDSFile("sub-01_T1w.nii.gz")
    f2 = BIDSFile("sub-01_T1w.nii.gz")
    f3 = BIDSFile("sub-02_T1w.nii.gz")

    assert f1 == f2
    assert f1 != f3
    assert f1 != "sub-01_T1w.nii.gz"  # Not equal to string


def test_bidsfile_hashable():
    """Test that BIDSFile can be used in sets/dicts."""
    f1 = BIDSFile("sub-01_T1w.nii.gz")
    f2 = BIDSFile("sub-01_T1w.nii.gz")
    f3 = BIDSFile("sub-02_T1w.nii.gz")

    # Can create set
    file_set = {f1, f2, f3}
    assert len(file_set) == 2  # f1 and f2 are equal

    # Can use as dict key
    file_dict = {f1: "first", f3: "third"}
    assert file_dict[f2] == "first"  # f2 equals f1
