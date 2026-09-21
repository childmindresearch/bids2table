"""Tests for BIDSFile class."""

import pytest

pandas = pytest.importorskip("pandas", reason="pandas not available")

from bids2table.pybids import BIDSFile  # noqa: E402 - skip tests if pandas not avail


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


# ---------------------------------------------------------------------------
# Green lifts: exact entity dicts and full comparison/surface behavior.
# NOTE: our parser also yields the ``datatype`` entity (a bidsschematools
# convention), so these expected dicts document *our* semantics, not upstream
# pybids'. The degenerate `.DS_Store` case is a known gap (see
# test_compat_gaps.py, B3) and is intentionally NOT asserted here.
# ---------------------------------------------------------------------------


def test_bidsfile_canonical_entities():
    """Exact entity dict for a canonical anatomical file."""
    f = BIDSFile("sub-01/anat/sub-01_T1w.nii.gz")
    assert f.get_entities() == {
        "sub": "01",
        "datatype": "anat",
        "suffix": "T1w",
        "ext": ".nii.gz",
    }


def test_bidsfile_dwi_with_run_entity():
    """A DWI file with a run entity parses all four fields."""
    f = BIDSFile("sub-01/dwi/sub-01_run-1_dwi.nii.gz")
    assert f.get_entities() == {
        "sub": "01",
        "run": "1",
        "datatype": "dwi",
        "suffix": "dwi",
        "ext": ".nii.gz",
    }


def test_bidsfile_single_nii_extension():
    """A single-dot ``.nii`` filename parses its extension as ``.nii``."""
    f = BIDSFile("sub-01/anat/sub-01_T1w.nii")
    assert f.get_entities()["ext"] == ".nii"


def test_bidsfile_desc_is_entity_not_suffix():
    """``desc-`` is a file entity; the true suffix is the trailing token."""
    f = BIDSFile("sub-01/anat/sub-01_desc-T1w_mask.nii.gz")
    e = f.get_entities()
    assert e["desc"] == "T1w"
    assert e["suffix"] == "mask"


def test_bidsfile_bare_suffix_only():
    """A bare-suffix filename has no entities, datatype, or directory."""
    f = BIDSFile("T1w.nii.gz")
    assert f.get_entities() == {"suffix": "T1w", "ext": ".nii.gz"}


def test_bidsfile_ordering_comparisons():
    """Total-order comparisons work; cross-type comparisons are NotImplemented."""
    a = BIDSFile("sub-01_T1w.nii.gz")
    b = BIDSFile("sub-02_T1w.nii.gz")
    assert a < b
    assert b > a
    assert a <= a
    assert a >= a
    assert a <= b
    # Comparing to a non-BIDSFile yields NotImplemented (delegated to the other side).
    assert a.__lt__("not-a-bidsfile") is NotImplemented
    assert a.__gt__("not-a-bidsfile") is NotImplemented


def test_bidsfile_contains_is_substring():
    """``in`` performs a substring test on the path."""
    f = BIDSFile("sub-01/anat/sub-01_T1w.nii.gz")
    assert "sub-01" in f
    assert "anat" in f
    assert "zzz" not in f


def test_listify():
    """listify: None -> [], list/tuple -> list, else wrapped in a list."""
    from bids2table.pybids import listify

    assert listify(None) == []
    assert listify("a") == ["a"]
    assert listify(["a", "b"]) == ["a", "b"]
    assert listify((1, 2)) == [1, 2]
    assert listify(5) == [5]
