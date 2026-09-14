"""Compatibility-gap pins: where the wrapper diverges from upstream pybids.

Each xfail is written to the upstream pybids contract with ``strict=True``:
it fails for the reason in its marker, and a strict XPASS forces the marker
off once the gap is closed. The green tests pin intended divergences (they
should keep passing, not be "fixed").

Gap registry (test — upstream source — fix). Upstream source is pybids
test_layout.py unless otherwise noted:

B1a  test_get_run_accepts_string_value — test_get_with_wrong_dtypes
     fix: coerce numeric entity values in _apply_filter
B1b  test_get_ses_accepts_int_value — test_get_with_wrong_dtypes (session half)
     fix: as B1a (separate column, so neither masks the other)
B2   test_get_results_are_naturally_sorted — test_get_return_sorted
     fix: natural-sort results in _format_results
B3   test_bidsfile_degenerate_paths_parse_empty
     — test_utils.py::test_parse_degenerate_files; fix: return {} for dotfiles / no-ext
B4   test_query_required_sentinel — test_get_val_enum_any_optional
     fix: add Query.REQUIRED + a notna() branch
B5a  test_run_match_list_with_query_constants
     — test_get_with_query_constants_in_match_list; fix: hoist sentinels before isin
B5b  test_acq_match_list_with_query_constants
     — test_get_non_run_entity_with_query_constants_in_match_list; fix: as B5a
B6a  test_get_with_regex_search — test_get_with_regex_search
     fix: treat regex_search as a mode flag; re.search each filter value
B6b  test_get_with_regex_search_numeric_entity
     — test_get_with_regex_search_bad_dtype; fix: as B6a
B7   test_validate_drops_invalid_files
     — test_validation.py::test_layout_with_validation; fix: drop failing files
B8   test_get_file_missing_returns_none — test_get_file ("No such file" half)
     fix: return None when the path is not in the index
B9   test_target_kwarg_is_warned_and_ignored — GREEN, intended divergence:
     warn on target= and still apply the other filters
"""

import re
from pathlib import Path

import pytest

pytest.importorskip("pandas", reason="pandas not available")

from bids2table.pybids import (
    BIDSFile,
    BIDSLayout,
    Query,
)
from tests.pybids.conftest import LayoutFactory


def _natural_sort_key(value: str) -> list[str | int]:
    """Natural-sort key: split on digit runs (pybids ``natural_sort``)."""
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", value)]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B1: 'run' is int32[pyarrow] and _apply_filter compares with a bare ==/isin, "
        "so run='1' / run='01' match 0 rows (run=1 matches 219). "
        "Fix: coerce numeric entity values in _apply_filter."
    ),
)
def test_get_run_accepts_string_value(make_layout: LayoutFactory) -> None:
    """B1a: 'run' accepts int and string values alike.

    Upstream: pybids test_layout.py::test_get_with_wrong_dtypes.
    """
    layout = make_layout("7t_trt")
    assert len(layout.get(run=1)) == 219
    assert len(layout.get(run="1")) == 219
    assert len(layout.get(run="01")) == 219


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B1: 'ses' is string[pyarrow] and _apply_filter compares with a bare ==, "
        "so ses=1 (int) matches 0 rows (ses='1' matches 327). "
        "Fix: coerce entity values in _apply_filter (separate column from B1a's run)."
    ),
)
def test_get_ses_accepts_int_value(make_layout: LayoutFactory) -> None:
    """B1b: 'ses' accepts int and string values alike.

    Upstream: pybids test_layout.py::test_get_with_wrong_dtypes (session half).
    """
    layout = make_layout("7t_trt")
    assert len(layout.get(ses="1")) == 327
    assert len(layout.get(ses=1)) == 327


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B2: get() returns rows in index-walk order, not sorted (only "
        "return_type='dir' sorts). Fix: natural-sort results in _format_results."
    ),
)
def test_get_results_are_naturally_sorted(make_layout: LayoutFactory) -> None:
    """B2: results come back in natural-sorted order.

    Upstream: pybids test_layout.py::test_get_return_sorted (minus target=,
    which is pinned separately as B9).
    """
    layout = make_layout("7t_trt")
    paths = [str(p) for p in layout.get(return_type="filename")]
    assert paths == sorted(paths, key=_natural_sort_key)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B3: BIDSFile parsing keeps a suffix for degenerate names — "
        "'.DS_Store' -> {'suffix': 'Store'}, 'stub' -> {'suffix': 'stub'} — "
        "where pybids returns {}. Fix: return {} for dotfiles / extensionless names."
    ),
)
@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (".DS_Store", {}),
        ("/path/to/.dotfile", {}),
        ("/path/to/stub", {}),
    ],
)
def test_bidsfile_degenerate_paths_parse_empty(
    path: str,
    expected: dict[str, str],
) -> None:
    """B3: dotfiles and extensionless names parse to no entities.

    Upstream: pybids test_utils.py::test_parse_degenerate_files.
    """
    assert BIDSFile(path).get_entities() == expected


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B4: Query has no REQUIRED member (AttributeError), so this "
        "'entity must be present' filter cannot be expressed. "
        "Fix: add Query.REQUIRED and a notna() branch in _apply_filter."
    ),
)
def test_query_required_sentinel(make_layout: LayoutFactory) -> None:
    """B4: Query.REQUIRED selects rows where the entity is present.

    Upstream: pybids test_layout.py::test_get_val_enum_any_optional.
    ds005 has no session entity, so the result must be empty.
    """
    layout = make_layout("ds005")
    files = layout.get(
        subject="01",
        run=1,
        suffix="bold",
        ses=Query.REQUIRED,  # ty: ignore[unresolved-attribute] -- the B4 gap
    )
    assert files == []


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B5: Query sentinels inside a list reach pandas isin() unhoisted — "
        "pyarrow cannot convert Query to an Arrow type (ArrowInvalid). "
        "Fix: hoist NONE/ANY out of list values before filtering."
    ),
)
def test_run_match_list_with_query_constants(make_layout: LayoutFactory) -> None:
    """B5a: a list containing Query sentinels matches the union of matches.

    Upstream: pybids test_layout.py::test_get_with_query_constants_in_match_list.
    7t_trt: run=1 -> 219, run=NONE -> 197, ANY -> all 635.
    """
    layout = make_layout("7t_trt")
    get1 = layout.get(run=1)
    get_none = layout.get(run=Query.NONE)
    get_any = layout.get(run=Query.ANY)
    get1_and_any = layout.get(
        run=[Query.ANY, 1],  # ty: ignore[invalid-argument-type] -- the B5 gap
    )
    get_none_and_any = layout.get(
        run=[Query.ANY, Query.NONE],  # ty: ignore[invalid-argument-type] -- the B5 gap
    )
    assert set(get1_and_any) == set(get1) | set(get_any)
    assert set(get_none_and_any) == set(get_none) | set(get_any)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B5: Query sentinels inside a list reach pandas isin() unhoisted — "
        "pyarrow cannot convert Query to an Arrow type (ArrowInvalid). "
        "Fix: same as B5a (non-run entity variant)."
    ),
)
def test_acq_match_list_with_query_constants(make_layout: LayoutFactory) -> None:
    """B5b: sentinel-in-list union semantics on a non-run entity.

    Upstream: pybids test_layout.py::
    test_get_non_run_entity_with_query_constants_in_match_list.
    7t_trt: acq=fullbrain -> 175, acq=NONE -> 373, ANY -> all 635.
    """
    layout = make_layout("7t_trt")
    get1 = layout.get(acq="fullbrain")
    get_none = layout.get(acq=Query.NONE)
    get_any = layout.get(acq=Query.ANY)
    get1_and_any = layout.get(
        acq=[Query.ANY, "fullbrain"],  # ty: ignore[invalid-argument-type] -- the B5 gap
    )
    get_none_and_any = layout.get(
        acq=[Query.ANY, Query.NONE],  # ty: ignore[invalid-argument-type] -- the B5 gap
    )
    assert set(get1_and_any) == set(get1) | set(get_any)
    assert set(get_none_and_any) == set(get_none) | set(get_any)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B6: regex_search=True is swallowed as an unknown entity (warn + drop), "
        "so filters stay literal: subject='1' matches no subject -> 0 files "
        "(pybids: '1' as a regex hits sub-01 and sub-10 -> 2). "
        "Fix: treat regex_search as a mode flag and re.search each filter."
    ),
)
def test_get_with_regex_search(make_layout: LayoutFactory) -> None:
    """B6a: with regex_search, filter values are regular expressions.

    Upstream: pybids test_layout.py::test_get_with_regex_search.
    'fron.al' matches acquisition 'prefrontal'; '^1' matches sub-10 only.
    """
    layout = make_layout("7t_trt")
    results = layout.get(
        subject="1",
        session="1",
        task="rest",
        suffix="bold",
        acquisition="fron.al",
        extension=".nii.gz",
        regex_search=True,
    )
    assert len(results) == 2

    results = layout.get(
        subject="^1",
        session="1",
        task="rest",
        suffix="bold",
        acquisition="fron.al",
        extension=".nii.gz",
        regex_search=True,
        return_type="filename",
    )
    assert len(results) == 1
    assert str(results[0]).endswith("sub-10_ses-1_task-rest_acq-prefrontal_bold.nii.gz")


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B6: regex_search=True is swallowed as an unknown entity, so subject='1' "
        "is compared literally and matches nothing (0 files; pybids: 4). "
        "Fix: same as B6a."
    ),
)
def test_get_with_regex_search_numeric_entity(make_layout: LayoutFactory) -> None:
    """B6b: numeric entities work under regex search (implicit coercion).

    Upstream: pybids test_layout.py::test_get_with_regex_search_bad_dtype.
    subject='1' (regex) with run=1 (int) -> 4 bold files.
    """
    layout = make_layout("7t_trt")
    results = layout.get(
        subject="1",
        run=1,
        task="rest",
        suffix="bold",
        acquisition="fullbrain",
        extension=".nii.gz",
        regex_search=True,
    )
    assert len(results) == 4


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B7: validate=True is accepted in **kwargs but never used — the layout "
        "is identical to validate=False, so an invalid-suffix file "
        "(sub-01_foobar.nii.gz) stays indexed. "
        "Fix: run a validation pass and drop failing files when validate=True."
    ),
)
def test_validate_drops_invalid_files(tmp_path: Path) -> None:
    """B7: validate=True drops files a BIDS validator would reject.

    Upstream: pybids test_validation.py::test_layout_with_validation.
    'foobar' is not a valid BIDS suffix.
    """
    root = tmp_path / "ds"
    (root / "sub-01" / "anat").mkdir(parents=True)
    (root / "dataset_description.json").write_text(
        '{"Name": "min", "BIDSVersion": "1.9.0"}'
    )
    (root / "sub-01" / "anat" / "sub-01_T1w.nii.gz").touch()
    bad = "sub-01/anat/sub-01_foobar.nii.gz"
    (root / bad).touch()
    unvalidated = BIDSLayout(root, cache_path=tmp_path / "unvalidated.parquet")
    validated = BIDSLayout(
        root, cache_path=tmp_path / "validated.parquet", validate=True
    )
    assert bad in unvalidated.get(return_type="filename")
    assert bad not in validated.get(return_type="filename")


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B8: get_file() returns a BIDSFile for any path without checking "
        "membership, so a missing path is truthy instead of None. "
        "Fix: return None when the path is not in the index."
    ),
)
def test_get_file_missing_returns_none(make_layout: LayoutFactory) -> None:
    """B8: a missing path yields None (pybids contract).

    Upstream: pybids test_layout.py::test_get_file ("No such file" half).
    """
    layout = make_layout("7t_trt")
    missing = "sub-01/anat/sub-01_NOPE_T1w.nii.gz"
    assert layout.get_file(missing) is None


def test_get_file_existing_returns_bidsfile(make_layout: LayoutFactory) -> None:
    """Positive half of pybids test_get_file: an indexed path resolves.

    Green on purpose — the missing-path half is pinned as gap B8 above.
    """
    layout = make_layout("7t_trt")
    indexed = "sub-01/ses-1/anat/sub-01_ses-1_T1w.nii.gz"
    assert indexed in layout.get(return_type="filename")  # precondition
    f = layout.get_file(indexed)
    assert f is not None
    assert str(f) == indexed


def test_target_kwarg_is_warned_and_ignored(make_layout: LayoutFactory) -> None:
    """B9 (intended divergence): target= warns and the other filters apply.

    Upstream test_get_return_sorted passes target='subject' silently; the
    wrapper has no such feature, so we pin the documented behavior.
    """
    layout = make_layout("7t_trt")
    with pytest.warns(UserWarning, match="target"):
        files = layout.get(target="subject", subject="01")
    assert len(files) == len(layout.get(subject="01"))
