"""Dataset-wide green invariants on the workhorse datasets.

Every test here is parametrized over ``DATASETS`` (defined in ``conftest.py``),
so broadening coverage is a one-line list edit. The tests pin two things:

1. The exact indexed file count per dataset (see ``_EXPECTED_INDEXED_COUNTS``),
   and
2. Structural invariants that hold regardless of count:
   * no extensionless file is indexed,
   * no dotfile is indexed,
   * every indexed filename parses to a ``suffix`` and an ``ext``, and
   * no *sidecar* JSON is indexed (a ``.json`` sharing a stem with an indexed
     data file in the same directory).

Counting note: the indexer selects a BIDS **data-file subset** (has an
extension, a leading entity prefix, and a suffix+ext; ``.json`` sidecars are
excluded), **not** every on-disk file. The counts below were re-derived from
the current ``bids-examples@1.9.0`` checkout (e.g. ``7t_trt`` has more files on
disk than it indexes). See the selection rule in ``bids2table/_indexing.py``.
"""

from pathlib import Path

import pytest

pytest.importorskip("pandas", reason="pandas not available")

from tests.pybids.conftest import DATASETS, LayoutFactory

# Ground-truth indexed counts, re-derived from the current checkout. This is the
# single source of truth for the data-file-subset count (not a per-test literal).
_EXPECTED_INDEXED_COUNTS = {
    "7t_trt": 635,
    "ds005": 128,
    "ds000117": 673,
    "synthetic": 110,
}

# Only index a dataset when it is part of the current DATASETS list (conftest),
# so the parametrization and the count table stay in lockstep.
_PARAMS = [name for name in DATASETS if name in _EXPECTED_INDEXED_COUNTS]


@pytest.mark.parametrize("name", _PARAMS)
def test_indexed_count_matches_ground_truth(
    name: str,
    make_layout: LayoutFactory,
):
    """Indexing a dataset yields exactly its re-derived data-file count."""
    assert len(make_layout(name).df) == _EXPECTED_INDEXED_COUNTS[name]


@pytest.mark.parametrize("name", _PARAMS)
def test_no_extensionless_file_indexed(
    name: str,
    make_layout: LayoutFactory,
):
    """Every indexed file has a non-empty extension."""
    extless = [p for p in make_layout(name).df["path"] if Path(p).suffix == ""]
    assert extless == []


@pytest.mark.parametrize("name", _PARAMS)
def test_no_dotfile_indexed(
    name: str,
    make_layout: LayoutFactory,
):
    """No indexed path contains a dotfile component."""
    dots = [
        p
        for p in make_layout(name).df["path"]
        if any(part.startswith(".") for part in Path(p).parts)
    ]
    assert dots == []


@pytest.mark.parametrize("name", _PARAMS)
def test_every_indexed_file_parses_to_suffix_and_ext(
    name: str,
    make_layout: LayoutFactory,
):
    """The indexer only keeps files that resolve to both a suffix and an ext."""
    from bids2table._entities import parse_bids_entities

    layout = make_layout(name)
    for p in layout.df["path"]:
        entities = parse_bids_entities(str(layout.root / p))
        assert entities.get("suffix") is not None, f"no suffix: {p}"
        assert entities.get("ext") is not None, f"no ext: {p}"


@pytest.mark.parametrize("name", _PARAMS)
def test_no_sidecar_json_indexed(
    name: str,
    make_layout: LayoutFactory,
):
    """A ``.json`` sharing a stem with an indexed data file is a sidecar.

    Legitimate JSON data files (e.g. ``*_coordsystem.json`` in ds000117) have no
    same-stem sibling and are therefore allowed; true sidecars are not.
    """
    indexed = {Path(p) for p in make_layout(name).df["path"]}
    sidecars = []
    for p in indexed:
        if p.suffix != ".json":
            continue
        if any(
            s.parent == p.parent and s.stem == p.stem and s.suffix != ".json"
            for s in indexed
        ):
            sidecars.append(str(p))
    assert sidecars == [], f"sidecar JSON files were indexed: {sidecars}"
