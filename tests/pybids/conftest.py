"""Shared fixtures for the pybids compatibility tests.

These fixtures let the test modules reference the "workhorse" datasets and
build hermetic ``BIDSLayout`` instances without polluting the ``bids-examples``
checkout (every layout uses a throwaway parquet cache, and every mutation test
operates on a private copy).

``DATASETS`` is the single knob to broaden coverage: appending a dataset name
here (once its expected values are re-derived) widens the parametrized tests in
``test_layout_on_workhorses.py`` and any test that iterates the list.
"""

import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from bids2table.pybids import BIDSLayout

# Factory fixture types shared by the test modules (avoids per-file aliases).
LayoutFactory = Callable[..., "BIDSLayout"]
DatasetCopyFactory = Callable[[str], Path]

# The datasets we pin concrete pybids-API behavior against for now. Extend to
# the full bids-examples corpus later (see .notes/pybids-tests/overview.md).
DATASETS = ["7t_trt", "ds005", "ds000117", "synthetic"]

# Root of the bids-examples submodule used as test fixtures.
BIDS_EXAMPLES = Path(__file__).resolve().parents[2] / "bids-examples"


def dataset_root(name: str) -> Path:
    """Resolve a workhorse dataset name to its ``bids-examples`` path."""
    return BIDS_EXAMPLES / name


def _require_dataset(name: str) -> Path:
    """Return the dataset path, skipping the test if the checkout is missing."""
    path = dataset_root(name)
    if not path.exists():
        pytest.skip(f"BIDS dataset not found: {path}")
    return path


@pytest.fixture(scope="session")
def make_layout():
    """Factory fixture building a hermetic ``BIDSLayout`` for a dataset name.

    Each layout gets a throwaway parquet cache so the dataset checkout is never
    written. Layouts are memoized per ``(name, kwargs)`` for the session, so a
    dataset is indexed once rather than once per test (e.g. ``7t_trt`` has 635
    files). ``BIDSLayout`` is read-only after construction, so sharing an
    instance across tests is safe.

    Example:
        >>> layout = make_layout("7t_trt")
        >>> layout.get(suffix="bold", return_type="filename")
    """
    pytest.importorskip("pandas", reason="pandas not available")
    from bids2table.pybids import BIDSLayout

    memo: dict[tuple[str, str], tuple[tempfile.TemporaryDirectory, BIDSLayout]] = {}

    def _make(
        name: str,
        derivatives: str | Path | list[str | Path] | None = None,
    ) -> BIDSLayout:
        # Canonical, always-hashable key (derivatives may be a list of Paths).
        key = (name, repr(derivatives))
        if key not in memo:
            tmp = tempfile.TemporaryDirectory()
            layout = BIDSLayout(
                _require_dataset(name),
                cache_path=Path(tmp.name) / f"{name}.parquet",
                derivatives=derivatives,
            )
            memo[key] = (tmp, layout)
        return memo[key][1]

    return _make


@pytest.fixture
def mutable_dataset(tmp_path: Path):
    """Factory fixture that copies a workhorse dataset into ``tmp_path``.

    Returns the copied root, so tests can add/remove files and re-index without
    touching the shared ``bids-examples`` checkout.

    Example:
        >>> root = mutable_dataset("ds005")
        >>> (root / "sub-01").unlink()  # mutate the copy
    """
    import shutil

    def _copy(name: str) -> Path:
        src = _require_dataset(name)
        dst = tmp_path / name
        shutil.copytree(src, dst)
        return dst

    return _copy
