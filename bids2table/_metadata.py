import json
from collections.abc import Generator
from functools import lru_cache
from typing import Any

from bids2table._entities import _cache_parse_bids_entities
from bids2table._indexing import _is_bids_dataset
from bids2table._pathlib import PathT, as_path


def load_bids_metadata(path: str | PathT, *, inherit: bool = True) -> dict[str, Any]:
    """Load the full JSON sidecar metadata for a BIDS file.

    Sidecar files are loaded according to the inheritance principle in top-down order.

    Args:
        path: BIDS file path
        inherit: Load the full metadata according to inheritance. Otherwise, load only
            the first JSON sidecar found in the bottom-up search.

    Returns:
        A sidecar metadata dictionary.
    """
    path = as_path(path)
    entities = _cache_parse_bids_entities(path)
    query = dict(entities, ext=".json")

    metadata = {}

    parent = path.parent
    if inherit:
        sidecars = reversed(list(_find_bids_parents(parent, query)))
    else:
        sidecars = [next(_find_bids_parents(parent, query))]

    for path in sidecars:
        try:
            data = _load_json(path)
            metadata.update(data)
        except (json.JSONDecodeError, TypeError):
            continue
    return metadata


@lru_cache
def _load_json(path: PathT) -> Any:  # noqa: ANN401 - match return type of `json.loads`
    """Parse a JSON file into a Python object."""
    return json.loads(path.read_text())


def _find_bids_parents(
    start: PathT,
    query: dict[str, str],
) -> Generator[PathT, None, None]:
    """Find BIDS parent files satisfying the inheritance principle for ``query``.

    A parent's entities must be a subset of ``query`` with matching values;
    results are yielded bottom-up.
    """
    suffix = query.get("suffix")
    ext = query.get("ext")
    if not (suffix or ext):
        raise ValueError("At least one of 'suffix' or 'ext' are required in query.")
    pattern = f"*{suffix}{ext}" if suffix else f"*{ext}"

    parent = start.resolve()
    if not parent.is_dir():
        parent = parent.parent

    while parent.name:
        for path in _glob(parent, pattern):
            entities = _cache_parse_bids_entities(path)
            if _test_bids_inheritance(query, entities):
                yield path
        # Stop climbing if we find a BIDS dataset root.
        # NOTE: This will also stop at a nested dataset. Are there cases where we need
        # to load metadata from the parent dataset?
        if _is_bids_dataset(parent):
            break
        parent = parent.parent


@lru_cache
def _glob(path: PathT, pattern: str) -> list[PathT]:
    """Return paths under ``path`` matching ``pattern`` as a list (cached)."""
    return list(path.glob(pattern))


def _test_bids_inheritance(query: dict[str, str], entities: dict[str, str]) -> bool:
    """Test if entities satisfies the inheritance principle for query."""
    entities = {k: v for k, v in entities.items() if k != "datatype"}
    return set(entities).issubset(query) and all(
        query[k] == v for k, v in entities.items()
    )
