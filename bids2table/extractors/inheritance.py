from pathlib import Path
from typing import Dict, Generator, Optional

from elbow.typing import StrOrPath

from bids2table.entities import parse_bids_entities

from ._utils import _glob
from .dataset import is_dataset_root


def find_bids_parents(
    query: Dict[str, str],
    start: StrOrPath,
    depth: Optional[int] = None,
) -> Generator[str, None, None]:
    """
    Find all BIDS files satisfying the inheritance principle requirements for the given
    `query` entities dict. The `query` must contain at least one of `'suffix'` or
    `'ext'`. Search up the directory hierarchy at most `depth` levels, starting from and
    including `start`, or until a `dataset_description.json` file is found, indicating
    the BIDS dataset root directory. If `depth` is `None`, the search may continue to
    the filesystem root.

    Yields matching `path`s in decreasing topological order.
    """
    suffix = query.get("suffix", "")
    ext = query.get("ext", "")
    if not (suffix or ext):
        raise ValueError("At least one of 'suffix' or 'ext' are required in `query`.")
    pattern = f"*{suffix}{ext}"

    start = Path(start).absolute()
    if not start.is_dir():
        start = start.parent

    if depth is None:
        depth = len(start.parts)

    for _ in range(depth):
        for path in _glob(start, pattern):
            entities = parse_bids_entities(path)
            if _test_bids_match(query, entities):
                yield str(path)

        # Stop climbing the directory if we find the description json, which should
        # always be at the top-level dataset directory.
        # TODO: for nested datasets, can you inherit beyond the first root? I hope not..
        if is_dataset_root(start):
            break

        start = start.parent


def find_first_bids_parent(
    query: Dict[str, str], start: StrOrPath, depth: Optional[int] = None
) -> Optional[str]:
    """
    Find the first BIDS parent file matching the ``query`` entities dict. Returns
    ``None`` if no parents found. See :func:`find_bids_parents` for more details.
    """
    return next(find_bids_parents(query, start, depth), None)


def _test_bids_match(query: Dict[str, str], entities: Dict[str, str]) -> bool:
    """
    Test if entities satisfies the inheritance principle for query.
    """
    entities = entities.copy()
    entities.pop("datatype", None)
    return set(entities).issubset(query) and all(
        query[k] == v for k, v in entities.items()
    )
