from functools import lru_cache
from pathlib import Path
from typing import Dict, Generator, List, Optional

from elbow.typing import StrOrPath

from .dataset import is_dataset_root
from .entities import parse_bids_entities


def find_bids_parents(
    query: Dict[str, str],
    root: StrOrPath,
    depth: Optional[int] = 4,
) -> Generator[str, None, None]:
    """
    Find all BIDS files satisfying the inheritance principle requirements for the given
    ``query`` entities dict. The ``query`` must contain at least one of ``'suffix'`` or
    ``'ext'``. Search up the directory hierarchy at most ``depth`` levels,
    starting from and including ``root``. Yields matching ``path``s in decreasing
    topological order.

    The default depth of 4 is appropriate for directory structures of the form
    ``{dataset}/sub-{sub}/ses-{ses}/{datatype}``. If depth is None, search all the way
    up the tree. Note also that the search stops once a ``dataset_description.json`` is
    found.
    """
    suffix = query.get("suffix")
    ext = query.get("ext")
    if not (suffix or ext):
        raise ValueError("At least one of 'suffix' or 'ext' are required in `query`.")
    pattern = f"*_{suffix}{ext}" if suffix else f"*{ext}"

    root = Path(root).absolute()
    if not root.is_dir():
        root = root.parent

    if depth is None:
        depth = len(root.parts)

    for _ in range(depth):
        for path in _glob(root, pattern):
            entities = parse_bids_entities(path)
            if _test_bids_match(query, entities):
                yield str(path)

        # Stop climbing the directory if we find the description json, which should
        # always be at the top-level dataset directory.
        # TODO: for nested datasets, can you inherit beyond the first root? I hope not..
        if is_dataset_root(root):
            break

        root = root.parent


def find_first_bids_parent(
    query: Dict[str, str], root: StrOrPath, depth: int = 4
) -> Optional[str]:
    """
    Find the first BIDS parent file matching the ``query`` entities dict. Returns
    ``None`` if no parents found. See :func:`find_bids_parents` for more details.
    """
    return next(find_bids_parents(query, root, depth), None)


@lru_cache(maxsize=16)
def _glob(path: Path, pattern: str) -> List[Path]:
    return list(path.glob(pattern))


def _test_bids_match(query: Dict[str, str], entities: Dict[str, str]) -> bool:
    """
    Test if entities satisfies the inheritance principle for query.
    """
    entities = entities.copy()
    entities.pop("datatype", None)
    return set(entities).issubset(query) and all(
        query[k] == v for k, v in entities.items()
    )
