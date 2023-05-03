import json
import logging
import traceback
from functools import lru_cache
from pathlib import Path
from typing import Dict, Generator, List, Optional

from elbow.record import Record
from elbow.typing import StrOrPath

from .entities import parse_bids_entities


def json_sidecar(path: StrOrPath) -> Record:
    """
    Load the JSON sidecar metadata associated with ``path``. Supports metadata
    inheritance by searching up the directory tree for matching JSON files.
    """
    entities = parse_bids_entities(path)
    query = dict(entities, extension=".json")
    root = Path(path).parent

    metadata = {}
    sidecars = reversed(list(find_bids_parents(query, root=root)))
    for path in sidecars:
        with open(path) as f:
            try:
                metadata.update(json.load(f))
            except (json.JSONDecodeError, TypeError):
                logging.warning(
                    f"Bad JSON sidecar data {path}\n\n" + traceback.format_exc()
                )

    # TODO: type aliases for json, pickle, etc so we can use a dataclass here.
    rec = Record({"sidecar": metadata or None}, types={"sidecar": "json"})
    return rec


def find_first_bids_parent(
    query: Dict[str, str], root: StrOrPath, depth: int = 4
) -> Optional[str]:
    """
    Find the first BIDS parent file matching the ``query`` entities dict. Returns
    ``None`` if no parents found. See :func:`find_bids_parents` for more details.
    """
    return next(find_bids_parents(query, root, depth), None)


def find_bids_parents(
    query: Dict[str, str], root: StrOrPath, depth: int = 4
) -> Generator[str, None, None]:
    """
    Find all BIDS files satisfying the inheritance principle requirements for the given
    ``query`` entities dict. The ``query`` must contain at least one of ``'suffix'`` or
    ``'extension'``. Search up the directory hierarchy at most ``depth`` levels,
    starting from and including ``root``. Yields matching ``path``s in decreasing
    topological order.

    The default depth of 4 is appropriate for directory structures of the form
    ``{dataset}/sub-{sub}/ses-{ses}/{datatype}``. Note also that the search stops once a
    ``dataset_description.json`` is found.
    """
    suffix = query.get("suffix")
    ext = query.get("extension")
    if not (suffix or ext):
        raise ValueError(
            "At least one of 'suffix' or 'extension' are required in `query`."
        )
    pattern = f"*_{suffix}{ext}" if suffix else f"*{ext}"

    root = Path(root)
    if not root.is_dir():
        root = root.parent

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


@lru_cache(maxsize=16)
def _glob(path: Path, pattern: str) -> List[Path]:
    return list(path.glob(pattern))


def _test_bids_match(query: Dict[str, str], entities: Dict[str, str]) -> bool:
    entities = {k: v for k, v in entities.items() if k not in {"datatype"}}

    return set(entities).issubset(query) and all(
        query[k] == v for k, v in entities.items()
    )


@lru_cache(maxsize=512)
def is_dataset_root(path: Path) -> bool:
    """
    Test if ``path`` is a BIDS dataset root directory.
    """
    return path.is_dir() and (path / "dataset_description.json").exists()
