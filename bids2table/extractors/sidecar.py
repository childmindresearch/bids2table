import json
import logging
import traceback
from functools import lru_cache
from pathlib import Path
from typing import Dict, Generator, List

from elbow.record import Record
from elbow.typing import StrOrPath

from .dataset import is_dataset_root
from .entities import parse_bids_entities


def json_sidecar(path: StrOrPath) -> Record:
    """
    Load the JSON sidecar metadata associated with ``path``. Supports metadata
    inheritance by searching up the directory tree for matching JSON files.
    """
    entities = parse_bids_entities(path)
    query = dict(entities, ext=".json")
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


def find_bids_parents(
    query: Dict[str, str], root: StrOrPath, depth: int = 4
) -> Generator[str, None, None]:
    """
    Find all BIDS files satisfying the inheritance principle requirements for the given
    ``query`` entities dict. The ``query`` must contain at least one of ``'suffix'`` or
    ``'ext'``. Search up the directory hierarchy at most ``depth`` levels,
    starting from and including ``root``. Yields matching ``path``s in decreasing
    topological order.

    The default depth of 4 is appropriate for directory structures of the form
    ``{dataset}/sub-{sub}/ses-{ses}/{datatype}``. Note also that the search stops once a
    ``dataset_description.json`` is found.
    """
    suffix = query.get("suffix")
    ext = query.get("ext")
    if not (suffix or ext):
        raise ValueError("At least one of 'suffix' or 'ext' are required in `query`.")
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
    entities = entities.copy()
    entities.pop("datatype", None)
    return set(entities).issubset(query) and all(
        query[k] == v for k, v in entities.items()
    )
