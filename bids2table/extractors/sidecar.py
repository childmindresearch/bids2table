import json
import logging
import traceback
from pathlib import Path

from elbow.record import Record
from elbow.typing import StrOrPath

from ._inheritance import find_bids_parents
from .entities import parse_bids_entities


def extract_sidecar(path: StrOrPath) -> Record:
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
