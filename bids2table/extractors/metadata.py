import json
import logging
import traceback
from pathlib import Path

from elbow.record import Record
from elbow.typing import StrOrPath

from bids2table.entities import parse_bids_entities

from .inheritance import _glob, find_bids_parents


def extract_metadata(path: StrOrPath) -> Record:
    """
    Load the JSON sidecar metadata associated with ``path``. Supports metadata
    inheritance by searching up the directory tree for matching JSON files.
    """
    entities = parse_bids_entities(path)
    query = dict(entities, ext=".json")

    metadata = {}
    sidecars = reversed(list(find_bids_parents(query, start=Path(path).parent)))
    for path in sidecars:
        with open(path) as f:
            try:
                metadata.update(json.load(f))
            except (json.JSONDecodeError, TypeError):
                logging.warning(
                    f"Bad JSON sidecar data {path}\n\n" + traceback.format_exc()
                )

    # TODO: type aliases for json, pickle, etc so we can use a dataclass here.
    rec = Record({"json": metadata or None}, types={"json": "json"})
    return rec


def is_associated_sidecar(path: StrOrPath) -> bool:
    """
    Check if a file is a JSON sidecar associated with other data file(s).
    """
    path = Path(path)

    # Must be JSON
    if not path.suffix == ".json":
        return False

    entities = parse_bids_entities(path)

    # Assume all JSON above the lowest level of hierarchy are associated
    if entities.get("datatype") is None:
        return True

    # All sidecars must contain a suffix
    suffix = entities.get("suffix")
    if suffix is None:
        return False

    # Finally, check if there are any matches at the lowest level
    # If not, we are a key-value file or solo sidecar like an MRIQC IQM JSON.
    # Note this pattern always matches the file itself, so we check if there are any
    # extra matches.
    if len(_glob(path.parent, f"*_{suffix}.*")) > 1:
        return True

    return False
