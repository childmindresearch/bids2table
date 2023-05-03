from pathlib import Path
from typing import Optional

from elbow import Record, concat
from elbow.extractors import file_meta
from elbow.typing import StrOrPath

from .entities import all_bids_entities, bids_entities
from .sidecar import json_sidecar


def bids_extract(path: StrOrPath) -> Optional[Record]:
    """
    Extract BIDS entities and metadata from a file in a BIDS dataset.
    """
    # Exclude JSON files, only want data files
    # TODO: other checks?
    if Path(path).suffix == ".json":
        return None

    file_info = file_meta(path)
    known_ents = bids_entities(path)
    all_ents = all_bids_entities(path)
    sidecar = json_sidecar(path)

    rec = concat([known_ents, all_ents, sidecar, file_info])
    return rec
