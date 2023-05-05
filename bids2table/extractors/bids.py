import logging
from pathlib import Path
from typing import Optional

from elbow import Record, concat
from elbow.extractors import file_meta
from elbow.typing import StrOrPath

from .dataset import dataset_meta
from .entities import all_bids_entities, bids_entities
from .image import image_meta
from .sidecar import json_sidecar


def bids_extract(path: StrOrPath) -> Optional[Record]:
    """
    Extract BIDS entities and metadata from a file in a BIDS dataset.
    """
    # Exclude JSON files, only want data files
    # TODO: other checks?
    #   - skip files matching patterns in .bidsignore?
    path = Path(path)
    if path.is_dir() or path.suffix == ".json" or not path.name.startswith("sub-"):
        return None

    try:
        known_ents = bids_entities(path)
    except (TypeError, ValueError) as exc:
        logging.warning(
            f"Incomplete and/or invalid entities in file '{path}'", exc_info=exc
        )
        return None

    dset_info = dataset_meta(path)
    all_ents = all_bids_entities(path)
    sidecar = json_sidecar(path)
    image_info = image_meta(path)
    file_info = file_meta(path)

    rec = concat([dset_info, known_ents, all_ents, sidecar, image_info, file_info])
    return rec
