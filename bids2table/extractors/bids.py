import logging
from pathlib import Path
from typing import Optional

from elbow import Record, concat
from elbow.extractors import file_meta
from elbow.typing import StrOrPath

from .dataset import dataset_meta
from .entities import BIDSEntities
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
        entities = BIDSEntities.from_path(path)
    except (TypeError, ValueError) as exc:
        logging.warning(
            "Incomplete and/or invalid entities in file %s", path, exc_info=exc
        )
        return None

    dset_info = dataset_meta(path)
    sidecar = json_sidecar(path)
    image_info = image_meta(path)
    file_info = file_meta(path)

    rec = concat([dset_info, entities, sidecar, image_info, file_info])
    return rec
