import logging
from pathlib import Path
from typing import Optional

from elbow.extractors import extract_file_meta
from elbow.record import Record, concat
from elbow.typing import StrOrPath

from .dataset import extract_dataset_meta
from .entities import BIDSEntities
from .image import extract_image_meta
from .sidecar import extract_sidecar


def extract_bids(path: StrOrPath) -> Optional[Record]:
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

    dset_meta = extract_dataset_meta(path)
    sidecar = extract_sidecar(path)
    image_meta = extract_image_meta(path)
    file_meta = extract_file_meta(path)

    rec = concat(
        {
            "dataset": dset_meta,
            "bids": concat([entities, sidecar]),
            "image": image_meta,
            "file": file_meta,
        }
    )
    return rec
