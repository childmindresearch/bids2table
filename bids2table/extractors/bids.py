import logging
from glob import iglob
from pathlib import Path
from typing import Generator, Optional

from elbow.extractors import extract_file_meta
from elbow.record import Record, concat
from elbow.typing import StrOrPath

from .dataset import extract_dataset_meta
from .entities import BIDSEntities
from .sidecar import extract_sidecar


def extract_bids_file(path: StrOrPath) -> Optional[Record]:
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
    bids_meta = extract_sidecar(path)
    file_meta = extract_file_meta(path)

    rec = concat(
        {
            "dataset": dset_meta,
            "entities": entities,
            "metadata": bids_meta,
            "file": file_meta,
        }
    )
    return rec


def extract_bids_subdir(path: StrOrPath) -> Generator[Optional[Record], None, None]:
    """
    Extract BIDS records recursively for all files in a sub-directory.
    """
    for path in iglob(str(Path(path) / "**"), recursive=True):
        yield extract_bids_file(path)
