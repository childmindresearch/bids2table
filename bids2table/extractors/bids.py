import logging
from glob import iglob
from pathlib import Path
from typing import Generator, Optional

from elbow.extractors import extract_file_meta
from elbow.record import Record, concat
from elbow.typing import StrOrPath

from bids2table.entities import BIDSEntities

from .dataset import extract_dataset
from .metadata import extract_metadata, is_associated_sidecar


def extract_bids_file(path: StrOrPath) -> Optional[Record]:
    """
    Extract BIDS entities and metadata from a data file in a BIDS dataset.
    """
    if not is_bids_file(path):
        return None

    try:
        entities = BIDSEntities.from_path(path)
    except (TypeError, ValueError) as exc:
        logging.warning(
            "Incomplete and/or invalid entities in file %s", path, exc_info=exc
        )
        return None

    dset_rec = extract_dataset(path)
    meta_rec = extract_metadata(path)
    file_rec = extract_file_meta(path)

    rec = concat({"ds": dset_rec, "ent": entities, "meta": meta_rec, "finfo": file_rec})
    return rec


def extract_bids_subdir(path: StrOrPath) -> Generator[Optional[Record], None, None]:
    """
    Extract BIDS records recursively for all files in a sub-directory.
    """
    for path in iglob(str(Path(path) / "**"), recursive=True):
        yield extract_bids_file(path)


def is_bids_file(path: StrOrPath) -> bool:
    """
    Check if `path` is a valid BIDS data file. E.g. not a directory or JSON sidecar
    associated to another data file.
    """
    # TODO: other checks?
    #   - skip files matching patterns in .bidsignore?
    path = Path(path)
    return (
        not path.is_dir()
        and path.name.startswith("sub-")
        and not is_associated_sidecar(path)
    )
