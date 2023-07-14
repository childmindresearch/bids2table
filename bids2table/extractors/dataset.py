import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from elbow.record import Record
from elbow.typing import StrOrPath


def extract_dataset_meta(path: StrOrPath) -> Record:
    """
    Get info about the BIDS dataset that ``path`` belongs to.
    """
    name, root = identify_bids_dataset(path)
    desc = get_dataset_description(root) if root is not None else None
    rec = Record(
        {
            "dataset": name,
            "dataset_path": str(root) if root else None,
            "dataset_description": desc,
        },
        types={"dataset_description": "json"},
    )
    return rec


def identify_bids_dataset(path: StrOrPath) -> Tuple[Optional[str], Optional[Path]]:
    """
    Identify the BIDS dataset that ``path`` belongs to. Return the dataset directory
    name and the full dataset path. For nested derivatives datasets, a composite name of
    the form ``"ds000001/derivatives/fmriprep"`` is returned.

    Note that the name is extracted from the path, not the dataset description JSON.
    """
    path = Path(path)
    parent = path if path.is_dir() else path.parent

    parts: List[str] = []
    scanning = False
    top_idx = None
    root = None

    while parent.name:
        if is_dataset_root(parent):
            scanning = True
            top_idx = len(parts)
            if root is None:
                root = parent

        if scanning:
            parts.append(parent.name)

        parent = parent.parent

    if len(parts) == 0:
        logging.warning("File %s is not part of any valid BIDS dataset.", path)
        return None, None

    parts = parts[: top_idx + 1]
    dataset = "/".join(reversed(parts))
    return dataset, root


@lru_cache(maxsize=512)
def is_dataset_root(path: Path) -> bool:
    """
    Test if ``path`` is a BIDS dataset root directory.
    """
    return path.is_dir() and (path / "dataset_description.json").exists()


@lru_cache(maxsize=64)
def get_dataset_description(root: Path) -> Optional[Dict[str, Any]]:
    """
    Load the JSON description for the BIDS dataset root directory ``root``.
    """
    if not is_dataset_root(root):
        raise ValueError(f"{root} is not a BIDS dataset root")

    desc_path = root / "dataset_description.json"
    with desc_path.open() as f:
        try:
            description = json.load(f)
        except json.JSONDecodeError:
            description = None
    return description
