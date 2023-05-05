import logging
from typing import Any, Dict

import nibabel as nib
import numpy as np
from elbow import Record
from elbow.typing import StrOrPath
from nibabel.filebasedimages import ImageFileError

from .entities import parse_bids_entities

# TODO: add more
IMAGE_EXTENSIONS = {".nii", ".nii.gz"}


def image_meta(path: StrOrPath) -> Record:
    entities = parse_bids_entities(path)
    ext = entities.get("ext")

    header = affine = None
    if ext in IMAGE_EXTENSIONS:
        try:
            img = nib.load(str(path))
            header = _load_image_header(img)
            affine = np.asarray(img.affine)
        except ImageFileError as exc:
            logging.warning(f"Failed to load image {path}", exc_info=exc)

    rec = Record(
        {"image_header": header, "image_affine": affine},
        types={"image_header": "json", "image_affine": "ndarray<float64>"},
    )
    return rec


def _load_image_header(img: Any) -> Dict[str, Any]:
    """
    Load a NiBabel image header as a JSON-serializable dict
    """
    header = dict(img.header)
    header = {k: _cast_header_value(v) for k, v in header.items()}
    return header


def _cast_header_value(value: np.ndarray) -> Any:
    """
    NiBabel header values appear to be numpy arrays whose data can be:

        - numeric vectors
        - numeric scalars
        - bytes scalars

    Convert these data to JSON-serializable python objects.

    TODO: Check these assumptions against more nibabel image types.
    """
    cast_value = value.tolist()
    if isinstance(cast_value, bytes):
        cast_value = cast_value.decode()
    return cast_value
