import logging
from typing import Any, Dict, Tuple

import nibabel as nib
import numpy as np
from elbow.record import Record
from elbow.typing import StrOrPath
from nibabel.filebasedimages import ImageFileError

from .entities import parse_bids_entities

try:
    import nifti

    has_nifti = True
except ModuleNotFoundError:
    has_nifti = False

# TODO: add more
IMAGE_EXTENSIONS = {".nii", ".nii.gz"}


def extract_image_meta(path: StrOrPath, *, backend: str = "nibabel") -> Record:
    entities = parse_bids_entities(path)
    ext = entities.get("ext")

    header = affine = None
    if ext in IMAGE_EXTENSIONS:
        try:
            header, affine = _read_image_meta(str(path), backend=backend)
        except (ImageFileError, SystemError) as exc:
            logging.warning("Failed to load image %s", path, exc_info=exc)

    rec = Record(
        {"image_header": header, "image_affine": affine},
        types={"image_header": "json", "image_affine": "ndarray<float64>"},
    )
    return rec


def _read_image_meta(
    path: str, backend: str = "nibabel"
) -> Tuple[Dict[str, Any], np.ndarray]:
    if backend == "nifti":
        if not has_nifti:
            raise ModuleNotFoundError("nifti image backend not installed")

        # TODO: the nifti C lib prints a lot of error output that I'd like to suppress
        header = nifti.read_header(path)
        # TODO: affine not currently implemented for nifti lib
        affine = None
    else:
        img = nib.load(path)
        header = dict(img.header)
        affine = np.asarray(img.affine)

    header = {k: _cast_header_value(v) for k, v in header.items()}
    return header, affine


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
