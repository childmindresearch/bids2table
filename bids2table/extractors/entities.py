import re
from dataclasses import dataclass, field, fields
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from elbow.record import Record
from elbow.typing import StrOrPath

BIDS_DATATYPES = (
    "anat",
    "beh",
    "dwi",
    "eeg",
    "fmap",
    "func",
    "ieeg",
    "meg",
    "micr",
    "perf",
    "pet",
)


def bids_field(
    name: str,
    required: bool = False,
    allowed_values: Optional[Iterable] = None,
):
    """
    BIDS entity dataclass field.
    """
    if allowed_values is not None:
        allowed_values = set(allowed_values)

    if required:
        return field(metadata=dict(name=name, allowed_values=allowed_values))
    return field(default=None, metadata=dict(name=name, allowed_values=allowed_values))


@dataclass
class BIDSEntities:
    """
    A dataclass representing known BIDS entities.

    NOTE: Only minimal validation of the entity values is performed. Use
    `bids_validator` for thorough validation of BIDS datasets.

    Reference:
        https://bids-specification.readthedocs.io/en/stable/appendices/entities.html
    """

    sub: str = bids_field(name="Subject", required=True)
    ses: str = bids_field(name="Session")
    sample: str = bids_field(name="Sample")
    task: str = bids_field(name="Task")
    acq: str = bids_field(name="Acquisition")
    ce: str = bids_field(name="Contrast Enhancing Agent")
    trc: str = bids_field(name="Tracer")
    stain: str = bids_field(name="Stain")
    rec: str = bids_field(name="Reconstruction")
    dir: str = bids_field(name="Phase-Encoding Direction")
    run: int = bids_field(name="Run")
    mod: str = bids_field(name="Corresponding Modality")
    echo: int = bids_field(name="Echo")
    flip: int = bids_field(name="Flip Angle")
    inv: int = bids_field(name="Inversion Time")
    mt: str = bids_field(name="Magnetization Transfer", allowed_values={"on", "off"})
    part: str = bids_field(name="Part", allowed_values={"mag", "phase", "real", "imag"})
    proc: str = bids_field(name="Processed (on device)")
    hemi: str = bids_field(name="Hemisphere", allowed_values={"L", "R"})
    space: str = bids_field(name="Space")
    split: str = bids_field(name="Split")
    recording: str = bids_field(name="Recording")
    chunk: int = bids_field(name="Chunk")
    atlas: str = bids_field(name="Atlas")
    res: str = bids_field(name="Resolution")
    den: str = bids_field(name="Density")
    label: str = bids_field(name="Label")
    desc: str = bids_field(name="Description")
    datatype: str = bids_field(name="Data type", allowed_values=BIDS_DATATYPES)
    suffix: str = bids_field(name="Suffix")
    ext: str = bids_field(name="Extension")

    @classmethod
    def from_dict(cls, entities: Dict[str, Any]):
        """
        Initialize from a dict of entities.
        """
        filtered = {}
        for f in fields(cls):
            val = entities.get(f.name)
            if val is not None:
                try:
                    val = f.type(val)
                except ValueError as exc:
                    raise ValueError(
                        f"Unable to coerce {repr(val)} to type {f.type} for "
                        f"entity {f.name}"
                    ) from exc

                allowed_values = f.metadata.get("allowed_values")
                if allowed_values and val not in allowed_values:
                    raise ValueError(
                        f"Value {val} for entity {f.name} isn't one of the "
                        f"allowed values {allowed_values}"
                    )

                filtered[f.name] = val

        return cls(**filtered)


def bids_entities(path: StrOrPath) -> BIDSEntities:
    """
    Extract known BIDS entities from the file name.
    """
    entities = parse_bids_entities(path)
    return BIDSEntities.from_dict(entities)


def all_bids_entities(path: StrOrPath) -> Record:
    """
    Extract all BIDS entities. Returns a record with a single JSON-values field
    'entities'.
    """
    entities = parse_bids_entities(path)
    record = Record({"entities": entities}, types={"entities": "json"})
    return record


@lru_cache(maxsize=8)
def parse_bids_entities(path: StrOrPath) -> Dict[str, Optional[str]]:
    """
    Parse all BIDS filename ``"{key}-{value}"`` entities as well as special entities:

        - datatype
        - suffix
        - ext (extension)

    from the file path.

    .. note:: This function does not validate entities.
    """
    path = Path(path)
    entities = {}

    # datatype
    match = re.search(
        f"/({'|'.join(BIDS_DATATYPES)})/",
        path.as_posix(),
    )
    datatype = match.group(1) if match is not None else None

    filename = path.name
    parts = filename.split("_")

    # suffix and extension
    suffix_ext = parts.pop()
    idx = suffix_ext.find(".")
    if idx < 0:
        suffix, ext = suffix_ext, None
    else:
        suffix, ext = suffix_ext[:idx], suffix_ext[idx:]

    # suffix is actually an entity, put back in list
    if "-" in suffix:
        parts.append(suffix)
        suffix = None

    # parse entities
    for part in parts:
        if "-" in part:
            key, val = part.split("-", maxsplit=1)
        else:
            key, val = part, True
        entities[key] = val

    entities["datatype"] = datatype
    entities["suffix"] = suffix
    entities["ext"] = ext
    return entities
