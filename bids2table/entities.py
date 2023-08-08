"""
A structured representation for BIDS entities.
"""

import re
import warnings
from dataclasses import asdict, dataclass, field, fields
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import pandas as pd
from elbow.typing import StrOrPath
from typing_extensions import get_args, get_origin

_BIDS_DATATYPES = (
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


def _bids_field(
    name: str,
    display_name: str,
    required: bool = False,
    allowed_values: Optional[Iterable] = None,
    default: Optional[Any] = None,
    default_factory: Optional[Callable] = None,
):
    """
    BIDS entity dataclass field.
    """
    if allowed_values is not None:
        allowed_values = list(allowed_values)

    metadata = {
        "name": name,
        "display_name": display_name,
        "allowed_values": allowed_values,
    }
    if required:
        fld = field(metadata=metadata)
    elif default_factory is not None:
        fld = field(default_factory=default_factory, metadata=metadata)
    else:
        fld = field(default=default, metadata=metadata)
    return fld


@dataclass
class BIDSEntities:
    """
    A dataclass representing known BIDS entities.

    Note:
        Only minimal validation of the entity values is performed. Use `bids_validator`
        for thorough validation of BIDS datasets.

    Reference:
        https://bids-specification.readthedocs.io/en/stable/appendices/entities.html
    """

    sub: str = _bids_field(name="subject", display_name="Subject", required=True)
    ses: Optional[str] = _bids_field(name="session", display_name="Session")
    sample: Optional[str] = _bids_field(name="sample", display_name="Sample")
    task: Optional[str] = _bids_field(name="task", display_name="Task")
    acq: Optional[str] = _bids_field(name="acquisition", display_name="Acquisition")
    ce: Optional[str] = _bids_field(
        name="ceagent", display_name="Contrast Enhancing Agent"
    )
    trc: Optional[str] = _bids_field(name="tracer", display_name="Tracer")
    stain: Optional[str] = _bids_field(name="stain", display_name="Stain")
    rec: Optional[str] = _bids_field(
        name="reconstruction", display_name="Reconstruction"
    )
    dir: Optional[str] = _bids_field(
        name="direction", display_name="Phase-Encoding Direction"
    )
    run: Optional[int] = _bids_field(name="run", display_name="Run")
    mod: Optional[str] = _bids_field(
        name="modality", display_name="Corresponding Modality"
    )
    echo: Optional[int] = _bids_field(name="echo", display_name="Echo")
    flip: Optional[int] = _bids_field(name="flip", display_name="Flip Angle")
    inv: Optional[int] = _bids_field(name="inversion", display_name="Inversion Time")
    mt: Optional[str] = _bids_field(
        name="mtransfer",
        display_name="Magnetization Transfer",
        allowed_values={"on", "off"},
    )
    part: Optional[str] = _bids_field(
        name="part",
        display_name="Part",
        allowed_values={"mag", "phase", "real", "imag"},
    )
    proc: Optional[str] = _bids_field(
        name="processing", display_name="Processed (on device)"
    )
    hemi: Optional[str] = _bids_field(
        name="hemisphere", display_name="Hemisphere", allowed_values={"L", "R"}
    )
    space: Optional[str] = _bids_field(name="space", display_name="Space")
    split: Optional[int] = _bids_field(name="split", display_name="Split")
    recording: Optional[str] = _bids_field(name="recording", display_name="Recording")
    chunk: Optional[int] = _bids_field(name="chunk", display_name="Chunk")
    atlas: Optional[str] = _bids_field(name="atlas", display_name="Atlas")
    res: Optional[str] = _bids_field(name="resolution", display_name="Resolution")
    den: Optional[str] = _bids_field(name="density", display_name="Density")
    label: Optional[str] = _bids_field(name="label", display_name="Label")
    desc: Optional[str] = _bids_field(name="description", display_name="Description")
    datatype: Optional[str] = _bids_field(
        name="datatype", display_name="Data type", allowed_values=_BIDS_DATATYPES
    )
    suffix: Optional[str] = _bids_field(name="suffix", display_name="Suffix")
    ext: Optional[str] = _bids_field(name="extension", display_name="Extension")
    extra_entities: Optional[Dict[str, Union[str, int]]] = _bids_field(
        name="extra_entities",
        display_name="Extra entities",
        default_factory=dict,
    )

    @staticmethod
    def special() -> List[str]:
        """
        Get list of field keys which are not standard entities.
        """
        return ["datatype", "suffix", "ext", "extra_entities"]

    @classmethod
    def from_dict(cls, entities: Dict[str, Any], valid_only: bool = False):
        """
        Initialize from a dict of entities.
        """
        filtered = {}
        extra_entities: Dict[str, Union[str, int]] = {}
        fields_map = {f.name: f for f in fields(cls) if f.name != "extra_entities"}

        def add_to_extra(k: Any, v: Any):
            if not isinstance(key, str):
                raise TypeError(
                    f"Extra entity {k} has type {type(k)}; only str supported"
                )
            if isinstance(v, (str, int)):
                extra_entities[k] = v
            else:
                warnings.warn(
                    f"Value {v} for extra entity {k} has type {type(v)}; "
                    f"only str, int supported"
                )

        for key, val in entities.items():
            if pd.isna(val):
                continue

            if key in fields_map:
                fld = fields_map[key]
                typ = _get_type(fld.type)
                try:
                    val = typ(val)
                except ValueError as exc:
                    raise ValueError(
                        f"Unable to coerce {repr(val)} to type {typ} for "
                        f"entity {fld.name}"
                    ) from exc

                allowed_values = fld.metadata.get("allowed_values")
                if allowed_values and val not in allowed_values:
                    raise ValueError(
                        f"Value {val} for entity {fld.name} isn't one of the "
                        f"allowed values {allowed_values}"
                    )

                filtered[key] = val

            # Special handling if the dict already contains 'extra_entities'. This makes
            # it easy to reconstruct entities from a df row.
            elif key == "extra_entities":
                assert isinstance(
                    val, dict
                ), "Value for 'extra_entities' key must be a dict"
                for k, v in val.items():
                    add_to_extra(k, v)

            elif not valid_only:
                add_to_extra(key, val)

        return cls(**filtered, extra_entities=extra_entities)

    @classmethod
    def from_path(cls, path: StrOrPath):
        """
        Initialize from a file path.
        """
        entities = parse_bids_entities(path)
        return cls.from_dict(entities)

    def to_dict(self, valid_only: bool = False) -> Dict[str, Any]:
        """
        Convert entities to a plain dict.
        """
        data = asdict(self)
        extra = data.pop("extra_entities")
        if not valid_only and extra:
            data.update(extra)
        return data

    def to_path(
        self,
        prefix: Optional[StrOrPath] = None,
        valid_only: bool = False,
        int_format: str = "%d",
    ) -> Path:
        """
        Generate a filepath based on the entitities.
        """
        special = {"datatype", "suffix", "ext"}
        data = self.to_dict(valid_only=valid_only)

        name = "_".join(
            _fmt_ent(k, v, int_format=int_format)
            for k, v in data.items()
            if k not in special and not pd.isna(v)
        )
        if self.suffix:
            name += f"_{self.suffix}"
        if self.ext:
            name += self.ext

        path = Path(name)
        if self.datatype:
            path = self.datatype / path
        if self.ses:
            path = f"ses-{self.ses}" / path
        path = f"sub-{self.sub}" / path
        if prefix:
            path = prefix / path
        return path

    def with_update(
        self, entitities: Optional[Dict[str, Any]] = None, **kwargs
    ) -> "BIDSEntities":
        """
        Create a new instance with updated entities.
        """
        data = self.to_dict(valid_only=False)
        if entitities:
            data.update(entitities)
        if kwargs:
            data.update(kwargs)
        return BIDSEntities.from_dict(data)


def _get_type(alias: Any) -> type:
    """
    Unbox type aliases of the form `Optional[str]`.
    """
    if _is_optional(alias):
        return get_args(alias)[0]
    return alias


def _is_optional(alias: Any) -> bool:
    """
    Check if alias is an optional of a primitive type like `Optional[str]`.
    """
    return (
        get_origin(alias) is Union
        and len(get_args(alias)) == 2
        and isinstance(None, get_args(alias)[1])
    )


def _fmt_ent(
    k: str,
    v: Union[str, int],
    *,
    int_format: str = "%d",
):
    if isinstance(v, int):
        v = int_format % v
    ent = f"{k}-{v}" if len(v) > 0 else k
    return ent


@lru_cache()
def parse_bids_entities(path: StrOrPath) -> Dict[str, str]:
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
        f"/({'|'.join(_BIDS_DATATYPES)})/",
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
            key, val = part, ""
        entities[key] = val

    for k, v in zip(["datatype", "suffix", "ext"], [datatype, suffix, ext]):
        if v is not None:
            entities[k] = v
    return entities
