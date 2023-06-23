import re
from dataclasses import asdict, dataclass, field, fields
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Union

from elbow.typing import StrOrPath
from typing_extensions import get_args, get_origin

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
    default: Optional[Any] = None,
    default_factory: Optional[Callable] = None,
):
    """
    BIDS entity dataclass field.
    """
    if allowed_values is not None:
        allowed_values = set(allowed_values)

    metadata = dict(name=name, allowed_values=allowed_values)
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

    sub: str = bids_field(name="Subject", required=True)
    ses: Optional[str] = bids_field(name="Session")
    sample: Optional[str] = bids_field(name="Sample")
    task: Optional[str] = bids_field(name="Task")
    acq: Optional[str] = bids_field(name="Acquisition")
    ce: Optional[str] = bids_field(name="Contrast Enhancing Agent")
    trc: Optional[str] = bids_field(name="Tracer")
    stain: Optional[str] = bids_field(name="Stain")
    rec: Optional[str] = bids_field(name="Reconstruction")
    dir: Optional[str] = bids_field(name="Phase-Encoding Direction")
    run: Optional[int] = bids_field(name="Run")
    mod: Optional[str] = bids_field(name="Corresponding Modality")
    echo: Optional[int] = bids_field(name="Echo")
    flip: Optional[int] = bids_field(name="Flip Angle")
    inv: Optional[int] = bids_field(name="Inversion Time")
    mt: Optional[str] = bids_field(
        name="Magnetization Transfer", allowed_values={"on", "off"}
    )
    part: Optional[str] = bids_field(
        name="Part", allowed_values={"mag", "phase", "real", "imag"}
    )
    proc: Optional[str] = bids_field(name="Processed (on device)")
    hemi: Optional[str] = bids_field(name="Hemisphere", allowed_values={"L", "R"})
    space: Optional[str] = bids_field(name="Space")
    split: Optional[int] = bids_field(name="Split")
    recording: Optional[str] = bids_field(name="Recording")
    chunk: Optional[int] = bids_field(name="Chunk")
    atlas: Optional[str] = bids_field(name="Atlas")
    res: Optional[str] = bids_field(name="Resolution")
    den: Optional[str] = bids_field(name="Density")
    label: Optional[str] = bids_field(name="Label")
    desc: Optional[str] = bids_field(name="Description")
    datatype: Optional[str] = bids_field(
        name="Data type", allowed_values=BIDS_DATATYPES
    )
    suffix: Optional[str] = bids_field(name="Suffix")
    ext: Optional[str] = bids_field(name="Extension")
    extra_entities: Optional[Dict[str, Union[str, int, float]]] = bids_field(
        name="Extra entities",
        default_factory=dict,
    )

    @classmethod
    def from_dict(cls, entities: Dict[str, Any]):
        """
        Initialize from a dict of entities.
        """
        filtered = {}
        extra_entities: Dict[str, Union[str, int, float]] = {}
        fields_map = {f.name: f for f in fields(cls) if not f.name.startswith("_")}

        def add_to_extra(k: Any, v: Any):
            if not isinstance(key, str):
                raise TypeError(
                    f"Extra entity {k} has type {type(k)}; only str supported"
                )
            if not isinstance(v, (str, int, float)):
                raise TypeError(
                    f"Value {v} for extra entity {k} has type {type(v)}; "
                    f"only str, int, float supported"
                )
            extra_entities[k] = v

        for key, val in entities.items():
            if _is_na(val):
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

            else:
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
        int_format: str = "%02d",
    ) -> Path:
        """
        Generate a filepath based on the entitities.
        """
        special = {"datatype", "suffix", "ext"}
        data = self.to_dict(valid_only=valid_only)

        name = "_".join(
            _fmt_ent(k, v, int_format=int_format)
            for k, v in data.items()
            if k not in special and not _is_na(v)
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


def _is_na(value: Any) -> bool:
    return value is None or value == float("nan")


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
    int_format: str = "%02d",
):
    if isinstance(v, int):
        v = int_format % v
    ent = f"{k}-{v}" if len(v) > 0 else k
    return ent


@lru_cache(maxsize=8)
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
            key, val = part, ""
        entities[key] = val

    for k, v in zip(["datatype", "suffix", "ext"], [datatype, suffix, ext]):
        if v is not None:
            entities[k] = v
    return entities
