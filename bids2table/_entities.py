"""Utilities for parsing and (minimal) validation of BIDS entities.

Uses the BIDS schema for validation.
"""

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import bidsschematools.schema
import pyarrow as pa
from bidsschematools.types import Namespace

from ._logging import setup_logger

BIDSValue = str | int

# Global BIDS schema namespace.
_BIDS_SCHEMA: Namespace
# Map of entity names to schema metadata.
_BIDS_ENTITY_SCHEMA: dict[str, dict[str, Any]]
# Map of BIDS short names (e.g. 'sub') to long entities ('subject').
_BIDS_NAME_ENTITY_MAP: dict[str, str]

# BIDS schema in Arrow format
_BIDS_ENTITY_ARROW_SCHEMA: pa.Schema

# "Special" entities that are part of the BIDS file name spec but not in the BIDS schema
# (bc they don't follow the '{key}-{value}' format).
_BIDS_SPECIAL_ENTITY_SCHEMA = {
    "datatype": {
        "name": "datatype",
        "display_name": "Data type",
        "description": "A functional group of different types of data.",
        "type": "string",
        "format": "special",
    },
    "suffix": {
        "name": "suffix",
        "display_name": "Suffix",
        "description": "Final part of file name after final '_' and before extension.",
        "type": "string",
        "format": "special",
    },
    "extension": {
        "name": "ext",
        "display_name": "File extension",
        "description": "Full file extension after the left-most period.",
        "type": "string",
        "format": "special",
    },
}

_BIDS_FORMAT_ARROW_DTYPE_MAP = {
    "index": pa.int32(),
    "label": pa.string(),
    "special": pa.string(),
}

_BIDS_FORMAT_PY_TYPE_MAP = {
    "index": int,
    "label": str,
    "special": str,
}

# Matches sub-directory after subject ('sub-abc') and (optionally) session ('ses-01')
# directories. Must be all lowercase.
_BIDS_DATATYPE_PATTERN = re.compile(
    r"sub-[a-zA-Z0-9]+(?:[/\\]ses-[a-zA-Z0-9]+)?[/\\]([a-z]+)[/\\]"
)

_logger = setup_logger(__package__)


def set_bids_schema(path: str | Path | None = None) -> None:
    """Set the BIDS schema."""
    global _BIDS_SCHEMA, _BIDS_ENTITY_SCHEMA, _BIDS_NAME_ENTITY_MAP
    global _BIDS_ENTITY_ARROW_SCHEMA

    schema = bidsschematools.schema.load_schema(path)
    entity_schema = {
        entity: schema.objects.entities[entity].to_dict()
        for entity in schema.rules.entities
    }
    # Also include special extra entities (datatype, suffix, extension).
    entity_schema.update(_BIDS_SPECIAL_ENTITY_SCHEMA)
    name_entity_map = {cfg["name"]: entity for entity, cfg in entity_schema.items()}

    _BIDS_SCHEMA = schema
    _BIDS_ENTITY_SCHEMA = entity_schema
    _BIDS_NAME_ENTITY_MAP = name_entity_map

    _BIDS_ENTITY_ARROW_SCHEMA = _bids_entity_arrow_schema(
        entity_schema,
        bids_version=schema["bids_version"],
        schema_version=schema["schema_version"],
    )


def _bids_entity_arrow_schema(
    entity_schema: dict[str, dict[str, Any]],
    bids_version: str,
    schema_version: str,
) -> pa.Schema:
    """Create Arrow schema from BIDS entity schema."""
    fields = []
    for entity, cfg in entity_schema.items():
        # Use short entity name (e.g. sub) as the field name.
        name = cfg["name"]
        dtype = _BIDS_FORMAT_ARROW_DTYPE_MAP[cfg["format"]]
        # Insert full entity name (e.g. subject) into metadata.
        metadata = {"entity": entity}
        metadata.update(
            {k: v if isinstance(v, str) else json.dumps(v) for k, v in cfg.items()}
        )

        field = pa.field(name, dtype, metadata=metadata)
        fields.append(field)

    metadata = {"bids_version": bids_version, "schema_version": schema_version}
    arrow_schema = pa.schema(fields, metadata=metadata)
    return arrow_schema


def get_bids_schema() -> Namespace:
    """Get the current BIDS schema."""
    return _BIDS_SCHEMA


def get_bids_entity_arrow_schema() -> pa.Schema:
    """Get the current BIDS entity schema in Arrow format."""
    return _BIDS_ENTITY_ARROW_SCHEMA


def parse_bids_entities(path: str | Path) -> dict[str, str]:
    """Parse entities from BIDS file path.

    Parses all BIDS filename `"{key}-{value}"` entities as well as special entities:
    datatype, suffix, ext (extension). Does not validate entities or cast to types.

    Args:
        path: BIDS path to parse.

    Returns:
        A dict mapping BIDS entity keys to values.
    """
    if isinstance(path, str):
        path = Path(path)
    entities = {}

    filename = path.name
    parts = filename.split("_")

    datatype = _parse_bids_datatype(path)

    # Get suffix and extension.
    suffix_ext = parts.pop()
    suffix, dot, ext = suffix_ext.partition(".")
    ext = dot + ext if ext else None

    # Suffix is actually an entity, put back in list.
    if "-" in suffix:
        parts.append(suffix)
        suffix = None

    # Split entities, skipping any that don't contain a '-'.
    for part in parts:
        if "-" in part:
            key, val = part.split("-", maxsplit=1)
            entities[key] = val

    for k, v in zip(["datatype", "suffix", "ext"], [datatype, suffix, ext]):
        if v is not None:
            entities[k] = v
    return entities


# Version with caching to use internally. Decorating the public function loses the
# docstring.
_cache_parse_bids_entities = lru_cache(parse_bids_entities)


def _parse_bids_datatype(path: Path) -> str | None:
    """Parse BIDS datatype from file path.

    Datatype is assumed to be the name of the sub-directory after the subject and
    (optionally) session directories. Returns `None` if no match found.
    """
    match = re.search(_BIDS_DATATYPE_PATTERN, str(path))
    datatype = match.group(1) if match is not None else None
    return datatype


def validate_bids_entities(
    entities: dict[str, Any],
) -> tuple[dict[str, BIDSValue], dict[str, Any]]:
    """Validate BIDS entities.

    Validates the type and allowed values of each entity against the BIDS schema.

    Args:
        entities: dict mapping BIDS keys to unvalidated entities

    Returns:
        A tuple of `(valid_entities, extra_entities)`, where `valid_entities` is a
            mapping of valid BIDS keys to type-casted values, and `extra_entities` a
            mapping of any leftover entity mappings that didn't match a known entity or
            failed validation.
    """
    valid_entities = {}
    extra_entities = {}

    for name, value in entities.items():
        if name in _BIDS_NAME_ENTITY_MAP:
            entity = _BIDS_NAME_ENTITY_MAP[name]
            cfg = _BIDS_ENTITY_SCHEMA[entity]
            typ = _BIDS_FORMAT_PY_TYPE_MAP[cfg["format"]]

            # Cast to target type.
            try:
                value = typ(value)
            except ValueError:
                _logger.warning(
                    f"Unable to coerce {repr(value)} to type {typ} for entity '{name}'.",
                )
                extra_entities[name] = value
                continue

            # Check allowed values.
            if "enum" in cfg and value not in cfg["enum"]:
                _logger.warning(
                    f"Value {value} for entity '{name}' isn't one of the "
                    f"allowed values: {cfg['enum']}.",
                )
                extra_entities[name] = value
                continue

            valid_entities[name] = value
        else:
            extra_entities[name] = value

    return valid_entities, extra_entities


def format_bids_path(entities: dict[str, Any], int_format: str = "%d") -> Path:
    """Construct a formatted BIDS path from entities dict.

    Args:
        entities: dict mapping BIDS keys to values.
        int_format: format string for integer (index) BIDS values.

    Returns:
        A formatted `Path` instance.
    """
    special = {"datatype", "suffix", "ext"}

    # Formatted key-value entities.
    entities_fmt = []
    for name, value in entities.items():
        if name not in special:
            if isinstance(value, int):
                value = int_format % value
            entities_fmt.append(f"{name}-{value}")
    name = "_".join(entities_fmt)

    # Append suffix and extension.
    if suffix := entities.get("suffix"):
        name += f"_{suffix}"
    if ext := entities.get("ext"):
        name += ext

    # Prepend parent directories.
    path = Path(name)
    if datatype := entities.get("datatype"):
        path = datatype / path
    if ses := entities.get("ses"):
        path = f"ses-{ses}" / path
    path = f"sub-{entities['sub']}" / path
    return path


# Initialize the default BIDS schema.
set_bids_schema()
