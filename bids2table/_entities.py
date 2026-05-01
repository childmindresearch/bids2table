"""Utilities for parsing and (minimal) validation of BIDS entities.

Uses the BIDS schema for validation.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import pyarrow as pa

from ._logging import setup_logger
from ._schema import SchemaAdapter

BIDSValue = str | int

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
    entities: dict[str, str] = {}

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
    schema: SchemaAdapter | pa.Schema | None = None,
) -> tuple[dict[str, BIDSValue], dict[str, Any]]:
    """Validate BIDS entities against a schema.

    Validates the type and allowed values of each entity against the BIDS schema.

    Args:
        entities: dict mapping BIDS keys to unvalidated entities.
        schema: A `SchemaAdapter`, a `pa.Schema` (e.g. inside a worker process), or
            None to use the module-level default.

    Returns:
        A tuple of `(valid_entities, extra_entities)`, where `valid_entities` is a
            mapping of valid BIDS keys to type-casted values, and `extra_entities` a
            mapping of any leftover entity mappings that didn't match a known entity or
            failed validation.
    """
    schema_adapter = SchemaAdapter.load(schema)

    valid_entities: dict[str, BIDSValue] = {}
    extra_entities: dict[str, str] = {}

    for name, value in entities.items():
        if entity := schema_adapter.name_entity_map.get(name):
            cfg = schema_adapter.entity_schema[entity]
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
