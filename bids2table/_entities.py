"""Utilities for parsing and (minimal) validation of BIDS entities.

Uses the BIDS schema for validation.
"""

import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import pyarrow as pa

from bids2table._logging import setup_logger
from bids2table._schema import (
    BIDSSchemaAdapter,
    SchemaSpec,
    _char_class_for,
    decode_metadata,
    entity_arrow_schema,
    get_entity_directory_order,
    load_bids_schema,
)

BIDSValue = str | int

_BIDS_FORMAT_PY_TYPE_MAP = {
    "index": int,
    "label": str,
    "special": str,
}

_logger = setup_logger(__package__)


@lru_cache
def _build_datatype_pattern(adapter: BIDSSchemaAdapter) -> re.Pattern[str]:
    """Build a regex matching entity directories followed by a datatype directory.

    The datatype is the first non-entity directory in a BIDS path. This pattern
    matches one or more entity directories (e.g. sub-A01, ses-B02) followed by
    a datatype directory (e.g. func, anat), capturing the datatype name.

    Note:
        Directory entities are identified via `get_entity_directory_order`; their
        value classes come from `adapter.format_patterns`. Only entities that
        appear in the directory order are included as alternatives.

    Args:
        adapter: A ``BIDSSchemaAdapter`` with entity and format definitions.

    Returns:
        A compiled regex that captures the datatype name from a BIDS path.

    Raises:
        ValueError: If the schema contains no directory entities.
    """
    dir_names = set(get_entity_directory_order(adapter))
    alts = []
    for entity, cfg in adapter.entity_schema.items():
        if entity in ("datatype", "suffix", "extension"):
            continue
        name = cfg.get("name", entity)
        if name not in dir_names:
            continue
        char_class = _char_class_for(adapter, cfg.get("format", "special"))
        alts.append(rf"{name}-{char_class}[/\\]")
    if not alts:
        raise ValueError("No directory entities found in BIDS schema")
    return re.compile(rf"(?:{'|'.join(alts)})+([a-z]+)[/\\]")


def _parse_bids_datatype(path: Path, adapter: BIDSSchemaAdapter) -> str | None:
    """Parse BIDS datatype from file path.

    Uses `re.search`, so the entity-directory + datatype pattern can appear
    anywhere in the path (e.g. after a dataset name prefix).

    Args:
        path: BIDS file path.
        adapter: A ``BIDSSchemaAdapter`` for building the datatype pattern.

    Returns:
        The datatype name, or ``None`` if not found.

    Raises:
        ValueError: If the schema contains no directory entities (propagated
            from `_build_datatype_pattern`).
    """
    pattern = _build_datatype_pattern(adapter)
    match = pattern.search(str(path))
    return match.group(1) if match is not None else None


def parse_bids_entities(
    path: str | Path, *, schema: SchemaSpec = None
) -> dict[str, str]:
    """Parse entities from BIDS file path.

    Parses all BIDS filename `"{key}-{value}"` entities as well as special entities:
    datatype, suffix, ext (extension). Does not validate entities or cast to types.

    Args:
        path: BIDS path to parse.
        schema: Optional BIDS schema. If ``None``, uses the default schema.

    Returns:
        A dict mapping BIDS entity keys to values.

    Raises:
        TypeError: If `schema` is not a valid `SchemaSpec`.
        ValueError: If the schema contains no directory entities.
    """
    if isinstance(path, str):
        path = Path(path)
    adapter = load_bids_schema(schema)
    return _cache_parse_bids_entities(path, adapter)


@lru_cache
def _cache_parse_bids_entities(
    path: Path, adapter: BIDSSchemaAdapter | None = None
) -> dict[str, str]:
    """Cached entity parsing.

    Args:
        path: BIDS file path.
        adapter: Optional ``BIDSSchemaAdapter``. If ``None``, uses the default schema.

    Returns:
        A dict mapping BIDS entity keys to values.

    Raises:
        ValueError: If the schema contains no directory entities.
    """
    if adapter is None:
        adapter = load_bids_schema()
    entities: dict[str, str] = {}

    filename = path.name
    parts = filename.split("_")

    datatype = _parse_bids_datatype(path, adapter)

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

    entities |= {
        k: v
        for k, v in zip(
            ["datatype", "suffix", "ext"], [datatype, suffix, ext], strict=True
        )
        if v is not None
    }
    return entities


def validate_bids_entities(
    entities: dict[str, Any], *, schema: SchemaSpec = None
) -> tuple[dict[str, BIDSValue], dict[str, Any]]:
    """Validate BIDS entities against a BIDS schema.

    Args:
        entities: dict mapping BIDS keys to unvalidated entities
        schema: optional `SchemaSpec` (`Namespace | str | PathT | None`).
            `None` uses the default BIDS schema bundled with bidsschematools.

    Returns:
        `(valid_entities, extra_entities)` — valid entities cast to the
        declared type, plus any leftover entries that did not match a
        known entity or failed validation.
    """
    adapter = load_bids_schema(schema)
    pa_schema = entity_arrow_schema(adapter)
    return _pyarrow_validate_entities(entities, pa_schema=pa_schema)


def _pyarrow_validate_entities(
    entities: dict[str, Any], *, pa_schema: pa.Schema
) -> tuple[dict[str, BIDSValue], dict[str, Any]]:
    """Pyarrow-tier validation. Workers call this directly with a `pa.Schema`."""
    name_to_entity, entity_cfg = _lookups_from_arrow(pa_schema)
    valid_entities: dict[str, BIDSValue] = {}
    extra_entities: dict[str, Any] = {}

    for name, value in entities.items():
        if name in name_to_entity:
            entity = name_to_entity[name]
            cfg = entity_cfg[entity]
            typ = _BIDS_FORMAT_PY_TYPE_MAP[cfg["format"]]

            try:
                value = typ(value)
            except ValueError:
                _logger.warning(
                    f"Unable to coerce {value!r} to type {typ} for entity '{name}'.",
                )
                extra_entities[name] = value
                continue

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


@lru_cache
def _lookups_from_arrow(
    pa_schema: pa.Schema,
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    """Reconstruct (name_entity_map, entity_schema) from a `pa.Schema`.

    Used inside `_pyarrow_validate_entities`. Filters out non-entity fields
    (those without an "entity" key in their decoded metadata). `pa.Schema`
    is hashable, so `lru_cache` keys correctly. The cache lives in the
    worker process; one field-walk per worker per distinct schema.
    """
    name_entity_map: dict[str, str] = {}
    entity_schema: dict[str, dict[str, Any]] = {}
    for field in pa_schema:
        meta = decode_metadata(field.metadata or {})
        if long_entity := meta.pop("entity", None):
            name_entity_map[field.name] = long_entity
            entity_schema[long_entity] = meta
    return name_entity_map, entity_schema


def format_bids_path(
    entities: dict[str, Any], int_format: str = "%d", schema: SchemaSpec = None
) -> Path:
    """Construct a formatted BIDS path from an entities dict.

    Directory entities (e.g. ``sub``, ``ses``, ``tpl``) become path
    directories and are also repeated in the file name, per the BIDS
    convention. The special entities are handled by their role: ``datatype``
    is the innermost directory, while ``suffix`` and ``ext`` are appended to
    the file name. All other entities are formatted into the file name.

    Args:
        entities: dict mapping BIDS entity names to values.
        int_format: format string for integer (index) BIDS values.
        schema: optional BIDS schema. If ``None``, uses the default schema.

    Returns:
        A formatted `Path` instance.
    """
    adapter = load_bids_schema(schema)
    return _format_bids_path(entities, int_format, adapter)


def _format_bids_path(
    entities: dict[str, Any], int_format: str, adapter: BIDSSchemaAdapter
) -> Path:
    """Build a BIDS path from entities using a resolved `BIDSSchemaAdapter`."""
    dir_order = get_entity_directory_order(adapter)
    special = {
        cfg.get("name", entity)
        for entity, cfg in adapter.entity_schema.items()
        if cfg.get("format") == "special"
    }

    # File name: all non-special entities ordered per BIDS convention
    name_parts = []
    for name, value in entities.items():
        if name not in special:
            if isinstance(value, int):
                value = int_format % value
            name_parts.append(f"{name}-{value}")
    name = "_".join(name_parts)

    if suffix := entities.get("suffix"):
        name += f"_{suffix}"
    if ext := entities.get("ext"):
        name += ext

    # Prepend parent directories, innermost to outermost.
    path = Path(name)
    if datatype := entities.get("datatype"):
        path = Path(datatype) / path
    for dir_entity in reversed(dir_order):
        if dir_entity in entities:
            path = Path(f"{dir_entity}-{entities[dir_entity]}") / path
    return path


def get_root_entity_types(adapter: BIDSSchemaAdapter) -> tuple[str, ...]:
    """Return entity prefixes that form root-level BIDS directories.

    Derived from the BFS directory order — ``"sub"`` (subject) is always
    first; ``"tpl"`` (template) appears in derivatives schemas.

    Args:
        adapter: A ``BIDSSchemaAdapter``.

    Returns:
        A tuple of prefix strings (e.g., ``("sub", "tpl")``).
    """
    order = get_entity_directory_order(adapter)
    roots: set[str] = set()
    for prefix in order:
        if prefix in roots:
            continue
        if prefix in ("sub", "tpl"):
            roots.add(prefix)
    return tuple(sorted(roots))


def get_file_entity_prefixes(adapter: BIDSSchemaAdapter) -> tuple[str, ...]:
    """Return entity prefixes valid in filenames (non-directory, non-special).

    Excludes all directory entities (``subject``, ``session``, ``template``,
    etc.) and special entities (``datatype``, ``suffix``, ``extension``).

    Args:
        adapter: A ``BIDSSchemaAdapter``.

    Returns:
        A tuple of short name strings.
    """
    dir_names = set(get_entity_directory_order(adapter))
    special = {"datatype", "suffix", "extension"}
    prefixes: list[str] = []
    for entity, cfg in adapter.entity_schema.items():
        if entity in special:
            continue
        name = cfg.get("name", entity)
        if name in dir_names:
            continue
        prefixes.append(name)
    return tuple(sorted(prefixes))
