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

# Matches sub-directory after subject and (optionally) session directories.
# Must be all lowercase. Rebuilt in set_bids_schema() once schema is loaded.
_BIDS_DATATYPE_PATTERN: re.Pattern | None = None

_logger = setup_logger(__package__)


def set_bids_schema(path: str | Path | None = None) -> None:
    """Set the BIDS schema."""
    global _BIDS_SCHEMA, _BIDS_ENTITY_SCHEMA, _BIDS_NAME_ENTITY_MAP
    global _BIDS_ENTITY_ARROW_SCHEMA, _BIDS_DATATYPE_PATTERN

    schema = bidsschematools.schema.load_schema(path)
    entity_schema = {
        entity: schema.objects.entities[entity].to_dict()
        for entity in schema.objects.entities
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

    # Build datatype pattern from entity names (e.g. sub, ses).
    sub_name = get_entity_name("subject")
    ses_name = get_entity_name("session")
    _BIDS_DATATYPE_PATTERN = re.compile(
        rf"{sub_name}-[a-zA-Z0-9]+(?:[/\\]{ses_name}-[a-zA-Z0-9]+)?[/\\]([a-z]+)[/\\]"
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
        dtype = _BIDS_FORMAT_ARROW_DTYPE_MAP.get(cfg["format"], pa.string())
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
    if _BIDS_DATATYPE_PATTERN is None:
        return None
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


def _get_entity_prefix_directory_order() -> list[str]:
    """Return entity name prefixes (e.g. 'sub', 'ses') in schema-defined
    directory hierarchy order (shallower depth first).

    Derived from the schema's ``rules.directories`` nesting: subject and
    template are depth-1 children of root, session and cohort depth-2,
    datatype depth-3.  Entity types that never appear as directories are
    omitted.
    """
    depths: dict[str, int] = {}
    for dtype in get_all_dataset_types():
        rules_raw = _BIDS_SCHEMA.get("rules", {}).get("directories", {}).get(dtype, {})
        # bidsschematools may return Namespace objects; normalise to plain dict.
        rules: dict[str, Any] = _ensure_dict(rules_raw)
        queue: list[tuple[str, int]] = [("root", 0)]
        visited: set[str] = set()
        while queue:
            rule_name, depth = queue.pop(0)
            if rule_name in visited:
                continue
            visited.add(rule_name)
            rule = rules.get(rule_name, {})
            entity = rule.get("entity")
            if entity:
                ename = get_entity_name(entity)
                if ename and (ename not in depths or depth < depths[ename]):
                    depths[ename] = depth
            for subdir in rule.get("subdirs", []):
                for name in _get_subdir_names([subdir]):
                    queue.append((name, depth + 1))
    return sorted(depths, key=lambda n: depths[n])


def format_bids_path(entities: dict[str, Any], int_format: str = "%d") -> Path:
    """Construct a formatted BIDS path from entities dict.

    Args:
        entities: dict mapping BIDS keys to values.
        int_format: format string for integer (index) BIDS values.

    Returns:
        A formatted ``Path`` instance.
    """
    special = {cfg["name"] for cfg in _BIDS_SPECIAL_ENTITY_SCHEMA.values()}

    # Formatted key-value entities.
    entities_fmt = []
    for name, value in entities.items():
        if name not in special:
            if isinstance(value, int):
                value = int_format % value
            entities_fmt.append(f"{name}-{value}")
    filename = "_".join(entities_fmt)

    # Append suffix and extension.
    if suffix := entities.get("suffix"):
        filename += f"_{suffix}"
    if ext := entities.get("ext"):
        filename += ext

    path = Path(filename)

    # Prepend schema-derived directory hierarchy.
    dir_order = _get_entity_prefix_directory_order()
    dir_entities = [n for n in dir_order if n in entities]
    if datatype := entities.get("datatype"):
        path = datatype / path
    for entity_name in reversed(dir_entities):
        path = f"{entity_name}-{entities[entity_name]}" / path
    return path


def get_entity_name(entity_type: str) -> str:
    """Get the short name prefix for a BIDS entity (e.g. 'sub' for 'subject')."""
    ent = _BIDS_ENTITY_SCHEMA.get(entity_type)
    if ent is not None:
        return ent.get("name", "")
    try:
        ent = _BIDS_SCHEMA["objects"]["entities"].get(entity_type)
        if ent is not None:
            return ent.get("name", "")
    except (KeyError, AttributeError):
        pass
    return ""


def get_entity_pattern(entity_type: str) -> str:
    """Get the regex pattern for a BIDS entity directory name (e.g. 'sub-[a-zA-Z0-9]+')."""
    name = get_entity_name(entity_type)
    if not name:
        return ""
    return f"{name}-[a-zA-Z0-9]+"


def get_entity_glob_pattern(entity_type: str) -> str:
    """Get the glob pattern for a BIDS entity directory (e.g. 'sub-*' for 'subject')."""
    name = get_entity_name(entity_type)
    if not name:
        return ""
    return f"{name}-*"


def get_all_dataset_types() -> tuple[str, ...]:
    """Get all BIDS dataset types defined in the schema (e.g. 'raw', 'derivative', 'study')."""
    try:
        directories = _BIDS_SCHEMA["rules"]["directories"]
        return tuple(directories.keys())
    except (KeyError, AttributeError):
        return ()


def _ensure_dict(obj: Any) -> dict:
    """Convert a bidsschematools Namespace to a plain dict."""
    return obj.to_dict() if hasattr(obj, "to_dict") else obj


def _get_subdir_names(subdirs: list) -> list[str]:
    """Normalize a subdirs list into flat name strings, expanding ``oneOf`` entries."""
    names: list[str] = []
    for entry in subdirs:
        if isinstance(entry, dict):
            names.extend(entry.get("oneOf", []))
        else:
            names.append(entry)
    return names


def _get_json_data_suffixes() -> frozenset[str]:
    """Collect suffixes for which ``.json`` is the only valid extension.

    These are data files (not sidecars), e.g. ``coordsystem.json``.
    Derived from the schema's ``rules.files``.
    """
    from collections import defaultdict

    suffix_exts: dict[str, set[str]] = defaultdict(set)

    def _walk(node) -> None:
        if hasattr(node, "items"):
            items = node.items()
        elif hasattr(node, "to_dict"):
            items = _ensure_dict(node).items()
        else:
            return
        for _, v in items:
            if v is None:
                continue
            if hasattr(v, "get") or isinstance(v, dict):
                inner = _ensure_dict(v)
                if isinstance(inner, dict):
                    sufs = inner.get("suffixes", [])
                    exts = inner.get("extensions", [])
                    if sufs and exts:
                        for s in sufs:
                            suffix_exts[s].update(exts)
            _walk(v)

    try:
        files_rules = _BIDS_SCHEMA["rules"]["files"]
        _walk(files_rules)
    except (KeyError, AttributeError):
        return frozenset()

    return frozenset(
        s for s, exts in suffix_exts.items()
        if ".json" in exts and all(e == ".json" for e in exts)
    )


def get_all_root_entity_types() -> tuple[str, ...]:
    """Return deduplicated root-level entity types across all dataset types.

    For example, across raw/derivative/study this returns ``("subject", "template")``.
    """
    seen: set[str] = set()
    result: list[str] = []
    for dtype in get_all_dataset_types():
        for et in get_entity_child_dirs(dtype, "root"):
            if et not in seen:
                seen.add(et)
                result.append(et)
    return tuple(result)


def get_entity_child_dirs(dataset_type: str, parent_rule: str = "root") -> list[str]:
    """Get entity types valid as child directories of a parent in a dataset type.

    Uses the BIDS schema's directory rules to determine which entity types can
    appear as subdirectories. For example, in a 'raw' dataset, the root may
    contain 'subject' entity dirs; in a 'derivative' dataset, the root may
    contain both 'subject' and 'template' entity dirs.

    Args:
        dataset_type: BIDS dataset type (e.g. 'raw', 'derivative', 'study').
        parent_rule: Name of the parent directory rule (e.g. 'root', 'subject').

    Returns:
        List of entity type names (e.g. ['subject'], ['subject', 'template']).
    """
    try:
        directories = _BIDS_SCHEMA["rules"]["directories"]
        dir_rules = directories.get(dataset_type, {})
    except (KeyError, AttributeError):
        return []

    if not hasattr(dir_rules, "get"):
        return []

    parent = dir_rules.get(parent_rule, {})
    if not hasattr(parent, "get"):
        return []

    subdirs = parent.get("subdirs", [])
    entity_types = []

    for name in _get_subdir_names(subdirs):
        child = dir_rules.get(name, {})
        entity = child.get("entity") if hasattr(child, "get") else None
        if entity:
            entity_types.append(entity)

    return entity_types


def get_file_entity_prefixes() -> tuple[str, ...]:
    """Get entity name prefixes that BIDS filenames can start with (e.g. 'sub', 'tpl').

    Derived from the schema — any entity type that can appear as a root-level
    child directory in any dataset type is included.
    """
    prefixes: set[str] = set()
    for et in get_all_root_entity_types():
        name = get_entity_name(et)
        if name:
            prefixes.add(name)
    return tuple(sorted(prefixes))


# Initialize the default BIDS schema.
set_bids_schema()
