"""BIDS schema adapter and pyarrow-metadata round-trip utilities.

This module owns the loading and representation of the BIDS schema for
the rest of bids2table. See
docs/superpowers/specs/2026-05-03-bids-schema-adapter-design.md for the
full design.
"""

import json
import os
from collections import deque
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, TypeAlias

import bidsschematools.schema
import pyarrow as pa
from bidsschematools.types import Namespace

from bids2table._pathlib import PathT


def encode_metadata(metadata: dict[str, Any]) -> dict[bytes, bytes]:
    """Encode a metadata dict for use as `pa.field(metadata=...)`.

    String values are stored verbatim (encoded to bytes). Non-string
    values are JSON-encoded then encoded to bytes.
    """
    return {
        k.encode(): (v if isinstance(v, str) else json.dumps(v)).encode()
        for k, v in metadata.items()
    }


def decode_metadata(metadata: dict[bytes, bytes]) -> dict[str, Any]:
    """Inverse of `encode_metadata`.

    For each value, attempts `json.loads` and falls back to the literal
    decoded string. Round-trips correctly for every value type used by
    bids2table's entity-config metadata in practice (`name`, `entity`,
    `format`, `enum`, `description`, `display_name`).

    Caveat: a `str` value that happens to be valid JSON (e.g. literal
    "true", "null", or a bare numeric string) decodes to the parsed JSON
    value rather than the original string. None of the BIDS entity-config
    fields hit that case.
    """
    out: dict[str, Any] = {}
    for k, v in metadata.items():
        s = v.decode()
        try:
            out[k.decode()] = json.loads(s)
        except json.JSONDecodeError:
            out[k.decode()] = s
    return out


@dataclass(frozen=True)
class BIDSSchemaAdapter:
    """BIDS schema components used by bids2table.

    Hashed only on ``(bids_version, schema_version)`` — ``lru_cache`` treats
    same-version adapters as identical.  Dict fields are ``hash=False`` but
    included in ``__eq__``, so same-version-different-content adapters are
    still distinct cache entries.

    ``format_patterns`` holds only entity-relevant patterns (``"label"``,
    ``"index"``) extracted from ``schema.objects.formats``.  More can be
    adopted if the schema adds them.

    Not part of the public API.
    """

    bids_version: str
    schema_version: str
    entity_schema: dict[str, dict[str, Any]] = field(hash=False)
    rules: dict[str, Any] = field(default_factory=dict, hash=False)
    format_patterns: dict[str, str] = field(default_factory=dict, hash=False)


_BIDS_SPECIAL_ENTITY_SCHEMA: dict[str, dict[str, Any]] = {
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


def _build_adapter_from_namespace(schema: Namespace) -> BIDSSchemaAdapter:
    """Build a `BIDSSchemaAdapter` from a loaded `bidsschematools` Namespace."""
    entity_schema = {
        entity: schema.objects.entities[entity].to_dict()
        for entity in schema.rules.entities
    }
    entity_schema.update(_BIDS_SPECIAL_ENTITY_SCHEMA)

    # Extract only entity-relevant format patterns.
    format_patterns = {k: schema.objects.formats[k].pattern for k in ("label", "index")}
    format_patterns["special"] = ".+"

    return BIDSSchemaAdapter(
        bids_version=schema["bids_version"],
        schema_version=schema["schema_version"],
        entity_schema=entity_schema,
        rules=schema["rules"].to_dict(),
        format_patterns=format_patterns,
    )


SchemaSpec: TypeAlias = Namespace | str | PathT | None  # noqa: UP040 - req'd for py311


@lru_cache
def _load_from_path(path: str | PathT | None) -> BIDSSchemaAdapter:
    """Load and cache a BIDSSchemaAdapter from a path or the default schema."""
    schema = bidsschematools.schema.load_schema(path)
    return _build_adapter_from_namespace(schema)


def load_bids_schema(spec: SchemaSpec = None) -> BIDSSchemaAdapter:
    """Resolve a `SchemaSpec` to a `BIDSSchemaAdapter`.

    Hashable specs (`None`, `str`, `PathT`) hit a memoized loader.
    `Namespace` instances fall through to a fresh build per call;
    `Namespace` is not stably hashable and the caller has already paid
    the load cost.
    """
    if isinstance(spec, Namespace):
        return _build_adapter_from_namespace(spec)
    if spec is None or isinstance(spec, str | os.PathLike):
        return _load_from_path(spec)
    raise TypeError(
        f"schema must be Namespace | str | PathT | None, got {type(spec).__name__}"
    )


_BIDS_FORMAT_ARROW_DTYPE_MAP: dict[str, pa.DataType] = {
    "index": pa.int32(),
    "label": pa.string(),
    "special": pa.string(),
}


@lru_cache
def entity_arrow_schema(adapter: BIDSSchemaAdapter) -> pa.Schema:
    """Construct a `pa.Schema` of the BIDS entity columns from `adapter`.

    Per-field metadata carries the long entity name and the entity config
    so that workers receiving only `pa.Schema` can reconstruct the lookups
    they need (see `_lookups_from_arrow` in _entities.py).
    """
    fields = []
    for entity, cfg in adapter.entity_schema.items():
        name = cfg["name"]
        dtype = _BIDS_FORMAT_ARROW_DTYPE_MAP[cfg["format"]]
        metadata = encode_metadata({"entity": entity, **cfg})
        fields.append(pa.field(name, dtype, metadata=metadata))
    schema_metadata = encode_metadata(
        {
            "bids_version": adapter.bids_version,
            "schema_version": adapter.schema_version,
        }
    )
    return pa.schema(fields, metadata=schema_metadata)


def _char_class_for(adapter: BIDSSchemaAdapter, fmt: str) -> str:
    """Return the character class for a BIDS format name, defaulting to `special`."""
    return adapter.format_patterns.get(fmt, adapter.format_patterns["special"])


def get_dataset_types(adapter: BIDSSchemaAdapter) -> tuple[str, ...]:
    """Return the dataset types defined by the BIDS schema.

    Args:
        adapter: A ``BIDSSchemaAdapter`` containing the schema rules.

    Returns:
        A tuple of dataset type strings (e.g., ``("study", "raw", "derivative")``).
    """
    return tuple(adapter.rules["directories"].keys())


def get_json_data_suffixes(adapter: BIDSSchemaAdapter) -> frozenset[str]:
    """Return suffixes whose JSON files are actual data, not sidecar metadata.

    Scans ``rules.files`` and ``rules.tabular_data`` (plain dicts).
    A suffix is included only if its extensions contain ``.json`` and
    **nothing other than** ``.json`` / ``.tsv``.  In other words,
    suffixes paired with binary / imaging extensions are excluded —
    their ``.json`` files are sidecars.

    Replaces the previously hardcoded ``{"coordsystem"}`` set.

    Args:
        adapter: A ``BIDSSchemaAdapter`` containing the schema rules.

    Returns:
        A frozenset of suffix strings.
    """
    suffix_extensions: dict[str, set[str]] = {}

    def _collect(node: Any) -> None:  # noqa: ANN401 - rules.to_dict() produces untyped nested dicts
        """Recursively collect (suffix, extensions) from a nested dict."""
        if isinstance(node, dict):
            for suffix in node.get("suffixes", []):
                suffix_extensions.setdefault(suffix, set())
                suffix_extensions[suffix].update(node.get("extensions", []))
            for child in node.values():
                _collect(child)

    _collect(adapter.rules.get("files"))
    _collect(adapter.rules.get("tabular_data"))

    # JSON data files: extensions subset of {.json, .tsv}.
    _text_exts: frozenset[str] = frozenset({".json", ".tsv"})
    return frozenset(
        suffix
        for suffix, exts in suffix_extensions.items()
        if ".json" in exts and exts.issubset(_text_exts)
    )


def get_entity_directory_order(adapter: BIDSSchemaAdapter) -> deque[str]:
    """Return entity prefixes ordered by directory nesting depth.

    Derived from ``adapter.rules["directories"]`` using a breadth-first
    search.  The first element is the outermost (root-level) directory
    entity; subsequent elements are nested deeper.

    Args:
        adapter: A ``BIDSSchemaAdapter`` containing the schema rules.

    Returns:
        A ``deque`` of entity prefix strings
        (e.g., ``deque(['sub', 'tpl', 'ses', 'cohort'])``).
    """
    rules_dirs: dict[str, Any] = adapter.rules.get("directories", {})
    entity_to_name = _entity_name_map(adapter)
    all_dirs = _aggregate_directory_entries(rules_dirs)
    root_subs = _collect_root_subdirs(rules_dirs)
    entity_keys = _bfs_entities(root_subs, all_dirs)
    return deque(entity_to_name.get(e, e) for e in entity_keys)


def _entity_name_map(adapter: BIDSSchemaAdapter) -> dict[str, str]:
    """Map entity type (e.g., ``"subject"``) to short name (``"sub"``)."""
    return {
        entity: cfg.get("name", entity) for entity, cfg in adapter.entity_schema.items()
    }


def _aggregate_directory_entries(rules_dirs: dict[str, Any]) -> dict[str, Any]:
    """Merge directory entries across dataset types so cross-variant lookups succeed."""
    all_dirs: dict[str, Any] = {}
    for _dtype in rules_dirs:
        for key, val in rules_dirs[_dtype].items():
            all_dirs.setdefault(key, val)
    return all_dirs


def _collect_root_subdirs(rules_dirs: dict[str, Any]) -> deque[str]:
    """Seed a BFS queue with the top-level subdirs of every dataset-type root."""
    queue: deque[str] = deque()
    for _dtype in rules_dirs:
        root = rules_dirs[_dtype].get("root")
        if isinstance(root, dict):
            for sub in root.get("subdirs", []):
                if isinstance(sub, str) and sub not in queue:
                    queue.append(sub)
    return queue


def _bfs_entities(queue: deque[str], all_dirs: dict[str, Any]) -> list[str]:
    """BFS through directory entries, returning encountered entity types in order."""
    seen_keys: set[str] = set()
    seen_entities: list[str] = []

    while queue:
        name = queue.popleft()
        if name in seen_keys:
            continue
        seen_keys.add(name)
        entry = all_dirs.get(name)
        if not isinstance(entry, dict):
            continue
        if (entry_entity := entry.get("entity")) and entry_entity not in seen_entities:
            seen_entities.append(entry_entity)
        for sub in _expand_subdirs(entry, seen_keys):
            queue.append(sub)

    return seen_entities


def _expand_subdirs(entry: dict[str, Any], seen: set[str]) -> list[str]:
    """Expand the ``subdirs`` field of a directory entry into new, unseen names."""
    new_subs: list[str] = []
    for sub in entry.get("subdirs", []):
        if isinstance(sub, str) and sub not in seen:
            new_subs.append(sub)
        elif isinstance(sub, dict) and "oneOf" in sub:
            new_subs.extend(alt for alt in sub["oneOf"] if alt not in seen)
    return new_subs
