"""BIDS schema value object and module-level default schema state."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import bidsschematools.schema
import pyarrow as pa
from bidsschematools.types import Namespace

# "Special" entities not part of the schema's `rules.entities` listing.
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

_BIDS_FORMAT_ARROW_DTYPE_MAP = {
    "index": pa.int32(),
    "label": pa.string(),
    "special": pa.string(),
}


def _build_arrow_schema(
    entity_schema: dict[str, dict[str, Any]],
    bids_version: str,
    schema_version: str,
) -> pa.Schema:
    fields = []
    for entity, cfg in entity_schema.items():
        name = cfg["name"]
        dtype = _BIDS_FORMAT_ARROW_DTYPE_MAP[cfg["format"]]
        metadata = {"entity": entity}
        # Heterogeneous encoding: strings stored raw, non-strings JSON-encoded.
        # _entity_lookups_from_arrow uses json.loads with a fallback; raw string
        # values that happen to be JSON-parseable (e.g. "null", "42") will not
        # round-trip cleanly. The current BIDS entity configs do not contain
        # such values, so this is a known limitation, not an active bug.
        metadata.update(
            {k: v if isinstance(v, str) else json.dumps(v) for k, v in cfg.items()}
        )
        fields.append(pa.field(name, dtype, metadata=metadata))
    schema_metadata = {"bids_version": bids_version, "schema_version": schema_version}
    return pa.schema(fields, metadata=schema_metadata)


def _entity_lookups_from_arrow(
    arrow_schema: pa.Schema,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Reconstruct entity_schema and name_entity_map from arrow field metadata.

    Mirrors the encoding done by `_build_arrow_schema`: each per-field metadata
    dict has an `entity` key (the long entity name) plus the original entity
    config dict, with non-string values JSON-encoded.
    """
    entity_schema: dict[str, dict[str, Any]] = {}
    name_entity_map: dict[str, str] = {}
    for f in arrow_schema:
        if f.metadata is None:
            raise ValueError(
                f"Arrow field {f.name!r} has no metadata; cannot reconstruct "
                "BIDS entity config."
            )
        meta = {k.decode(): v.decode() for k, v in f.metadata.items()}
        if "entity" not in meta:
            raise ValueError(
                f"Arrow field {f.name!r} metadata missing required 'entity' key."
            )
        entity_long = meta.pop("entity")
        cfg: dict[str, Any] = {}
        for k, v in meta.items():
            try:
                cfg[k] = json.loads(v)
            except ValueError:
                cfg[k] = v
        entity_schema[entity_long] = cfg
        if "name" not in cfg:
            raise ValueError(
                f"Arrow field {f.name!r} metadata missing required 'name' key."
            )
        name_entity_map[cfg["name"]] = entity_long
    return entity_schema, name_entity_map


@dataclass(frozen=True)
class SchemaAdapter:
    """Encapsulates a BIDS schema and its derived Arrow representation.

    Use `SchemaAdapter.load` rather than constructing directly.
    """

    arrow_schema: pa.Schema
    entity_schema: dict[str, dict[str, Any]] = field(repr=False)
    name_entity_map: dict[str, str] = field(repr=False)

    @classmethod
    def load(
        cls,
        schema: SchemaAdapter | pa.Schema | Namespace | str | Path | None = None,
    ) -> SchemaAdapter:
        """Polymorphic constructor.

        Existing SchemaAdapters and pyarrow Schemas are passed through with minimal processing.

        Paths and `None` are passed directly to `bidsschematools.schema.load_schema` for loading,
        and the resulting `Namespace` is queried to extract the components used by bids2table.

        A pre-loaded `Namespace` is also accepted, for callers that may want to modify a schema.
        """
        if isinstance(schema, cls):
            return schema
        elif isinstance(schema, pa.Schema):
            entity_schema, name_entity_map = _entity_lookups_from_arrow(schema)
            return cls(
                arrow_schema=schema,
                entity_schema=entity_schema,
                name_entity_map=name_entity_map,
            )

        ns: Namespace = (
            schema
            if isinstance(schema, Namespace)
            else bidsschematools.schema.load_schema(schema)
        )

        entity_schema = {
            entity: ns.objects.entities[entity].to_dict()
            for entity in ns.rules.entities
        }
        entity_schema.update(_BIDS_SPECIAL_ENTITY_SCHEMA)
        name_entity_map = {cfg["name"]: entity for entity, cfg in entity_schema.items()}
        arrow_schema = _build_arrow_schema(
            entity_schema,
            bids_version=ns["bids_version"],
            schema_version=ns["schema_version"],
        )
        return cls(
            arrow_schema=arrow_schema,
            entity_schema=entity_schema,
            name_entity_map=name_entity_map,
        )
