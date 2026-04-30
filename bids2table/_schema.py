"""BIDS schema value object and module-level default schema state."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import cached_property
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


class _NoSource:
    """Sentinel for BIDSSchema instances with no reloadable source."""

    _instance: "_NoSource | None" = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "<NO_SOURCE>"


_NO_SOURCE = _NoSource()


@dataclass(frozen=True)
class BIDSSchema:
    """Encapsulates a BIDS schema and its derived Arrow representation.

    Use `BIDSSchema.from_path`, `from_namespace`, `from_arrow`, or `prepare`
    rather than constructing directly.
    """

    arrow_schema: pa.Schema
    _entity_schema: dict[str, dict[str, Any]] = field(repr=False)
    _name_entity_map: dict[str, str] = field(repr=False)
    _source: str | Path | None | _NoSource = field(default=_NO_SOURCE)

    @classmethod
    def from_path(cls, path: str | Path | None) -> "BIDSSchema":
        ns = bidsschematools.schema.load_schema(path)
        return cls._build(ns, source=path)

    @classmethod
    def from_namespace(cls, ns: Namespace) -> "BIDSSchema":
        """Build from an already-loaded bidsschematools Namespace.

        Sets `_source=_NO_SOURCE`, so `.bids_schema` will return None.
        """
        return cls._build(ns, source=_NO_SOURCE)

    @classmethod
    def from_arrow(cls, arrow_schema: pa.Schema) -> "BIDSSchema":
        """Reconstruct from a pa.Schema previously emitted by bids2table.

        `bids_schema` will return None for instances built this way.
        """
        entity_schema, name_entity_map = _entity_lookups_from_arrow(arrow_schema)
        return cls(
            arrow_schema=arrow_schema,
            _entity_schema=entity_schema,
            _name_entity_map=name_entity_map,
            _source=_NO_SOURCE,
        )

    @classmethod
    def prepare(
        cls, obj: "BIDSSchema | pa.Schema | Namespace | str | Path | None"
    ) -> "BIDSSchema":
        """Polymorphic constructor.

        - `BIDSSchema` -> returned unchanged
        - `pa.Schema` -> via `from_arrow`
        - `Namespace` -> via `from_namespace`
        - `str` / `Path` / `None` -> via `from_path`
        """
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, pa.Schema):
            return cls.from_arrow(obj)
        if isinstance(obj, Namespace):
            return cls.from_namespace(obj)
        if obj is None or isinstance(obj, (str, Path)):
            return cls.from_path(obj)
        raise TypeError(
            "Expected BIDSSchema | pa.Schema | Namespace | str | Path | None, "
            f"got {type(obj).__name__}"
        )

    @classmethod
    def _build(
        cls, ns: Namespace, source: str | Path | None | _NoSource
    ) -> "BIDSSchema":
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
            _entity_schema=entity_schema,
            _name_entity_map=name_entity_map,
            _source=source,
        )

    @cached_property
    def bids_schema(self) -> Namespace | None:
        if isinstance(self._source, _NoSource):
            return None
        return bidsschematools.schema.load_schema(self._source)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("bids_schema", None)  # drop materialized cached_property
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        # frozen=True blocks __setattr__; bulk __dict__.update bypasses it.
        self.__dict__.update(state)
