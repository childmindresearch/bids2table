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
        metadata.update(
            {k: v if isinstance(v, str) else json.dumps(v) for k, v in cfg.items()}
        )
        fields.append(pa.field(name, dtype, metadata=metadata))
    schema_metadata = {"bids_version": bids_version, "schema_version": schema_version}
    return pa.schema(fields, metadata=schema_metadata)


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
