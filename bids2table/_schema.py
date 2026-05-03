"""BIDS schema adapter and pyarrow-metadata round-trip utilities.

This module owns the loading and representation of the BIDS schema for
the rest of bids2table. See
docs/superpowers/specs/2026-05-03-bids-schema-adapter-design.md for the
full design.
"""

import json
from dataclasses import dataclass, field
from typing import Any

from bidsschematools.types import Namespace


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
    """Internal value object holding the BIDS schema components bids2table uses.

    `entity_schema` is excluded from `__hash__` (its dict value is unhashable)
    but included in `__eq__`. This means two adapters with the same
    (bids_version, schema_version) hash identically; structural equality
    falls through to compare `entity_schema`. `lru_cache` therefore treats
    same-version-different-content adapters as distinct entries.

    Not part of the public API.
    """

    bids_version: str
    schema_version: str
    entity_schema: dict[str, dict[str, Any]] = field(hash=False)


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
    return BIDSSchemaAdapter(
        bids_version=schema["bids_version"],
        schema_version=schema["schema_version"],
        entity_schema=entity_schema,
    )
