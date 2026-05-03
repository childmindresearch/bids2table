"""BIDS schema adapter and pyarrow-metadata round-trip utilities.

This module owns the loading and representation of the BIDS schema for
the rest of bids2table. See
docs/superpowers/specs/2026-05-03-bids-schema-adapter-design.md for the
full design.
"""

import json
from typing import Any


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
