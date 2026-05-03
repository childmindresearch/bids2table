import functools
from dataclasses import FrozenInstanceError

import bidsschematools.schema
import pytest

from bids2table._schema import (
    BIDSSchemaAdapter,
    _build_adapter_from_namespace,
    decode_metadata,
    encode_metadata,
)


@pytest.mark.parametrize(
    "metadata",
    [
        {"name": "sub"},
        {"name": "sub", "format": "label"},
        {"name": "task", "format": "label", "enum": ["rest", "task"]},
        {"name": "run", "format": "index"},
        {"description": "Hello world"},
        {"display_name": "Subject", "format": "label", "enum": ["A", "B", "C"]},
    ],
)
def test_metadata_roundtrip(metadata):
    encoded = encode_metadata(metadata)
    assert all(isinstance(k, bytes) for k in encoded)
    assert all(isinstance(v, bytes) for v in encoded.values())
    decoded = decode_metadata(encoded)
    assert decoded == metadata


def test_encode_keeps_strings_verbatim():
    encoded = encode_metadata({"name": "sub"})
    assert encoded == {b"name": b"sub"}


def test_encode_json_encodes_non_strings():
    encoded = encode_metadata({"enum": ["a", "b"]})
    assert encoded == {b"enum": b'["a", "b"]'}


def _adapter(versions=("1.0.0", "0.1.0"), entities=None):
    return BIDSSchemaAdapter(
        bids_version=versions[0],
        schema_version=versions[1],
        entity_schema=entities or {"sub": {"name": "sub", "format": "label"}},
    )


def test_adapter_equal_when_all_fields_equal():
    a = _adapter()
    b = _adapter()
    assert a == b
    assert hash(a) == hash(b)


def test_adapter_unequal_when_entity_schema_differs():
    a = _adapter(entities={"sub": {"name": "sub"}})
    b = _adapter(entities={"task": {"name": "task"}})
    assert a != b
    # Same versions → same hash bucket; eq fall-through distinguishes them.
    assert hash(a) == hash(b)


def test_adapter_unequal_and_different_hash_when_versions_differ():
    a = _adapter(versions=("1.0.0", "0.1.0"))
    b = _adapter(versions=("2.0.0", "0.1.0"))
    assert a != b
    assert hash(a) != hash(b)


def test_adapter_lru_cache_distinguishes_same_version_different_content():
    """A function memoized on the adapter must not collide across distinct
    adapters that happen to share versions."""

    @functools.lru_cache
    def derive(adapter: BIDSSchemaAdapter) -> int:
        return id(adapter.entity_schema)

    a = _adapter(entities={"sub": {"name": "sub"}})
    b = _adapter(entities={"task": {"name": "task"}})
    assert derive(a) != derive(b)
    # And cache hit on identical adapter:
    assert derive(a) == derive(_adapter(entities={"sub": {"name": "sub"}}))


def test_adapter_is_frozen():
    a = _adapter()
    with pytest.raises(FrozenInstanceError):
        a.bids_version = "9.9.9"  # type: ignore[misc]


def test_build_adapter_from_namespace_uses_versions_and_entities():
    ns = bidsschematools.schema.load_schema()
    adapter = _build_adapter_from_namespace(ns)

    assert adapter.bids_version == ns["bids_version"]
    assert adapter.schema_version == ns["schema_version"]

    # All entities listed in rules.entities are present.
    for entity in ns.rules.entities:
        assert entity in adapter.entity_schema

    # Special entities are added.
    for special in ("datatype", "suffix", "extension"):
        assert special in adapter.entity_schema


def test_load_bids_schema_default_is_cached():
    from bids2table._schema import load_bids_schema

    a = load_bids_schema()
    b = load_bids_schema(None)
    assert a is b


def test_load_bids_schema_namespace_path_returns_equal_but_fresh():
    from bids2table._schema import load_bids_schema

    ns = bidsschematools.schema.load_schema()
    a = load_bids_schema(ns)
    b = load_bids_schema(ns)
    # Equal by value, distinct by identity (no Namespace caching).
    assert a == b
    assert a is not b


def test_load_bids_schema_rejects_unsupported_type():
    from bids2table._schema import load_bids_schema

    with pytest.raises(TypeError):
        load_bids_schema(42)  # type: ignore[arg-type]
