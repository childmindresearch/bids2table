"""Tests for the BIDS schema adapter, metadata encoding, and Arrow schema helpers."""

import functools
from dataclasses import FrozenInstanceError
from typing import Any

import bidsschematools.schema
import pyarrow as pa
import pytest
from bids2table._schema import (
    BIDSSchemaAdapter,
    _build_adapter_from_namespace,
    decode_metadata,
    encode_metadata,
    entity_arrow_schema,
    load_bids_schema,
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
def test_metadata_roundtrip(metadata: dict[str, Any]):
    """Verify that encoding then decoding metadata round-trips faithfully."""
    encoded = encode_metadata(metadata)
    assert all(isinstance(k, bytes) for k in encoded)
    assert all(isinstance(v, bytes) for v in encoded.values())
    decoded = decode_metadata(encoded)
    assert decoded == metadata


def test_encode_keeps_strings_verbatim():
    """String values are stored as raw bytes, not JSON-encoded."""
    encoded = encode_metadata({"name": "sub"})
    assert encoded == {b"name": b"sub"}


def test_encode_json_encodes_non_strings():
    """Non-string values are JSON-encoded before being turned into bytes."""
    encoded = encode_metadata({"enum": ["a", "b"]})
    assert encoded == {b"enum": b'["a", "b"]'}


def _adapter(
    versions: tuple[str, ...] = ("1.0.0", "0.1.0"),
    entities: dict[str, Any] | None = None,
) -> BIDSSchemaAdapter:
    """Build a ``BIDSSchemaAdapter`` for testing purposes."""
    return BIDSSchemaAdapter(
        bids_version=versions[0],
        schema_version=versions[1],
        entity_schema=entities or {"sub": {"name": "sub", "format": "label"}},
    )


def test_adapter_equal_when_all_fields_equal():
    """Two adapters with identical fields compare equal and hash equally."""
    a = _adapter()
    b = _adapter()
    assert a == b
    assert hash(a) == hash(b)


def test_adapter_unequal_when_entity_schema_differs():
    """Same versions but different entity schemas yield unequal adapters."""
    a = _adapter(entities={"sub": {"name": "sub"}})
    b = _adapter(entities={"task": {"name": "task"}})
    assert a != b
    # Same versions → same hash bucket; eq fall-through distinguishes them.
    assert hash(a) == hash(b)


def test_adapter_unequal_and_different_hash_when_versions_differ():
    """Different versions produce unequal adapters with distinct hashes."""
    a = _adapter(versions=("1.0.0", "0.1.0"))
    b = _adapter(versions=("2.0.0", "0.1.0"))
    assert a != b
    assert hash(a) != hash(b)


def test_adapter_lru_cache_distinguishes_same_version_different_content():
    """Lru cache must not collide for distinct adapters with same versions."""

    @functools.lru_cache
    def derive(adapter: BIDSSchemaAdapter) -> int:
        return id(adapter.entity_schema)

    a = _adapter(entities={"sub": {"name": "sub"}})
    b = _adapter(entities={"task": {"name": "task"}})
    assert derive(a) != derive(b)
    # And cache hit on identical adapter:
    assert derive(a) == derive(_adapter(entities={"sub": {"name": "sub"}}))


def test_adapter_is_frozen():
    """The adapter must be immutable after construction."""
    a = _adapter()
    with pytest.raises(FrozenInstanceError):
        a.bids_version = "9.9.9"  # ty: ignore[invalid-assignment]


def test_build_adapter_from_namespace_uses_versions_and_entities():
    """Extract version info and entity dicts from a loaded namespace."""
    ns = bidsschematools.schema.load_schema()
    adapter = _build_adapter_from_namespace(ns)

    assert adapter.bids_version == ns["bids_version"]
    assert adapter.schema_version == ns["schema_version"]

    # All entities listed in rules.entities are present.
    assert set(ns.rules.entities) <= adapter.entity_schema.keys()

    # Special entities are added.
    assert {"datatype", "suffix", "extension"} <= adapter.entity_schema.keys()


def test_load_bids_schema_default_is_cached():
    """Calling ``load_bids_schema`` with default schema returns same instance."""
    a = load_bids_schema()
    b = load_bids_schema(None)
    assert a is b


def test_load_bids_schema_namespace_path_returns_equal_but_fresh():
    """Passing a ``Namespace`` returns equal-but-distinct adapters (no caching)."""
    ns = bidsschematools.schema.load_schema()
    a = load_bids_schema(ns)
    b = load_bids_schema(ns)
    assert a == b
    assert a is not b


def test_load_bids_schema_rejects_unsupported_type():
    """Passing an unsupported schema type raises ``TypeError``."""
    with pytest.raises(TypeError):
        load_bids_schema(42)  # ty: ignore [invalid-argument-type]


def test_entity_arrow_schema_metadata_is_bytes():
    """The Arrow schema carries version metadata as bytes."""
    adapter = load_bids_schema()
    schema = entity_arrow_schema(adapter)
    assert isinstance(schema, pa.Schema)
    assert schema.metadata[b"bids_version"].decode() == adapter.bids_version
    assert schema.metadata[b"schema_version"].decode() == adapter.schema_version


def test_entity_arrow_schema_field_carries_entity_metadata():
    """Each Arrow field embeds entity config metadata."""
    adapter = load_bids_schema()
    schema = entity_arrow_schema(adapter)
    sub_field = schema.field("sub")
    assert sub_field.metadata[b"entity"] == b"subject"


def test_entity_arrow_schema_is_cached():
    """Calling ``entity_arrow_schema`` twice on same adapter returns same object."""
    adapter = load_bids_schema()
    a = entity_arrow_schema(adapter)
    b = entity_arrow_schema(adapter)
    assert a is b


def test_lookups_from_arrow_round_trips_with_adapter():
    """Recover entity config and name-entity map from an Arrow schema."""
    from bids2table._entities import _lookups_from_arrow

    adapter = load_bids_schema()
    schema = entity_arrow_schema(adapter)
    name_entity_map, entity_cfg = _lookups_from_arrow(schema)

    assert entity_cfg == adapter.entity_schema
    assert name_entity_map == {
        cfg["name"]: entity for entity, cfg in adapter.entity_schema.items()
    }


def test_lookups_from_arrow_is_cached():
    """Calling ``_lookups_from_arrow`` twice on same schema returns same object."""
    from bids2table._entities import _lookups_from_arrow

    adapter = load_bids_schema()
    schema = entity_arrow_schema(adapter)
    a = _lookups_from_arrow(schema)
    b = _lookups_from_arrow(schema)
    assert a is b


def test_lookups_from_arrow_skips_non_entity_fields():
    """Non-entity fields (no 'entity' key in metadata) must be skipped."""
    from bids2table._entities import _lookups_from_arrow

    adapter = load_bids_schema()
    entity_schema = entity_arrow_schema(adapter)
    extra = pa.field("dataset", pa.string(), metadata={b"name": b"dataset"})
    full = pa.schema([*list(entity_schema), extra], metadata=entity_schema.metadata)
    name_map, _ = _lookups_from_arrow(full)
    assert "dataset" not in name_map


def test_validate_bids_entities_accepts_schema_kwarg_namespace():
    """Pass a ``Namespace`` as the schema kwarg to ``validate_bids_entities``."""
    from bids2table._entities import validate_bids_entities

    ns = bidsschematools.schema.load_schema()
    valid, extra = validate_bids_entities({"sub": "A01", "task": "rest"}, schema=ns)
    assert valid["sub"] == "A01"
    assert valid["task"] == "rest"
    assert extra == {}


def test_validate_bids_entities_default_schema():
    """Validate entities against default schema and confirm unknown keys go to extra."""
    from bids2table._entities import validate_bids_entities

    valid, extra = validate_bids_entities({"sub": "A01", "unknown": "x"})
    assert valid["sub"] == "A01"
    assert extra == {"unknown": "x"}


def test_pyarrow_validate_entities_takes_only_pa_schema():
    """Validate entities against a PyArrow schema and confirm index coercion."""
    from bids2table._entities import _pyarrow_validate_entities

    adapter = load_bids_schema()
    pa_schema = entity_arrow_schema(adapter)
    valid, extra = _pyarrow_validate_entities({"sub": "A01"}, pa_schema=pa_schema)
    assert valid["sub"] == "A01"
    assert extra == {}


def test_get_arrow_schema_accepts_schema_kwarg():
    """Pass a ``Namespace`` to ``get_arrow_schema`` and verify index fields present."""
    from bids2table._indexing import get_arrow_schema

    ns = bidsschematools.schema.load_schema()
    schema = get_arrow_schema(schema=ns)
    assert isinstance(schema, pa.Schema)
    assert "sub" in schema.names
    assert "dataset" in schema.names
    assert "extra_entities" in schema.names


def test_get_column_names_accepts_schema_kwarg():
    """Pass a ``Namespace`` to ``get_column_names`` and verify entity columns appear."""
    from bids2table._indexing import get_column_names

    ns = bidsschematools.schema.load_schema()
    cols = get_column_names(schema=ns)
    assert any(c.value == "sub" for c in cols)
