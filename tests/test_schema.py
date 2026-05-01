"""Tests for the SchemaAdapter value object."""

import pyarrow as pa

from bids2table._schema import SchemaAdapter


def test_from_path_default_returns_arrow_schema_with_bids_metadata():
    schema = SchemaAdapter.load()
    assert isinstance(schema.arrow_schema, pa.Schema)
    assert b"bids_version" in schema.arrow_schema.metadata
    assert b"schema_version" in schema.arrow_schema.metadata


def test_from_path_default_has_subject_field():
    schema = SchemaAdapter.load()
    assert "sub" in {f.name for f in schema.arrow_schema}


def test_pyarrow_schema_roundtrip():
    original = SchemaAdapter.load()
    rebuilt = SchemaAdapter.load(original.arrow_schema)
    assert rebuilt.arrow_schema.equals(original.arrow_schema)
    assert rebuilt._entity_schema == original._entity_schema
    assert rebuilt._name_entity_map == original._name_entity_map


def test_prepare_passthrough_for_existing_instance():
    s = SchemaAdapter.load()
    assert SchemaAdapter.load(s) is s


def test_prepare_from_arrow_schema():
    s = SchemaAdapter.load()
    rebuilt = SchemaAdapter.load(s.arrow_schema)
    assert rebuilt.arrow_schema.equals(s.arrow_schema)


def test_prepare_from_namespace():
    import bidsschematools.schema

    ns = bidsschematools.schema.load_schema(None)
    rebuilt = SchemaAdapter.load(ns)
    assert rebuilt.arrow_schema.equals(SchemaAdapter.load().arrow_schema)
