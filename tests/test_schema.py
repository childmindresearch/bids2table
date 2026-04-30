"""Tests for the BIDSSchema value object."""

import pyarrow as pa

from bids2table._schema import BIDSSchema


def test_from_path_default_returns_arrow_schema_with_bids_metadata():
    schema = BIDSSchema.prepare(None)
    assert isinstance(schema.arrow_schema, pa.Schema)
    assert b"bids_version" in schema.arrow_schema.metadata
    assert b"schema_version" in schema.arrow_schema.metadata


def test_from_path_default_has_subject_field():
    schema = BIDSSchema.prepare(None)
    assert "sub" in {f.name for f in schema.arrow_schema}


def test_bids_schema_property_lazy_for_from_path():
    schema = BIDSSchema.prepare(None)
    assert schema.bids_schema is not None
    assert "bids_version" in schema.bids_schema


def test_pyarrow_schema_roundtrip():
    original = BIDSSchema.prepare(None)
    rebuilt = BIDSSchema.prepare(original.arrow_schema)
    assert rebuilt.arrow_schema.equals(original.arrow_schema)
    assert rebuilt._entity_schema == original._entity_schema
    assert rebuilt._name_entity_map == original._name_entity_map


def test_from_arrow_bids_schema_returns_none():
    original = BIDSSchema.prepare(None)
    rebuilt = BIDSSchema.prepare(original.arrow_schema)
    assert rebuilt.bids_schema is None


def test_prepare_passthrough_for_existing_instance():
    s = BIDSSchema.prepare(None)
    assert BIDSSchema.prepare(s) is s


def test_prepare_from_arrow_schema():
    s = BIDSSchema.prepare(None)
    rebuilt = BIDSSchema.prepare(s.arrow_schema)
    assert rebuilt.arrow_schema.equals(s.arrow_schema)


def test_prepare_from_namespace():
    import bidsschematools.schema

    ns = bidsschematools.schema.load_schema(None)
    rebuilt = BIDSSchema.prepare(ns)
    assert rebuilt.arrow_schema.equals(BIDSSchema.prepare(None).arrow_schema)
