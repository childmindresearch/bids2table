"""Tests for the BIDSSchema value object."""

import pyarrow as pa

from bids2table._schema import BIDSSchema


def test_from_path_default_returns_arrow_schema_with_bids_metadata():
    schema = BIDSSchema.from_path(None)
    assert isinstance(schema.arrow_schema, pa.Schema)
    md = {k.decode(): v.decode() for k, v in schema.arrow_schema.metadata.items()}
    assert "bids_version" in md
    assert "schema_version" in md


def test_from_path_default_has_subject_field():
    schema = BIDSSchema.from_path(None)
    assert "sub" in {f.name for f in schema.arrow_schema}


def test_from_namespace_round_trip_matches_from_path():
    import bidsschematools.schema

    ns = bidsschematools.schema.load_schema(None)
    a = BIDSSchema.from_namespace(ns)
    b = BIDSSchema.from_path(None)
    assert a.arrow_schema.equals(b.arrow_schema)


def test_bids_schema_property_lazy_for_from_path():
    schema = BIDSSchema.from_path(None)
    assert schema.bids_schema is not None
    assert "bids_version" in schema.bids_schema


def test_bids_schema_property_none_when_no_source():
    import bidsschematools.schema

    ns = bidsschematools.schema.load_schema(None)
    schema = BIDSSchema.from_namespace(ns)
    # from_namespace stores no source path -> bids_schema is None on lazy reload.
    assert schema.bids_schema is None
