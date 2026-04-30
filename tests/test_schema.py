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
