"""Tests for the BIDSSchema value object."""

import pickle
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

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


def test_from_arrow_round_trip_preserves_lookups():
    original = BIDSSchema.from_path(None)
    rebuilt = BIDSSchema.from_arrow(original.arrow_schema)
    assert rebuilt.arrow_schema.equals(original.arrow_schema)
    assert rebuilt._entity_schema.keys() == original._entity_schema.keys()
    assert rebuilt._name_entity_map == original._name_entity_map


def test_from_arrow_bids_schema_returns_none():
    original = BIDSSchema.from_path(None)
    rebuilt = BIDSSchema.from_arrow(original.arrow_schema)
    assert rebuilt.bids_schema is None


def test_prepare_passthrough_for_existing_instance():
    s = BIDSSchema.from_path(None)
    assert BIDSSchema.prepare(s) is s


def test_prepare_from_arrow_schema():
    s = BIDSSchema.from_path(None)
    rebuilt = BIDSSchema.prepare(s.arrow_schema)
    assert rebuilt.arrow_schema.equals(s.arrow_schema)


def test_prepare_from_namespace():
    import bidsschematools.schema

    ns = bidsschematools.schema.load_schema(None)
    rebuilt = BIDSSchema.prepare(ns)
    assert rebuilt.arrow_schema.equals(BIDSSchema.from_path(None).arrow_schema)


def test_prepare_dispatches_str_and_path_to_from_path(monkeypatch):
    """Verify str and Path inputs route to from_path with the original argument."""
    captured: list[Any] = []
    real_instance = BIDSSchema.from_path(None)

    def fake_from_path(cls, path):
        captured.append(path)
        return real_instance

    monkeypatch.setattr(BIDSSchema, "from_path", classmethod(fake_from_path))
    BIDSSchema.prepare("some/string")
    BIDSSchema.prepare(Path("some/path"))
    assert captured == ["some/string", Path("some/path")]


def test_prepare_rejects_unknown_type():
    with pytest.raises(TypeError, match=r"Expected BIDSSchema"):
        BIDSSchema.prepare(42)


def test_pickle_round_trip_default():
    s = BIDSSchema.from_path(None)
    restored = pickle.loads(pickle.dumps(s))
    assert restored.arrow_schema.equals(s.arrow_schema)
    assert restored._name_entity_map == s._name_entity_map


def test_pickle_round_trip_after_lazy_load():
    s = BIDSSchema.from_path(None)
    _ = s.bids_schema  # materialize cached_property
    restored = pickle.loads(pickle.dumps(s))
    assert restored.arrow_schema.equals(s.arrow_schema)
