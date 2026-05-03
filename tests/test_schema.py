import pytest

from bids2table._schema import decode_metadata, encode_metadata


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
