"""Tests for BIDS entity parsing, validation, formatting, and entity helpers."""

import logging
from typing import Any, NamedTuple

import pytest
from bids2table._entities import (
    format_bids_path,
    get_entity_glob_pattern,
    get_entity_name,
    get_entity_regex,
    get_file_entity_prefixes,
    get_root_entity_types,
    parse_bids_entities,
    validate_bids_entities,
)
from bids2table._schema import BIDSSchemaAdapter


class ExampleCase(NamedTuple):
    """A single BIDS entity test case with expected valid/extra splits."""

    path: str
    valid_entities: dict[str, Any]
    extra_entities: dict[str, Any]


EXAMPLES = [
    ExampleCase(
        "dataset/sub-A01/ses-B02/func/sub-A01_ses-B02_task-rest_run-1_bold.nii.gz",
        {
            "sub": "A01",
            "ses": "B02",
            "task": "rest",
            "run": 1,
            "datatype": "func",
            "suffix": "bold",
            "ext": ".nii.gz",
        },
        {},
    ),
    ExampleCase(
        "sub-A01/func/sub-A01_task-rest_run-1_bold.nii.gz",
        {
            "sub": "A01",
            "task": "rest",
            "run": 1,
            "datatype": "func",
            "suffix": "bold",
            "ext": ".nii.gz",
        },
        {},
    ),
    ExampleCase(
        "sub-A01_extraKey-true_extraKeyNoValue_bold.nii",
        {"sub": "A01", "suffix": "bold", "ext": ".nii"},
        {"extraKey": "true"},
    ),
    ExampleCase(
        "sub-A01_noExt",
        {"sub": "A01", "suffix": "noExt"},
        {},
    ),
    ExampleCase(
        "sub-A01_no-suffix",
        {"sub": "A01"},
        {"no": "suffix"},
    ),
]


@pytest.fixture(params=EXAMPLES)
def bids_example(request: pytest.FixtureRequest) -> ExampleCase:
    """Yield each ``ExampleCase`` as a separate test parameter."""
    return request.param


def test_parse_validate_bids_entities(bids_example: ExampleCase):
    """Verify parsing and schema-aware validation of BIDS entity paths."""
    path, expected_valid_entities, expected_extra_entities = bids_example
    entities = parse_bids_entities(path)
    valid_entities, extra_entities = validate_bids_entities(entities)
    assert valid_entities == expected_valid_entities
    assert extra_entities == expected_extra_entities


@pytest.mark.parametrize(
    ("path", "msg"),
    [
        ("sub-A01_run-abc_bold.nii.gz", "type"),  # Invalid run type
        ("sub-A01_part-phasee_bold.nii.gz", "allowed"),  # Not in allowed values, typo
    ],
)
def test_validate_warns(path: str, msg: str, caplog: pytest.LogCaptureFixture):
    """Ensure validation emits warnings for invalid entity values."""
    entities = parse_bids_entities(path)
    with caplog.at_level(logging.WARNING):
        validate_bids_entities(entities)
    assert msg in caplog.text


@pytest.mark.parametrize(
    "path",
    [
        "sub-A01/func/sub-A01_run-1_bold.nii.gz",
        "sub-A01/ses-1/func/sub-A01_ses-1_run-1_bold.nii.gz",
    ],
)
def test_format_bids_path(path: str):
    """Round-trip a BIDS path through parse → validate → format."""
    entities = parse_bids_entities(path)
    valid_entities, _ = validate_bids_entities(entities)
    path2 = format_bids_path(valid_entities)
    assert path == str(path2)


@pytest.mark.parametrize(
    ("entity_type", "expected_name"),
    [
        ("subject", "sub"),
        ("session", "ses"),
        ("run", "run"),
        ("task", "task"),
        ("template", "tpl"),
        ("acquisition", "acq"),
        ("datatype", "datatype"),
        ("suffix", "suffix"),
        ("extension", "ext"),
    ],
)
def test_get_entity_name(
    entity_type: str, expected_name: str, adapter: BIDSSchemaAdapter
):
    """Entity type maps to the expected short name."""
    assert get_entity_name(entity_type, adapter) == expected_name


def test_get_entity_name_returns_none_and_warns_for_unknown(
    adapter: BIDSSchemaAdapter,
):
    """Unknown entity_type returns None and logs a warning."""
    result = get_entity_name("nonexistent_entity", adapter)
    assert result is None


def test_get_entity_regex_returns_none_for_unknown(adapter: BIDSSchemaAdapter):
    """Unknown entity_type returns None instead of raising."""
    result = get_entity_regex("nonexistent_entity", adapter)
    assert result is None


def test_get_entity_glob_pattern_returns_none_for_unknown(adapter: BIDSSchemaAdapter):
    """Unknown entity_type returns None instead of raising."""
    result = get_entity_glob_pattern("nonexistent_entity", adapter)
    assert result is None


@pytest.mark.parametrize(
    ("entity_type", "value"),
    [
        ("subject", "abc"),
        ("session", "01"),
        ("run", "1"),
        ("task", "rest"),
        ("extension", "nii.gz"),
    ],
)
def test_get_entity_regex_matches_prefix(
    entity_type: str, value: str, adapter: BIDSSchemaAdapter
):
    """Regex matches the expected prefix-value form."""
    pattern = get_entity_regex(entity_type, adapter)
    name = get_entity_name(entity_type, adapter)
    assert pattern is not None
    assert name is not None
    assert pattern.fullmatch(f"{name}-{value}") is not None


@pytest.mark.parametrize(
    ("entity_type", "glob"),
    [("subject", "sub-*"), ("session", "ses-*"), ("run", "run-*")],
)
def test_get_entity_glob_pattern(
    entity_type: str, glob: str, adapter: BIDSSchemaAdapter
):
    """Glob pattern is prefix followed by wildcard."""
    assert get_entity_glob_pattern(entity_type, adapter) == glob


def test_get_root_entity_types_contains_subject(adapter: BIDSSchemaAdapter):
    """Subject is always a root entity."""
    roots = get_root_entity_types(adapter)
    assert "sub" in roots


def test_get_root_entity_types_contains_template(adapter: BIDSSchemaAdapter):
    """Template is a root entity in the default (with derivatives) schema."""
    roots = get_root_entity_types(adapter)
    assert "tpl" in roots


def test_get_file_entity_prefixes_excludes_directory_and_special(
    adapter: BIDSSchemaAdapter,
):
    """File entity prefixes should not include sub, ses, tpl, or special names."""
    prefixes = get_file_entity_prefixes(adapter)
    for excluded in ("sub", "ses", "tpl", "datatype", "suffix", "ext"):
        assert excluded not in prefixes


def test_get_file_entity_prefixes_contains_filename_entities(
    adapter: BIDSSchemaAdapter,
):
    """Common filename entities should be present."""
    prefixes = get_file_entity_prefixes(adapter)
    for expected in ("run", "task", "acq", "echo", "part", "space"):
        assert expected in prefixes
