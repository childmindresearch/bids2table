import logging
from typing import Any, NamedTuple

import pytest
from pytest import FixtureRequest, LogCaptureFixture

from bids2table._entities import (
    format_bids_path,
    parse_bids_entities,
    validate_bids_entities,
)


class ExampleCase(NamedTuple):
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
def bids_example(request: FixtureRequest) -> ExampleCase:
    return request.param


def test_parse_validate_bids_entities(bids_example: ExampleCase):
    path, expected_valid_entities, expected_extra_entities = bids_example
    entities = parse_bids_entities(path)
    valid_entities, extra_entities = validate_bids_entities(entities)
    assert valid_entities == expected_valid_entities
    assert extra_entities == expected_extra_entities


@pytest.mark.parametrize(
    "path,msg",
    [
        ("sub-A01_run-abc_bold.nii.gz", "type"),  # Invalid run type
        ("sub-A01_part-phasee_bold.nii.gz", "allowed"),  # Not in allowed values, typo
    ],
)
def test_validate_warns(path: str, msg: str, caplog: LogCaptureFixture):
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
    entities = parse_bids_entities(path)
    valid_entities, _ = validate_bids_entities(entities)
    path2 = format_bids_path(valid_entities)
    assert path == str(path2)
