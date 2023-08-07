import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd
import pytest
from pytest import FixtureRequest

from bids2table.entities import BIDSEntities, join_bids_path, parse_bids_entities

EXAMPLES = (
    (
        "dataset/sub-A01/ses-B02/func/sub-A01_ses-B02_task-rest_run-1_bold.nii",
        {
            "sub": "A01",
            "ses": "B02",
            "task": "rest",
            "run": "1",
            "datatype": "func",
            "suffix": "bold",
            "ext": ".nii",
        },
        BIDSEntities(
            sub="A01",
            ses="B02",
            task="rest",
            run=1,
            datatype="func",
            suffix="bold",
            ext=".nii",
        ),
    ),
    (
        "sub-A01_extraKey_bold.nii",
        {"sub": "A01", "suffix": "bold", "ext": ".nii", "extraKey": ""},
        BIDSEntities(
            sub="A01", suffix="bold", ext=".nii", extra_entities={"extraKey": ""}
        ),
    ),
    (
        "sub-A01_noExt",
        {"sub": "A01", "suffix": "noExt"},
        BIDSEntities(sub="A01", suffix="noExt"),
    ),
    (
        "sub-A01_no-suffix",
        {"sub": "A01", "no": "suffix"},
        BIDSEntities(sub="A01", extra_entities={"no": "suffix"}),
    ),
)


@pytest.fixture(params=EXAMPLES)
def bids_example(request: FixtureRequest) -> Tuple[str, Dict[str, str], BIDSEntities]:
    return request.param


def test_parse_bids_entities(bids_example: Tuple[str, Dict[str, str], BIDSEntities]):
    path, expected, _ = bids_example
    entities = parse_bids_entities(path)
    assert entities == expected


def test_bids_entities_from_path(
    bids_example: Tuple[str, Dict[str, str], BIDSEntities]
):
    path, _, expected = bids_example
    entities = BIDSEntities.from_path(path)
    for k, v in asdict(expected).items():
        v2 = getattr(entities, k)
        if v != v2:
            logging.warning("Entity mismatch: %s, %s, %s", k, v, v2)
    assert entities == expected


def test_bids_entities_from_dict(
    bids_example: Tuple[str, Dict[str, str], BIDSEntities]
):
    _, _, expected = bids_example
    entities = BIDSEntities.from_dict(asdict(expected))
    for k, v in asdict(expected).items():
        v2 = getattr(entities, k)
        if v != v2:
            logging.warning("Entity mismatch: %s, %s, %s", k, v, v2)
    assert entities == expected


def test_bids_entities_to_path(bids_example: Tuple[str, Dict[str, str], BIDSEntities]):
    path, _, _ = bids_example
    entities = BIDSEntities.from_path(path)
    path2 = entities.to_path()
    assert Path(path).name == path2.name


def test_bids_entities_with_update(
    bids_example: Tuple[str, Dict[str, str], BIDSEntities]
):
    path, _, _ = bids_example
    entities = BIDSEntities.from_path(path)
    entities = entities.with_update(sub="A02")
    assert entities.sub == "A02"


@pytest.mark.parametrize(
    "entities,prefix,valid_only,expected",
    [
        (
            {"sub": "A01", "ses": "b", "run": 2, "suffix": "bold", "ext": ".json"},
            None,
            False,
            "sub-A01/ses-b/sub-A01_ses-b_run-2_bold.json",
        ),
        (
            {"sub": "A01", "ses": "b", "run": 2, "suffix": "bold", "ext": ".json"},
            "dataset",
            False,
            "dataset/sub-A01/ses-b/sub-A01_ses-b_run-2_bold.json",
        ),
        (
            {
                "sub": "A01",
                "ses": "b",
                "run": 2,
                "extraKey": 1,
                "suffix": "bold",
                "ext": ".json",
            },
            None,
            False,
            "sub-A01/ses-b/sub-A01_ses-b_run-2_extraKey-1_bold.json",
        ),
        (
            {
                "sub": "A01",
                "ses": "b",
                "run": 2,
                "extraKey": 1,
                "suffix": "bold",
                "ext": ".json",
            },
            None,
            True,
            "sub-A01/ses-b/sub-A01_ses-b_run-2_bold.json",
        ),
        (
            pd.Series(
                {
                    "sub": "A01",
                    "ses": "b",
                    "run": 2,
                    "suffix": "bold",
                    "ext": ".json",
                }
            ),
            None,
            False,
            "sub-A01/ses-b/sub-A01_ses-b_run-2_bold.json",
        ),
    ],
)
def test_join_bids_path(
    entities: Union[Dict[str, Any], pd.Series],
    prefix: Optional[str],
    valid_only: bool,
    expected: str,
):
    path = join_bids_path(entities, prefix=prefix, valid_only=valid_only)
    assert str(path) == expected


if __name__ == "__main__":
    pytest.main([__file__])
