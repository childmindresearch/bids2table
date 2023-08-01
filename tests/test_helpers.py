from typing import Any, Dict, Optional

import pandas as pd
import pytest

from bids2table.helpers import join_bids_path


@pytest.mark.parametrize(
    "row,prefix,valid_only,expected",
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
            {
                "entities": {
                    "sub": "A01",
                    "ses": "b",
                    "run": 2,
                    "suffix": "bold",
                    "ext": ".json",
                }
            },
            None,
            False,
            "sub-A01/ses-b/sub-A01_ses-b_run-2_bold.json",
        ),
        (
            pd.concat(
                [
                    pd.Series(
                        {
                            "sub": "A01",
                            "ses": "b",
                            "run": 2,
                            "suffix": "bold",
                            "ext": ".json",
                        }
                    )
                ],
                keys=["entities"],
            ),
            None,
            False,
            "sub-A01/ses-b/sub-A01_ses-b_run-2_bold.json",
        ),
    ],
)
def test_join_bids_path(
    row: Dict[str, Any], prefix: Optional[str], valid_only: bool, expected: str
):
    path = join_bids_path(row, prefix=prefix, valid_only=valid_only)
    assert str(path) == expected


if __name__ == "__main__":
    pytest.main([__file__])
