from typing import Any, Dict, Optional

import pandas as pd
import pytest

from bids2table.helpers import (
    flat_to_multi_columns,
    join_bids_path,
    multi_to_flat_columns,
)


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


@pytest.mark.parametrize("sep", ["__", "."])
def test_flat_to_multi_columns(sep: str):
    df = pd.DataFrame(
        {
            f"A{sep}a": [1, 2, 3],
            f"A{sep}b": ["a", "b", "c"],
            f"B{sep}a": [4, 5, 6],
            f"B{sep}b": ["d", "e", "f"],
        }
    )
    multi_index = pd.MultiIndex.from_product([["A", "B"], ["a", "b"]])

    df_multi = flat_to_multi_columns(df, sep=sep)
    assert df_multi.columns.equals(multi_index)

    df_flat = multi_to_flat_columns(df_multi, sep=sep)
    assert df_flat.equals(df)


if __name__ == "__main__":
    pytest.main([__file__])
