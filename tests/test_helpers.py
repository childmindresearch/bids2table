import pandas as pd
import pytest

from bids2table.helpers import flat_to_multi_columns, multi_to_flat_columns


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
