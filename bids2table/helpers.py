import pandas as pd


def flat_to_multi_columns(df: pd.DataFrame, sep: str = "__") -> pd.DataFrame:
    """
    Convert a flat column index to a MultiIndex by splitting on `sep`.
    """
    # Do nothing if already a MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        return df

    # Do nothing for empty df
    # TODO: It would probably be better if the header was initialized even if there are
    # no records.
    if len(df.columns) == 0:
        return df

    split_columns = [col.split(sep) for col in df.columns]
    num_levels = max(map(len, split_columns))

    def _pad_col(col):
        return tuple((num_levels - len(col)) * [None] + col)

    df = df.copy(deep=False)
    df.columns = pd.MultiIndex.from_tuples(map(_pad_col, split_columns))
    return df


def multi_to_flat_columns(df: pd.DataFrame, sep: str = "__") -> pd.DataFrame:
    """
    Convert a column MultiIndex to a flat index by joining on `sep`.
    """
    # Do nothing if already flat
    if not isinstance(df.columns, pd.MultiIndex):
        return df

    columns = df.columns.to_flat_index()
    join_columns = [sep.join(col) for col in columns]

    df = df.copy(deep=False)
    df.columns = pd.Index(join_columns)
    return df
