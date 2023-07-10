import logging
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from elbow.builders import build_parquet, build_table
from elbow.sources.filesystem import Crawler
from elbow.typing import StrOrPath
from elbow.utils import setup_logging

from bids2table import exceptions
from bids2table.extractors.bids import extract_bids_subdir
from bids2table.helpers import flat_to_multi_columns

setup_logging()


def bids2table(
    root: StrOrPath,
    *,
    persistent: bool = False,
    output: Optional[StrOrPath] = None,
    incremental: bool = False,
    overwrite: bool = False,
    workers: Optional[int] = None,
    worker_id: Optional[int] = None,
    max_failures: Optional[int] = 0,
    return_df: bool = True,
    filters: Optional[Dict[str, Any]] = None,
) -> Optional[pd.DataFrame]:
    """
    Index a BIDS dataset directory and load as a pandas DataFrame.

    Args:
        root: path to BIDS dataset
        persistent: whether to save index to disk as a Parquet dataset
        output: path to output Parquet dataset directory if `persistent` is
            `True`. Defaults to `root / "index.b2t".
        incremental: update index incrementally with only new or changed files.
        overwrite: overwrite previous index.
        workers: number of parallel processes. If `None` or 1, run in the main
            process. Setting to <= 0 runs as many processes as there are cores
            available.
        worker_id: optional worker ID to use when scheduling parallel tasks externally.
            Specifying the number of workers is required in this case. Incompatible with
            overwrite.
        max_failures: number of extract failures to tolerate.
        return_df: whether to return the dataframe or just build the persistent index.
        filters: optional dictionary of filters to apply to the index. Keys are
            column names and values are values or lists of values to keep.

    Returns:
        A DataFrame containing the BIDS Index.
    """
    if worker_id is not None and not persistent:
        raise ValueError(
            "worker_id is only supported when generating a persistent index"
        )
    if not (return_df or persistent):
        raise ValueError("persistent and return_df should not both be False")

    root = Path(root)
    source = Crawler(
        root=root,
        include=["sub-*"],  # find subject dirs
        skip=["sub-*"],  # but don't crawl into subject dirs
        dirs_only=True,
        follow_links=True,
    )

    if output is None:
        output = root / "index.b2t"

    stale = overwrite or incremental or worker_id is not None
    if output.exists() and not stale:
        if return_df:
            logging.info("Loading cached index %s", output)
            df = load_index(output)
        else:
            logging.info("Found cached index %s; nothing to do", output)
            df = None
        return _filter(df, filters)

    if not persistent:
        logging.info("Building index in memory")
        df = build_table(
            source=source,
            extract=extract_bids_subdir,
            max_failures=max_failures,
        )
        df = flat_to_multi_columns(df)
        return _filter(df, filters)

    logging.info("Building persistent Parquet index")
    build_parquet(
        source=source,
        extract=extract_bids_subdir,
        output=output,
        incremental=incremental,
        overwrite=overwrite,
        workers=workers,
        worker_id=worker_id,
        max_failures=max_failures,
    )
    df = load_index(output) if return_df else None
    return _filter(df, filters)


def load_index(
    path: StrOrPath, split_columns: bool = True, sep: str = "__"
) -> pd.DataFrame:
    """
    Load a bids2table index, optionally splitting columns into a multi index on `sep`.
    """
    df = pd.read_parquet(path)
    if split_columns:
        df = flat_to_multi_columns(df, sep=sep)
    return df


def _filter(df: pd.DataFrame, filters: Optional[Dict[str, Any]]) -> pd.DataFrame:
    """
    Filter a pandas DataFrame based on a dictionary of filters.

    Args:
        df: The bids2table DataFrame to filter.
        filters: A dictionary of filters to apply to the DataFrame. Format must be
        either a single value or a list of values. If None, does not filter.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    if filters is None:
        return df

    for key, value in filters.items():
        if not isinstance(value, list):
            value = [value]
        try:
            df = df[df["entities"][key].isin(value)]
        except KeyError as exc_info:
            raise exceptions.InvalidFilterError(
                f"Invalid filter: {key} is not a valid column."
            ) from exc_info

    return df
