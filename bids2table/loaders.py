from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq
from elbow import load_parquet, load_table
from elbow.typing import StrOrPath

from .extractors.bids import bids_extract

__all__ = ["load_bids_table", "load_bids_parquet"]


def load_bids_table(path: StrOrPath) -> pd.DataFrame:
    """
    Index a BIDS dataset directory and load as a pandas DataFrame

    Args:
        path: path to BIDS dataset

    Returns:
        A DataFrame containing the BIDS Index
    """
    pattern = str(Path(path) / "**")
    df = load_table(
        source=pattern,
        extract=bids_extract,
        max_failures=0,
    )
    return df


def load_bids_parquet(
    path: StrOrPath,
    where: StrOrPath,
    incremental: bool = False,
    workers: Optional[int] = None,
) -> pq.ParquetDataset:
    """
    Index a BIDS dataset directory and load as a Parquet dataset

    Args:
        path: path to BIDS dataset
        where: path to output parquet dataset directory
        incremental: update dataset incrementally with only new or changed files.
        workers: number of parallel processes. If `None` or 1, run in the main
            process. Setting to -1 runs in `os.cpu_count()` processes.

    Returns:
        A PyArrow ParquetDataset handle to the loaded BIDS index
    """
    pattern = str(Path(path) / "**")
    dset = load_parquet(
        source=pattern,
        extract=bids_extract,
        where=where,
        incremental=incremental,
        workers=workers,
        max_failures=0,
    )
    return dset
