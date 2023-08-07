import logging
from pathlib import Path
from typing import Optional

from elbow.builders import build_parquet, build_table
from elbow.sources.filesystem import Crawler
from elbow.typing import StrOrPath
from elbow.utils import setup_logging

from bids2table.extractors.bids import extract_bids_subdir
from bids2table.table import BIDSTable

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
    return_table: bool = True,
) -> Optional[BIDSTable]:
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
        return_table: whether to return the BIDS table or just build the persistent
            index.

    Returns:
        A DataFrame containing the BIDS Index.
    """
    if worker_id is not None and not persistent:
        raise ValueError(
            "worker_id is only supported when generating a persistent index"
        )
    if not (return_table or persistent):
        raise ValueError("persistent and return_table should not both be False")

    root = Path(root).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"root directory {root} does not exists")

    source = Crawler(
        root=root,
        include=["sub-*"],  # find subject dirs
        skip=["sub-*"],  # but don't crawl into subject dirs
        dirs_only=True,
        follow_links=True,
    )

    if output is None:
        output = root / "index.b2t"
    else:
        output = Path(output).expanduser().resolve()

    stale = overwrite or incremental or worker_id is not None
    if output.exists() and not stale:
        if return_table:
            logging.info("Loading cached index %s", output)
            tab = BIDSTable.from_parquet(output)
        else:
            logging.info("Found cached index %s; nothing to do", output)
            tab = None
        return tab

    if not persistent:
        logging.info("Building index in memory")
        df = build_table(source=source, extract=extract_bids_subdir)
        tab = BIDSTable(df)
        return tab

    logging.info("Building persistent Parquet index")
    build_parquet(
        source=source,
        extract=extract_bids_subdir,
        output=output,
        incremental=incremental,
        overwrite=overwrite,
        workers=workers,
        worker_id=worker_id,
        path_column="file__file_path",
        mtime_column="file__mod_time",
    )
    tab = BIDSTable.from_parquet(output) if return_table else None
    return tab
