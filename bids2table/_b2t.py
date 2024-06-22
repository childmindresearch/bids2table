import logging
from functools import partial
from pathlib import Path
from typing import List, Optional

from elbow.builders import build_parquet, build_table
from elbow.sources.filesystem import Crawler
from elbow.typing import StrOrPath

from bids2table.extractors.bids import extract_bids_subdir
from bids2table.table import BIDSTable

logger = logging.getLogger("bids2table")


def bids2table(
    root: StrOrPath,
    *,
    with_meta: bool = True,
    persistent: bool = False,
    index_path: Optional[StrOrPath] = None,
    exclude: Optional[List[str]] = None,
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
        with_meta: extract JSON sidecar metadata. Excluding metadata can result in much
            faster indexing.
        persistent: whether to save index to disk as a Parquet dataset
        index_path: path to BIDS Parquet index to generate or load. Defaults to `root /
            "index.b2t"`. Index generation requires `persistent=True`.
        exclude: Optional list of directory names to exclude from indexing.
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
        A `BIDSTable` representing the indexed dataset(s), or `None` if `return_table`
        is `False`.
    """
    if not (return_table or persistent):
        raise ValueError("persistent and return_table should not both be False")

    root = Path(root).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"root directory {root} does not exists")
    
    if exclude is None:
        exclude = []
        
    source = Crawler(
        root=root,
        include=["sub-*"],  # find subject dirs
        skip=["sub-*"] + exclude,  # but don't crawl into subject dirs
        dirs_only=True,
        follow_links=True,
    )
    extract = partial(extract_bids_subdir, exclude=exclude, with_meta=with_meta)

    if index_path is None:
        index_path = root / "index.b2t"
    else:
        index_path = Path(index_path).resolve()

    stale = overwrite or incremental or worker_id is not None
    if index_path.exists() and not stale:
        if return_table:
            logger.info("Loading cached index %s", index_path)
            tab = BIDSTable.from_parquet(index_path)
        else:
            logger.info("Found cached index %s; nothing to do", index_path)
            tab = None
        return tab

    if not persistent:
        logger.info("Building index in memory")
        df = build_table(
            source=source,
            extract=extract,
            workers=workers,
            worker_id=worker_id,
        )
        tab = BIDSTable.from_df(df)
        return tab

    logger.info("Building persistent Parquet index")
    build_parquet(
        source=source,
        extract=extract,
        output=index_path,
        incremental=incremental,
        overwrite=overwrite,
        workers=workers,
        worker_id=worker_id,
        path_column="file__file_path",
        mtime_column="file__mod_time",
    )
    tab = BIDSTable.from_parquet(index_path) if return_table else None
    return tab
