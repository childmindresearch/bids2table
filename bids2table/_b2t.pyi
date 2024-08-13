from typing import List, Literal, Optional, overload

from elbow.typing import StrOrPath

from bids2table.table import BIDSTable

@overload
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
    return_table: Literal[True] = True,
) -> BIDSTable: ...
@overload
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
    return_table: Literal[False],
) -> None: ...
