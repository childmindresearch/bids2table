from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd

from .extractors.entities import BIDSEntities


def join_bids_path(
    row: Union[pd.Series, Dict[str, Any]],
    prefix: Optional[Union[str, Path]] = None,
    valid_only: bool = True,
) -> Path:
    """
    Reconstruct a BIDS path from a table row/record or entities dict.

    Example::

        df = pd.read_parquet("dataset.parquet")
        paths = df.apply(join_bids_path, axis=1)
    """
    if isinstance(row, pd.Series):
        row = row.to_dict()

    entities = BIDSEntities.from_dict(row)
    path = entities.to_path(prefix=prefix, valid_only=valid_only)
    return path
