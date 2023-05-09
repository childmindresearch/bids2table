from dataclasses import fields
from pathlib import Path
from typing import Any, Dict, Union

import pandas as pd

from .extractors.entities import BIDSEntities


def join_bids_path(row: Union[pd.Series, Dict[str, Any]]) -> Path:
    """
    Reconstruct a valid BIDS path from a table row/record.

    Example::

        df = pd.read_parquet("dataset.parquet")
        paths = df.apply(join_bids_path, axis=1)
    """
    if isinstance(row, pd.Series):
        row = row.to_dict()

    special = {"datatype", "suffix", "ext"}
    keys = [f.name for f in fields(BIDSEntities) if f.name not in special]
    filename = "_".join(f"{k}-{row[k]}" for k in keys if not pd.isna(row.get(k)))

    sub = row.get("sub")
    ses = row.get("ses")
    datatype = row.get("datatype")
    suffix = row.get("suffix")
    ext = row.get("ext")

    if suffix:
        filename = filename + "_" + suffix
    if ext:
        filename = filename + ext

    path = Path(filename)
    if datatype:
        path = datatype / path
    else:
        raise KeyError("Row is missing a valid datatype")

    if ses:
        path = f"ses-{ses}" / path

    if sub:
        path = f"sub-{sub}" / path
    else:
        raise KeyError("Row is missing a valid sub")

    return path
