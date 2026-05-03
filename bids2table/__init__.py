""".. include:: ../README.md"""  # noqa: D415

__all__ = [
    "index_dataset",
    "batch_index_dataset",
    "find_bids_datasets",
    "get_arrow_schema",
    "get_column_names",
    "parse_bids_entities",
    "validate_bids_entities",
    "format_bids_path",
    "load_bids_metadata",
    "cloudpathlib_is_available",
    "SchemaSpec",
]

import importlib.util

if importlib.util.find_spec("pandas"):
    __all__.append("pybids")

from ._entities import (
    format_bids_path,
    parse_bids_entities,
    validate_bids_entities,
)
from ._indexing import (
    batch_index_dataset,
    find_bids_datasets,
    get_arrow_schema,
    get_column_names,
    index_dataset,
)
from ._metadata import load_bids_metadata
from ._pathlib import cloudpathlib_is_available
from ._schema import SchemaSpec
from ._version import *
