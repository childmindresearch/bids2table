""".. include:: ../README.md"""  # noqa: D415

__all__ = [
    "SchemaSpec",
    "batch_index_dataset",
    "cloudpathlib_is_available",
    "find_bids_datasets",
    "format_bids_path",
    "get_arrow_schema",
    "get_column_names",
    "index_dataset",
    "load_bids_metadata",
    "parse_bids_entities",
    "validate_bids_entities",
]

import importlib.util

if importlib.util.find_spec("pandas"):
    __all__.append("pybids")

from bids2table._entities import (
    format_bids_path,
    parse_bids_entities,
    validate_bids_entities,
)
from bids2table._indexing import (
    batch_index_dataset,
    find_bids_datasets,
    get_arrow_schema,
    get_column_names,
    index_dataset,
)
from bids2table._metadata import load_bids_metadata
from bids2table._pathlib import cloudpathlib_is_available
from bids2table._schema import SchemaSpec
from bids2table._version import *
