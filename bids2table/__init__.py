""".. include:: ../README.md"""  # noqa: D415

__all__ = [
    "index_dataset",
    "batch_index_dataset",
    "find_bids_datasets",
    "get_arrow_schema",
    "get_column_names",
    "parse_bids_entities",
    "validate_bids_entities",
    "set_bids_schema",
    "clear_schema_caches",
    "get_bids_schema",
    "get_bids_entity_arrow_schema",
    "format_bids_path",
    "load_bids_metadata",
    "cloudpathlib_is_available",
    "pybids",
]

from . import pybids
from ._entities import (
    format_bids_path,
    get_bids_entity_arrow_schema,
    get_bids_schema,
    parse_bids_entities,
    set_bids_schema,
    validate_bids_entities,
)
from ._indexing import (
    batch_index_dataset,
    clear_schema_caches,
    find_bids_datasets,
    get_arrow_schema,
    get_column_names,
    index_dataset,
)
from ._metadata import load_bids_metadata
from ._pathlib import cloudpathlib_is_available
from ._version import *
