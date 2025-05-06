# ruff: noqa: I001
"""Index BIDS datasets fast, locally or in the cloud."""

from ._entities import (
    parse_bids_entities,
    validate_bids_entities,
    set_bids_schema,
    get_bids_schema,
    get_bids_entity_arrow_schema,
    format_bids_path,
)
from ._indexing import (
    find_bids_datasets,
    index_dataset,
    batch_index_dataset,
    get_arrow_schema,
    get_column_names,
)
from ._pathlib import Path, cloudpathlib_is_available
from ._version import *
