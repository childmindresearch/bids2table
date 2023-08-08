"""
Efficiently index and query large-scale BIDS datasets and derivatives.
"""

from ._b2t import bids2table
from ._version import __version__, __version_tuple__  # noqa
from .entities import BIDSEntities, parse_bids_entities
from .table import BIDSFile, BIDSTable, join_bids_path

__all__ = [
    "bids2table",
    "BIDSTable",
    "BIDSFile",
    "BIDSEntities",
    "parse_bids_entities",
    "join_bids_path",
]
