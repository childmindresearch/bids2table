"""
Efficiently index and query large-scale BIDS datasets and derivatives.
"""

from . import entities, extractors, table
from ._bids2table import bids2table  # noqa
from ._version import __version__, __version_tuple__  # noqa

__all__ = ["bids2table", "table", "entities", "extractors"]
