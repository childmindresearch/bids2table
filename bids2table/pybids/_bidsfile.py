"""
BIDSFile wrapper for file paths with entity access.

Provides a PyBIDS-compatible file object that can parse and cache
BIDS entities from file paths.
"""

from typing import Any

from .._entities import parse_bids_entities


class BIDSFile:
    """
    Wrapper around a BIDS file path with entity parsing.

    Provides PyBIDS-compatible interface for accessing file entities.
    Entities are lazily parsed and cached on first access.

    Example:
        >>> from bids2table_compat import BIDSFile
        >>> f = BIDSFile('sub-01/func/sub-01_task-rest_bold.nii.gz')
        >>> entities = f.get_entities()
        >>> print(entities)
        {'sub': '01', 'task': 'rest', 'suffix': 'bold', 'ext': '.nii.gz'}
    """

    def __init__(self, path: str):
        """
        Initialize BIDSFile.

        Args:
            path: Path to BIDS file (absolute or relative)
        """
        self.path = str(path)
        self._entities: dict[str, Any] | None = None

    def get_entities(self) -> dict[str, Any]:
        """
        Parse and return BIDS entities from filename.

        Entities are cached after first parse for performance.

        Returns:
            Dictionary of BIDS entities (e.g., {'sub': '01', 'task': 'rest'})
        """
        if self._entities is None:
            self._entities = parse_bids_entities(self.path)
        return self._entities

    def __str__(self) -> str:
        """String representation showing file path."""
        return self.path

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"BIDSFile('{self.path}')"

    def __eq__(self, other) -> bool:
        """Equality based on path."""
        if isinstance(other, BIDSFile):
            return self.path == other.path
        return False

    def __hash__(self) -> int:
        """Allow use in sets/dicts."""
        return hash(self.path)

    def __lt__(self, other) -> bool:
        """Less-than comparison based on path (for sorting)."""
        if isinstance(other, BIDSFile):
            return self.path < other.path
        return NotImplemented

    def __le__(self, other) -> bool:
        """Less-than-or-equal comparison based on path."""
        if isinstance(other, BIDSFile):
            return self.path <= other.path
        return NotImplemented

    def __gt__(self, other) -> bool:
        """Greater-than comparison based on path."""
        if isinstance(other, BIDSFile):
            return self.path > other.path
        return NotImplemented

    def __ge__(self, other) -> bool:
        """Greater-than-or-equal comparison based on path."""
        if isinstance(other, BIDSFile):
            return self.path >= other.path
        return NotImplemented

    def __contains__(self, item) -> bool:
        """Check if substring is in the file path (for 'in' operator)."""
        return item in self.path
