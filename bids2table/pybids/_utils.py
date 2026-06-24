"""Utility functions for PyBIDS compatibility."""

from typing import Any


class Query:
    """Special query values for entity filtering."""

    # Sentinel objects for special filtering behavior
    OPTIONAL = object()  # Allow missing or any value
    NONE = object()  # Match explicit null/missing
    ANY = object()  # Match any value (don't filter)

    def __repr__(self) -> str:
        return "Query"


def listify(obj: Any) -> list:  # noqa: ANN401 - passed object can be anything
    """Convert an object to a list if it isn't already one.

    This is a PyBIDS utility function that niworkflows and other packages use.

    Parameters
    ----------
    obj : Any
        Object to convert to list

    Returns:
    -------
    list
        If obj is None, returns empty list
        If obj is already a list/tuple, returns as list
        Otherwise returns [obj]

    Examples:
    --------
    >>> listify(None)
    []
    >>> listify("test")
    ['test']
    >>> listify(["a", "b"])
    ['a', 'b']
    >>> listify(("a", "b"))
    ['a', 'b']
    """
    if obj is None:
        return []
    if isinstance(obj, list | tuple):
        return list(obj)
    return [obj]
