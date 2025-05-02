from pathlib import Path

try:
    # Overshadow pathlib Path.
    from cloudpathlib import AnyPath as Path

    _CLOUDPATHLIB_AVAILABLE = True
except ImportError:
    _CLOUDPATHLIB_AVAILABLE = False

__all__ = ["Path", "cloudpathlib_is_available"]


def cloudpathlib_is_available() -> bool:
    """Check if cloudpathlib is available."""
    return _CLOUDPATHLIB_AVAILABLE
