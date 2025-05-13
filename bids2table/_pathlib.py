import os
import sys
from pathlib import Path
from typing import Iterator

try:
    from cloudpathlib import AnyPath, CloudPath, S3Client

    _CLOUDPATHLIB_AVAILABLE = True

    # Set unsigned client as default for s3:// paths
    S3Client(no_sign_request=True).set_as_default_client()

except ImportError:
    AnyPath = CloudPath = Path

    _CLOUDPATHLIB_AVAILABLE = False

__all__ = ["PathT", "as_path", "cloudpathlib_is_available"]

PathT = Path | CloudPath


def walk(
    root: str | PathT, follow_symlinks: bool = False
) -> Iterator[tuple[str, list[str], list[str]]]:
    if isinstance(root, str):
        root = as_path(root)

    # Py312+ or CloudPath
    if sys.version_info >= (3, 12) or (
        _CLOUDPATHLIB_AVAILABLE and isinstance(root, CloudPath)
    ):
        yield from root.walk(follow_symlinks=follow_symlinks)
    # Fall back to os.walk for local paths
    else:
        for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_symlinks):
            yield Path(dirpath), dirnames, filenames


def as_path(path: str | PathT) -> PathT:
    """Cast input to a `Path` type."""
    if isinstance(path, str):
        return AnyPath(path)
    return path


def cloudpathlib_is_available() -> bool:
    """Check if cloudpathlib is available."""
    return _CLOUDPATHLIB_AVAILABLE
