import fnmatch
import os
from collections.abc import Iterator
from pathlib import Path

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


def as_path(path: str | PathT) -> PathT:
    """Cast input to a `Path` type."""
    if isinstance(path, str):
        return AnyPath(path)
    return path


def cloudpathlib_is_available() -> bool:
    """Check if cloudpathlib is available."""
    return _CLOUDPATHLIB_AVAILABLE


def rglob(path: PathT, pattern: str, follow_symlinks: bool = True) -> Iterator[PathT]:
    """Safely glob paths, recursing symlinks.

    NOTE: Only needed to support recursive globbing in <py3.13
    """
    # Not expecting symlinks for cloudpaths so just use their glob method.
    if isinstance(path, CloudPath):
        yield from path.rglob(f"{pattern}")
        return

    visited = set()
    for dirpath, dirnames, filenames in os.walk(path, followlinks=follow_symlinks):
        dirpath = as_path(dirpath)
        try:
            stat = os.stat(dirpath, follow_symlinks=False)
        except FileNotFoundError:
            continue  # Broken symlink or race condition

        inode = (stat.st_dev, stat.st_ino)
        if inode in visited:
            continue
        visited.add(inode)

        all_names = [name for name in dirnames] + [name for name in filenames]
        for name in all_names:
            if fnmatch.fnmatch(name, pattern):
                yield dirpath / name
