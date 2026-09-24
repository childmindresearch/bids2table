from pathlib import Path

try:
    from cloudpathlib import AnyPath, CloudPath, GSClient, S3Client

    _CLOUDPATHLIB_AVAILABLE = True
    S3Client(no_sign_request=True).set_as_default_client()
    GSClient().set_as_default_client()
except Exception:
    # Assignment of cloudpath classes if cloudpathlib is unavailable
    AnyPath = CloudPath = Path  # ty:ignore[invalid-assignment] # needed for py311

    _CLOUDPATHLIB_AVAILABLE = False

__all__ = ["PathT", "as_path", "cloudpathlib_is_available"]

PathT = Path | CloudPath


def as_path(path: str | PathT) -> PathT:
    """Cast input to a `Path` type."""
    if isinstance(path, str):
        return AnyPath(path)
    return path


def cloudpathlib_is_available() -> bool:
    """Check if cloudpathlib is available.

    Returns:
        ``True`` if the ``cloud`` extra (cloudpathlib) is installed, else ``False``.
    """
    return _CLOUDPATHLIB_AVAILABLE
