from pathlib import Path

try:
    from cloudpathlib import AnyPath, CloudPath, GSClient, S3Client

    _CLOUDPATHLIB_AVAILABLE = True

    # Set default clients for cloud paths
    S3Client(no_sign_request=True).set_as_default_client()
    GSClient().set_as_default_client()

except Exception:
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
