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
