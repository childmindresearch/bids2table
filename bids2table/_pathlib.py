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


if _CLOUDPATHLIB_AVAILABLE:
    # Set unsigned client as default for s3:// paths
    from cloudpathlib import S3Client

    client = S3Client(no_sign_request=True)
    client.set_as_default_client()
