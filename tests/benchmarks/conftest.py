import pytest


@pytest.fixture(scope="module", autouse=True)
def _warm_schema():
    """Preemptively load and internally cache schema for testing."""
    from bidsschematools import schema

    schema.load_schema()
