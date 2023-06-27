from pathlib import Path

import pytest

from bids2table import bids2table

BIDS_EXAMPLES = Path(__file__).parent.parent / "bids-examples"


@pytest.mark.parametrize("persistent", [False, True])
def test_bids2table(tmp_path: Path, persistent: bool):
    root = BIDS_EXAMPLES / "ds001"
    output = tmp_path / "index.b2t"

    df = bids2table(root=root, persistent=persistent, output=output)
    assert df.shape == (128, 39)

    # Reload from cache
    df2 = bids2table(root=root, persistent=persistent, output=output)
    assert df.equals(df2)


if __name__ == "__main__":
    pytest.main([__file__])
