import sys
from contextlib import contextmanager
from pathlib import Path
from typing import List

import pandas as pd
import pytest

from bids2table import __main__ as cli
from bids2table.entities import ENTITY_NAMES_TO_KEYS

BIDS_EXAMPLES = Path(__file__).parent.parent / "bids-examples"


@contextmanager
def patch_argv(argv: List[str]):
    old_argv = sys.argv
    try:
        sys.argv = argv.copy()
        yield
    finally:
        sys.argv = old_argv


def test_main(tmp_path: Path):
    root = str(BIDS_EXAMPLES / "ds001")
    output = str(tmp_path / "index.b2t")
    argv = [
        str(Path(cli.__file__).absolute()),
        "--output",
        output,
        "--workers",
        "2",
        "--verbose",
        root,
    ]
    with patch_argv(argv):
        cli.main()

    df = pd.read_parquet(output)
    assert df.shape == (128, len(ENTITY_NAMES_TO_KEYS) + 8)


if __name__ == "__main__":
    pytest.main([__file__])
