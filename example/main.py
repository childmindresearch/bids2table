import logging
import shutil
from pathlib import Path

from bids2table import load_bids_parquet

logging.basicConfig(level=logging.INFO)


root = Path(__file__).parent
# Match all BIDS data directories under bids-examples
path = root / "bids-examples" / "*"
where = root / "tables" / "bids-examples.parquet"

if where.exists():
    shutil.rmtree(where)

dset = load_bids_parquet(
    path=path,
    where=where,
    incremental=False,
    workers=None,
)
