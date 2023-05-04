import logging
import shutil
from pathlib import Path

from elbow import load_parquet

from bids2table.extractors.bids import bids_extract

logging.basicConfig(level=logging.INFO)


root = Path(__file__).parent
bids_examples_path = root / "bids-examples"
pattern = str(bids_examples_path / "*" / "**")
where = root / "tables" / f"bids-examples.parquet"

if where.exists():
    shutil.rmtree(where)

dset = load_parquet(
    source=pattern,
    extract=bids_extract,
    where=where,
    incremental=False,
    workers=None,
    max_failures=0,
)
