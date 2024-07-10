import os
from functools import lru_cache
from pathlib import Path
from typing import List

import pandas as pd


@lru_cache()
def _glob(path: Path, pattern: str) -> List[Path]:
    return list(path.glob(pattern))


@lru_cache()
def _list_files(path: Path) -> pd.Series:
    return pd.Series(os.listdir(path))
