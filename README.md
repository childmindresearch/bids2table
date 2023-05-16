# bids2table

Organize neuroimaging data and derivatives into tables.

## Installation

Install the latest pre-released versions of [elbow](https://github.com/cmi-dair/elbow) and bids2table

```
pip install -U git+https://github.com/cmi-dair/elbow.git
pip install -U git+https://github.com/cmi-dair/bids2table.git
```

## Example

```python
import pandas as pd

from bids2table import load_bids_table, load_bids_parquet

# Load as pandas dataframe
df = load_bids_table(
    path=path_to_bids_dataset,
)

# Load as parquet dataset
load_bids_parquet(
    path=path_to_bids_dataset,
    where="dataset.parquet",
)

# Open parquet dataset with pandas
df = pd.read_parquet("dataset.parquet")
```

See [here](example/) for a more complete example.
