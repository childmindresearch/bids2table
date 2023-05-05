# bids2table

Organize neuroimaging data and derivatives into tables.

## Installation

Install the latest pre-released versions of [elbow](https://github.com/clane9/elbow) and bids2table

```
pip install -U git+https://github.com/clane9/elbow.git
pip install -U git+https://github.com/clane9/bids2table-v2.git
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
