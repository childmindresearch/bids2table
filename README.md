# bids2table

bids2table is a lightweight tool to index large-scale BIDS neuroimaging datasets and derivatives. It is similar to [PyBIDS](https://github.com/bids-standard/pybids), but focused narrowly on just efficient index building.

bids2table represents a BIDS dataset index as a simple table with columns for BIDS entities and file metadata. The index is stored in [Parquet](https://parquet.apache.org/) format, a binary tabular file format optimized for efficient storage and retrieval. bids2table is built with [elbow](https://github.com/cmi-dair/elbow).

## Installation

Install the latest pre-release versions of [elbow](https://github.com/cmi-dair/elbow) and bids2table

```
pip install -U git+https://github.com/cmi-dair/elbow.git
pip install -U git+https://github.com/cmi-dair/bids2table.git
```

## Example

```python
import pandas as pd

from bids2table import bids2table

# Load in memory as pandas dataframe
df = bids2table("/path/to/dataset")

# Load in parallel and stream to disk as a Parquet dataset
df = bids2table("/path/to/dataset", persistent=True, workers=8)
```

See [here](example/example.ipynb) for a more complete example.
